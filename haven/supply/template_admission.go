package supply

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (service *Supply) AdmitTemplate(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || command.Template == nil || (command.Action != "template_put" && command.Action != "template_build") {
		return empty, fmt.Errorf("%w: template requires tenant authority, specification and key", errs.ErrInvalid)
	}
	guard := service.guards.TenantRequest(principal.TenantID, key)
	guard.Lock()
	defer guard.Unlock()
	mac := service.commands.CommandMAC(principal, command)
	if accepted, found, err := service.existing(ctx, principal, key, mac); found || err != nil {
		return accepted, err
	}
	spec := *command.Template
	if spec.ID == "" || len(spec.ID) > 128 || spec.TargetWarm < 0 || spec.TargetWarm > service.opts.MaxWarm {
		return empty, fmt.Errorf("%w: template owner, identity or warm target invalid", errs.ErrInvalid)
	}
	if principal.Root {
		if spec.TenantID == "" {
			return empty, fmt.Errorf("%w: root template administration requires explicit owner", errs.ErrInvalid)
		}
	} else {
		if spec.TenantID != "" && spec.TenantID != principal.TenantID {
			return empty, fmt.Errorf("%w: template owner cannot change", errs.ErrInvalid)
		}
		spec.TenantID = principal.TenantID
	}
	if command.Action == "template_build" && (service.opts.Guest == nil || spec.SnapshotID != "") {
		return empty, fmt.Errorf("%w: template build requires guest effects and a fresh snapshot", errs.ErrInvalid)
	}
	if command.Action == "template_put" && spec.SnapshotID == "" && spec.TargetWarm != 0 {
		return empty, fmt.Errorf("%w: cold template cannot reserve warm instances", errs.ErrInvalid)
	}
	config := loom.Config{
		Image: spec.Image,
		Shape: spec.Shape,
	}
	if err := host.CheckConfig(&config); err != nil {
		return empty, err
	}
	if err := service.opts.Backend.Check(ctx, config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, service.store.Database, config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	var source *machine.SnapshotSource
	if spec.SnapshotID != "" {
		captured, err := service.proofs.Snapshots().LoadRestoreSource(ctx, service.store.Database, spec.TenantID, spec.SnapshotID)
		if err != nil {
			return empty, err
		}
		if err = machine.VerifyRestoreSourceFiles(captured); err != nil {
			return empty, err
		}
		if captured.Snapshot.Manifest.Config.Image != config.Image || !reflect.DeepEqual(captured.Snapshot.Manifest.Config.Shape, config.Shape) {
			return empty, fmt.Errorf("%w: template source shape/image differs", errs.ErrConflict)
		}
		config = captured.Snapshot.Manifest.Config
		source = &captured
	}
	previous, err := CurrentTemplate(ctx, service.store.Database, spec.ID)
	if err != nil {
		return empty, err
	}
	if previous != nil && previous.TenantID != spec.TenantID {
		return empty, fmt.Errorf("%w: template owner cannot change", errs.ErrConflict)
	}
	parent := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			TemplateID: spec.ID,
		},
	}
	desired := TemplateRecord{
		ID:         spec.ID,
		TenantID:   spec.TenantID,
		Config:     config,
		SnapshotID: spec.SnapshotID,
		TargetWarm: spec.TargetWarm,
		Warmup:     spec.Warmup,
		State:      "ready",
		Version:    parent.ID,
	}
	frozenCommand := operation.SanitizedCommand(command)
	frozenCommand.Template = &spec
	input := SupplyInput{
		RequesterRoot:   principal.Root,
		CreatedAt:       time.Now().UTC(),
		RequestTenantID: principal.TenantID,
		Command:         frozenCommand,
		Template:        desired,
		Previous:        previous,
		Source:          source,
	}
	if command.Action == "template_put" {
		return service.admitTemplatePut(ctx, parent, input, mac)
	}
	input.Template.State = "building"
	input.SandboxID = operation.NewID("sandbox_")
	parent.Result.SandboxID = input.SandboxID
	link := &SupplyAdmission{
		Parent: parent,
		Input:  input,
		MAC:    mac,
		Role:   "cold",
	}
	request := machine.FreshRequest{
		TenantID:   spec.TenantID,
		Key:        parent.ID + ":cold",
		TemplateID: spec.ID,
	}
	frozen := operation.Encode(struct {
		Action  string
		Request machine.FreshRequest
	}{"claim", request})
	child, err := service.provisioning.AdmitRaw(ctx, ProvisionRequest{
		TenantID: spec.TenantID,
		Key:      request.Key,
		Frozen:   frozen,
		Template: input.Template,
		Image:    manifest,
	}, RawTarget{
		NewSandboxID: input.SandboxID,
		Budget: &quota.SlotBudget{
			Kind:  "build_slot",
			Units: 1,
		},
		Parent: link,
	})
	if child.ID == "" {
		return empty, err
	}
	parent.Result.VMID = child.Result.VMID
	if err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Supply) admitTemplatePut(ctx context.Context, parent operation.Record, input SupplyInput, mac string) (operation.Record, error) {
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.gate.Check(); err != nil {
		return operation.Record{}, err
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return operation.Record{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Command
	}{input.Command.Action, input.Command})
	payload := operation.Encode(input)
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        parent.ID,
		ScopeKind: "tenant",
		ScopeID:   input.RequestTenantID,
		Key:       parent.Key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(parent.Result),
	}); err != nil {
		return operation.Record{}, err
	}
	if err = operations.InsertCredential(ctx, parent.ID, mac); err != nil {
		return operation.Record{}, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         parent.ID,
		StepKey:             "supply",
		Action:              "template_put",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: input.Template.ID,
		ExpectedInstance:    "",
		Status:              "pending",
		Body: operation.Encode(SupplyState{
			Phase: "admitted",
		}),
	})); err != nil {
		return operation.Record{}, err
	}
	if err = quota.New(tx).AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         input.Template.Config.Image,
		HolderKind: "step",
		HolderID:   parent.ID + ":supply",
	}); err != nil {
		return operation.Record{}, err
	}
	if err = input.Template.reservePublication(ctx, tx, input.Previous, parent.ID); err != nil {
		return operation.Record{}, err
	}
	if err = service.publishTemplate(ctx, tx, SupplyStep{
		OperationID: parent.ID,
		Hash:        operation.Digest(payload),
		Status:      "pending",
		Input:       input,
		State: SupplyState{
			Phase: "admitted",
		},
	}, input.Template.SnapshotID); err != nil {
		return operation.Record{}, err
	}
	if err = service.fault("template_publication_before_commit"); err != nil {
		return operation.Record{}, err
	}
	if err = service.committer.Commit(tx); err != nil {
		return operation.Record{}, err
	}
	parent.Status = "succeeded"
	parent.Result.SnapshotID = input.Template.SnapshotID
	return parent, service.store.Checkpoint()
}
