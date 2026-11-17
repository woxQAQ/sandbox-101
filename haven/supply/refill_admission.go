package supply

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (service *Supply) AdmitRefill(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || command.Action != "refill" || command.TemplateID == "" {
		return empty, fmt.Errorf("%w: refill requires tenant authority, template and key", errs.ErrInvalid)
	}
	guard := service.guards.TenantRequest(principal.TenantID, key)
	guard.Lock()
	defer guard.Unlock()
	mac := service.commands.CommandMAC(principal, command)
	if accepted, found, err := service.existing(ctx, principal, key, mac); found || err != nil {
		return accepted, err
	}
	draining := service.availability.IsDraining()
	if draining {
		return empty, fmt.Errorf("%w: node is draining", errs.ErrExhausted)
	}
	catalog := NewTemplateCatalog(service.store.Database)
	public, err := catalog.GetTemplate(ctx, principal, command.TemplateID)
	if err != nil {
		return empty, err
	}
	selected, err := catalog.Owned(ctx, public.TenantID, public.ID)
	if err != nil {
		return empty, err
	}
	if selected.TargetWarm < 1 || selected.SnapshotID == "" {
		return empty, fmt.Errorf("%w: refill requires golden template with warm target", errs.ErrInvalid)
	}
	now := time.Now().UTC()
	blocked, err := RefillBackoff(ctx, service.store.Database, selected.ID, now)
	if err != nil {
		return empty, err
	}
	if blocked {
		return empty, fmt.Errorf("%w: template refill backoff is active", errs.ErrExhausted)
	}
	source, err := service.proofs.Snapshots().LoadRestoreSource(ctx, service.store.Database, selected.TenantID, selected.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = machine.VerifyRestoreSourceFiles(source); err != nil {
		return empty, err
	}
	if err = service.opts.Backend.Check(ctx, selected.Config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, service.store.Database, selected.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	parent := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			TemplateID: selected.ID,
			SandboxID:  operation.NewID("sandbox_"),
			SnapshotID: selected.SnapshotID,
		},
	}
	input := SupplyInput{
		CreatedAt:       now,
		RequestTenantID: principal.TenantID,
		Command:         operation.SanitizedCommand(command),
		Template:        selected,
		Source:          &source,
		SandboxID:       parent.Result.SandboxID,
	}
	admission := &SupplyAdmission{
		Parent: parent,
		Input:  input,
		MAC:    mac,
		Role:   "clone",
	}
	request := machine.CloneRequest{
		TenantID:   selected.TenantID,
		Key:        parent.ID + ":clone",
		SnapshotID: selected.SnapshotID,
	}
	frozen := operation.Encode(struct {
		Action  string
		Request machine.CloneRequest
	}{"clone", request})
	child, err := service.provisioning.AdmitRaw(ctx, ProvisionRequest{
		TenantID: selected.TenantID,
		Key:      request.Key,
		Frozen:   frozen,
		Template: selected,
		Image:    manifest,
		Restore:  &source,
	}, RawTarget{
		NewSandboxID: input.SandboxID,
		Budget: &quota.SlotBudget{
			Kind:  "sandbox_slot",
			Units: 0,
		},
		Parent: admission,
	})
	if child.ID == "" {
		if errors.Is(err, errs.ErrExhausted) {
			return service.recordRefillFailure(ctx, parent, input, mac)
		}
		return empty, err
	}
	parent.Result.VMID = child.Result.VMID
	if err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Supply) recordRefillFailure(ctx context.Context, parent operation.Record, input SupplyInput, mac string) (operation.Record, error) {
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
	}{"refill", input.Command})
	payload := operation.Encode(input)
	parent.Status = "failed"
	parent.Result.Error = "refill admission exhausted"
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        parent.ID,
		ScopeKind: "tenant",
		ScopeID:   input.RequestTenantID,
		Key:       parent.Key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "failed",
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
		Action:              "refill",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: input.Template.ID,
		ExpectedInstance:    "",
		Status:              "failed",
		Body: operation.Encode(SupplyState{
			Phase:      "failed",
			FinishedAt: time.Now().UTC(),
			Error:      parent.Result.Error,
		}),
	})); err != nil {
		return operation.Record{}, err
	}
	if err = service.committer.Commit(tx); err != nil {
		return operation.Record{}, err
	}
	return parent, service.store.Checkpoint()
}
