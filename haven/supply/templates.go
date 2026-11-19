package supply

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	"github.com/woxQAQ/haven/types"
)

// TemplateCatalog verifies catalog ownership against the original publication.
// The catalog and frozen publisher are read from the same database or transaction.
type TemplateCatalog struct {
	queries  *dbgen.Queries
	database store.DBTX
}

func NewTemplateCatalog(database store.DBTX) *TemplateCatalog {
	return &TemplateCatalog{
		queries:  dbgen.New(database),
		database: database,
	}
}

func (tc *TemplateCatalog) Fresh(ctx context.Context, tenant, id string) (TemplateRecord, error) {
	record, err := tc.Owned(ctx, tenant, id)
	if err != nil {
		return record, err
	}
	if record.SnapshotID != "" || record.TargetWarm != 0 {
		return record, fmt.Errorf("%w: fresh workflow requires a fresh template without a warm pool", errs.ErrIncompatible)
	}
	return record, nil
}

func (tc *TemplateCatalog) Owned(ctx context.Context, tenant, id string) (TemplateRecord, error) {
	var record TemplateRecord
	body, err := tc.queries.GetOwnedTemplate(ctx, dbgen.GetOwnedTemplateParams{
		ID:     id,
		Tenant: tenant,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return record, fmt.Errorf("%w: template not found", errs.ErrNotFound)
	}
	if err != nil {
		return record, err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return record, err
	}
	owner, err := tc.queries.GetPublisher(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return record, fmt.Errorf("%w: template permanent ownership proof is missing", errs.ErrManualRecovery)
	}
	if err != nil {
		return record, err
	}
	proof, err := LoadSupply(ctx, tc.database, owner.OperationID)
	if err != nil {
		return record, fmt.Errorf("%w: template immutable publication step is missing", errs.ErrManualRecovery)
	}
	if owner.TenantID != tenant || owner.ScopeKind != "tenant" || owner.ScopeID != proof.Input.RequestTenantID || (!proof.Input.RequesterRoot && owner.ScopeID != tenant) || proof.Input.Template.ID != id || proof.Input.Template.TenantID != tenant || (proof.Input.Command.Action != "template_put" && proof.Input.Command.Action != "template_build") {
		return record, fmt.Errorf("%w: template publisher authorization is inconsistent", errs.ErrManualRecovery)
	}
	if (owner.Status == "pending" || owner.Status == "running") && proof.Status != "succeeded" && proof.Status != "failed" {
		return record, fmt.Errorf("%w: template publication is in progress", errs.ErrConflict)
	}
	expected := proof.Input.Template
	expected.State = "ready"
	expected.SnapshotID = proof.State.SnapshotID
	if record.ID != id || record.TenantID != tenant || record.Version != owner.OperationID || owner.Status != "succeeded" || proof.Status != "succeeded" || proof.State.Phase != "complete" || !reflect.DeepEqual(record, expected) {
		return record, fmt.Errorf("%w: template immutable publication differs from catalog", errs.ErrManualRecovery)
	}
	return record, nil
}

func (tc *TemplateCatalog) GetTemplate(ctx context.Context, principal authorization.Principal, id string) (operation.TemplateSpec, error) {
	if principal.SandboxID != "" {
		return operation.TemplateSpec{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	record, err := LoadTemplate(ctx, tc.database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return operation.TemplateSpec{}, errs.ErrNotFound
	}
	if err != nil {
		return operation.TemplateSpec{}, err
	}
	if !principal.Root && principal.TenantID != record.TenantID {
		return operation.TemplateSpec{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	verified, err := tc.Owned(ctx, record.TenantID, id)
	if err != nil {
		return operation.TemplateSpec{}, err
	}
	return operation.TemplateSpec{
		ID:         verified.ID,
		TenantID:   verified.TenantID,
		Image:      verified.Config.Image,
		Shape:      verified.Config.Shape,
		SnapshotID: verified.SnapshotID,
		State:      verified.State,
		TargetWarm: verified.TargetWarm,
		Warmup:     verified.Warmup,
	}, nil
}

func (tc *TemplateCatalog) Templates(ctx context.Context, principal authorization.Principal) ([]types.Template, error) {
	if principal.SandboxID != "" {
		return nil, fmt.Errorf("%w: tenant access required", errs.ErrConflict)
	}
	ids, err := tc.queries.ListTemplateIDs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]types.Template, 0, len(ids))
	for _, item := range ids {
		if !principal.Root && item.Tenant != principal.TenantID {
			continue
		}
		record, err := tc.Owned(ctx, item.Tenant, item.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, record.Public())
	}
	return out, nil
}

func (s *Supply) AdmitTemplate(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || command.Template == nil || (command.Action != "template_put" && command.Action != "template_build") {
		return empty, fmt.Errorf("%w: template requires tenant authority, specification and key", errs.ErrInvalid)
	}
	guard := s.guards.TenantRequest(principal.TenantID, key)
	guard.Lock()
	defer guard.Unlock()
	mac := s.commands.CommandMAC(principal, command)
	if accepted, found, err := s.existing(ctx, principal, key, mac); found || err != nil {
		return accepted, err
	}
	spec := *command.Template
	if spec.ID == "" || len(spec.ID) > 128 || spec.TargetWarm < 0 || spec.TargetWarm > s.opts.MaxWarm {
		return empty, errs.InvalidInput("template owner, identity or warm target invalid")
	}
	if principal.Root {
		if spec.TenantID == "" {
			return empty, errs.InvalidInput("root template administration requires explicit owner")
		}
	} else {
		if spec.TenantID != "" && spec.TenantID != principal.TenantID {
			return empty, errs.InvalidInput("template owner cannot change")
		}
		spec.TenantID = principal.TenantID
	}
	if command.Action == "template_build" && (s.opts.Guest == nil || spec.SnapshotID != "") {
		return empty, fmt.Errorf("%w: template build requires guest effects and a fresh snapshot", errs.ErrInvalid)
	}
	if command.Action == "template_put" && spec.SnapshotID == "" && spec.TargetWarm != 0 {
		return empty, errs.InvalidInput("cold template cannot reserve warm instances")
	}
	config := loom.Config{
		Image: spec.Image,
		Shape: spec.Shape,
	}
	if err := host.CheckConfig(&config); err != nil {
		return empty, err
	}
	if err := s.opts.Backend.Check(ctx, config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, s.store.Database, config.Image)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, errs.ErrNotFound
	}
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(s.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	var source *machine.SnapshotSource
	if spec.SnapshotID != "" {
		captured, err := s.proofs.Snapshots().LoadRestoreSource(ctx, s.store.Database, spec.TenantID, spec.SnapshotID)
		if errors.Is(err, sql.ErrNoRows) {
			return empty, errs.ErrNotFound
		}
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
	previous, err := CurrentTemplate(ctx, s.store.Database, spec.ID)
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
		return s.admitTemplatePut(ctx, parent, input, mac)
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
	frozen := machine.FreezeProvisionRequest(request, nil)
	child, err := s.provisioning.AdmitBare(ctx, ProvisionRequest{
		TenantID: spec.TenantID,
		Key:      request.Key,
		Frozen:   frozen,
		Template: input.Template,
		Image:    manifest,
	}, BareTarget{
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
	s.dispatch(parent.ID)
	return parent, nil
}

func (s *Supply) admitTemplatePut(ctx context.Context, parent operation.Record, input SupplyInput, mac string) (operation.Record, error) {
	s.admission.Lock()
	defer s.admission.Unlock()
	if err := s.gate.Check(); err != nil {
		return operation.Record{}, err
	}
	tx, err := s.store.Database.BeginTx(ctx, nil)
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
	result, err := s.publishTemplate(ctx, tx, SupplyStep{
		OperationID: parent.ID,
		Hash:        operation.Digest(payload),
		Status:      "pending",
		Input:       input,
		State: SupplyState{
			Phase: "admitted",
		},
	}, input.Template.SnapshotID)
	if err != nil {
		return operation.Record{}, err
	}
	if err = s.fault("template_publication_before_commit"); err != nil {
		return operation.Record{}, err
	}
	if err = s.committer.Complete(tx, operation.Completion{
		ID:     parent.ID,
		Result: result,
	}); err != nil {
		return operation.Record{}, err
	}
	if err = tx.Commit(); err != nil {
		return operation.Record{}, err
	}
	parent.Status = "succeeded"
	parent.Result.SnapshotID = input.Template.SnapshotID
	return parent, s.store.Checkpoint()
}

func (s *Supply) publishTemplate(ctx context.Context, tx *sql.Tx, step SupplyStep, snapshot string) (operation.Result, error) {
	current, err := LoadSupply(ctx, tx, step.OperationID)
	if err != nil {
		return operation.Result{}, err
	}
	if current.Hash != step.Hash || string(operation.Encode(current.Input)) != string(operation.Encode(step.Input)) || !reflect.DeepEqual(current.State, step.State) || operation.Terminal(current.Status) {
		return operation.Result{}, fmt.Errorf("template publication intent changed")
	}
	input := step.Input
	queries := dbgen.New(tx)
	references := quota.New(tx)
	operations := operation.New(tx)
	intent, err := queries.GetIntent(ctx, input.Template.ID)
	if err != nil {
		return operation.Result{}, err
	}
	if intent.TenantID != input.Template.TenantID || intent.OperationID != step.OperationID {
		return operation.Result{}, fmt.Errorf("template publication was superseded")
	}
	previous, err := CurrentTemplate(ctx, tx, input.Template.ID)
	if err != nil {
		return operation.Result{}, err
	}
	if !reflect.DeepEqual(previous, input.Previous) {
		return operation.Result{}, fmt.Errorf("template prior publication changed")
	}
	desired := input.Template
	desired.State = "ready"
	desired.SnapshotID = snapshot
	if snapshot != "" {
		source, err := s.proofs.Snapshots().SnapshotPublication(ctx, tx, desired.TenantID, snapshot)
		if err != nil {
			return operation.Result{}, err
		}
		if string(operation.Encode(source.Snapshot.Manifest.Config)) != string(operation.Encode(desired.Config)) {
			return operation.Result{}, fmt.Errorf("template snapshot configuration changed")
		}
		if input.Command.Action == "template_put" {
			if input.Source == nil || !reflect.DeepEqual(*input.Source, source) {
				return operation.Result{}, fmt.Errorf("template import original publication changed")
			}
		} else {
			captured, status, err := machine.LoadResourceLink(ctx, tx, step.OperationID, "capture")
			if err != nil {
				return operation.Result{}, err
			}
			if status != "succeeded" || captured.ChildID != source.CaptureOperation {
				return operation.Result{}, fmt.Errorf("build snapshot lacks original capture completion")
			}
			released, status, err := machine.LoadResourceLink(ctx, tx, step.OperationID, "release")
			if err != nil {
				return operation.Result{}, err
			}
			if status != "succeeded" {
				return operation.Result{}, fmt.Errorf("build scratch retirement incomplete")
			}
			resource, err := machine.LoadResourceStep(ctx, tx, released.ChildID)
			if err != nil {
				return operation.Result{}, err
			}
			if resource.Status != "succeeded" || resource.State.Phase != "complete" || resource.Input.SandboxID != input.SandboxID {
				return operation.Result{}, fmt.Errorf("build scratch deletion proof changed")
			}
		}
	}
	if previous == nil || previous.SnapshotID != snapshot {
		if snapshot != "" {
			if err = references.AddResourceReference(ctx, quota.ResourceReference{
				Kind:       "snapshot",
				ID:         snapshot,
				HolderKind: "template",
				HolderID:   desired.ID,
			}); err != nil {
				return operation.Result{}, err
			}
			if err = references.AddCostReference(ctx, quota.CostReference{
				TenantID:   desired.TenantID,
				Kind:       "snapshot",
				Key:        snapshot,
				HolderKind: "active_use",
				HolderID:   "template:" + desired.ID,
			}); err != nil {
				return operation.Result{}, err
			}
		}
		if previous != nil && previous.SnapshotID != "" {
			if err = references.RemoveResourceReference(ctx, quota.ResourceReference{
				Kind:       "snapshot",
				ID:         previous.SnapshotID,
				HolderKind: "template",
				HolderID:   desired.ID,
			}); err != nil {
				return operation.Result{}, err
			}
			if err = references.RemoveCostReference(ctx, quota.CostReference{
				TenantID:   desired.TenantID,
				Kind:       "snapshot",
				Key:        previous.SnapshotID,
				HolderKind: "active_use",
				HolderID:   "template:" + desired.ID,
			}); err != nil {
				return operation.Result{}, err
			}
		}
	}
	if previous == nil || previous.Config.Image != desired.Config.Image {
		if err = references.AddResourceReference(ctx, quota.ResourceReference{
			Kind:       "image",
			ID:         desired.Config.Image,
			HolderKind: "template",
			HolderID:   desired.ID,
		}); err != nil {
			return operation.Result{}, err
		}
		if previous != nil {
			if err = references.RemoveResourceReference(ctx, quota.ResourceReference{
				Kind:       "image",
				ID:         previous.Config.Image,
				HolderKind: "template",
				HolderID:   desired.ID,
			}); err != nil {
				return operation.Result{}, err
			}
		}
	} else {
		count, lookup := references.CountResourceReference(ctx, quota.ResourceReference{
			Kind:       "image",
			ID:         desired.Config.Image,
			HolderKind: "template",
			HolderID:   desired.ID,
		})
		if lookup != nil || count != 1 {
			return operation.Result{}, fmt.Errorf("template prior image responsibility missing: %v", lookup)
		}
	}
	if err = publishTemplate(ctx, tx, desired, previous); err != nil {
		return operation.Result{}, err
	}
	parent, err := operations.Projection(ctx, step.OperationID)
	if err != nil {
		return operation.Result{}, err
	}
	if operation.Terminal(parent.Status) || parent.ScopeKind != "tenant" || parent.ScopeID != input.RequestTenantID {
		return operation.Result{}, fmt.Errorf("template result authority changed")
	}
	var result operation.Result
	if err = json.Unmarshal(parent.Body, &result); err != nil {
		return operation.Result{}, err
	}
	result.TemplateID = desired.ID
	result.SnapshotID = snapshot
	completed := operation.Encode(SupplyState{
		FinishedAt: time.Now().UTC(),
		Phase:      "complete",
		SnapshotID: snapshot,
	})
	previousState := operation.Encode(step.State)
	if err = references.RemoveResourceReference(ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         desired.Config.Image,
		HolderKind: "step",
		HolderID:   step.OperationID + ":supply",
	}); err != nil {
		return operation.Result{}, err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteUnchangedStep(ctx, dbgen.CompleteUnchangedStepParams{
		OperationID:  step.OperationID,
		StepKey:      "supply",
		InputHash:    step.Hash,
		PreviousBody: previousState,
		NewBody:      completed,
	})); err != nil {
		return operation.Result{}, err
	}
	return result, nil
}

// TemplateRecord is a published or proposed template version. Publication is verified
// against the original supply workflow before a catalog record is used.
type TemplateRecord struct {
	Version    string      `json:"version"`
	State      string      `json:"state"`
	Warmup     string      `json:"warmup,omitempty"`
	ID         string      `json:"id"`
	TenantID   string      `json:"tenant_id"`
	Config     loom.Config `json:"config"`
	SnapshotID string      `json:"snapshot_id,omitempty"`
	TargetWarm int         `json:"target_warm"`
}

func (record TemplateRecord) Public() types.Template {
	return types.Template{
		ID:         record.ID,
		TenantID:   record.TenantID,
		Image:      record.Config.Image,
		Shape:      types.Shape(record.Config.Shape),
		SnapshotID: record.SnapshotID,
		State:      record.State,
		TargetWarm: record.TargetWarm,
		Warmup:     record.Warmup,
	}
}
