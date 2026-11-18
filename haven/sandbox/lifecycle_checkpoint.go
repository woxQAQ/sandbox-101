package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// Checkpoint is a tenant-owned business binding to a retained snapshot. Its
// identity is independent of the sandbox's current VM generation.
type Checkpoint struct {
	TemplateID string     `json:"template_id,omitempty"`
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	SandboxID  string     `json:"sandbox_id"`
	SnapshotID string     `json:"snapshot_id"`
	Shape      loom.Shape `json:"shape"`
	CreatedAt  time.Time  `json:"created_at"`
}

func (checkpoint Checkpoint) Public() types.Checkpoint {
	return types.Checkpoint{
		ID:         checkpoint.ID,
		TenantID:   checkpoint.TenantID,
		SandboxID:  checkpoint.SandboxID,
		SnapshotID: checkpoint.SnapshotID,
		Shape:      types.Shape(checkpoint.Shape),
		CreatedAt:  checkpoint.CreatedAt,
	}
}

func bindCheckpoint(ctx context.Context, query store.DBTX, checkpoint Checkpoint, operationID string) error {
	body, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(query).InsertCheckpoint(ctx, dbgen.InsertCheckpointParams{
		ID:          checkpoint.ID,
		Tenant:      checkpoint.TenantID,
		SnapshotID:  checkpoint.SnapshotID,
		OperationID: operationID,
		Body:        body,
	}))
}

func unbindCheckpoint(ctx context.Context, query store.DBTX, checkpoint Checkpoint) error {
	body, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(query).DeleteCheckpoint(ctx, dbgen.DeleteCheckpointParams{
		ID:         checkpoint.ID,
		Tenant:     checkpoint.TenantID,
		SnapshotID: checkpoint.SnapshotID,
		Body:       body,
	}))
}

func LoadCheckpoint(ctx context.Context, query store.DBTX, id string) (Checkpoint, error) {
	var checkpoint Checkpoint
	body, err := dbgen.New(query).GetCheckpoint(ctx, id)
	if err != nil {
		return checkpoint, err
	}
	err = json.Unmarshal(body, &checkpoint)
	return checkpoint, err
}

func LoadOwnedCheckpoint(ctx context.Context, query store.DBTX, id, tenant string) (Checkpoint, error) {
	var checkpoint Checkpoint
	body, err := dbgen.New(query).GetOwnedCheckpoint(ctx, dbgen.GetOwnedCheckpointParams{
		ID:     id,
		Tenant: tenant,
	})
	if err != nil {
		return checkpoint, err
	}
	if err = json.Unmarshal(body, &checkpoint); err != nil {
		return checkpoint, err
	}
	if checkpoint.ID != id || checkpoint.TenantID != tenant {
		return checkpoint, fmt.Errorf("%w: checkpoint binding identity changed", errs.ErrManualRecovery)
	}
	return checkpoint, nil
}

func CheckpointInventory(ctx context.Context, query store.DBTX) ([]Checkpoint, error) {
	bodies, err := dbgen.New(query).ListCheckpoints(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Checkpoint, 0, len(bodies))
	for _, body := range bodies {
		var checkpoint Checkpoint
		if err = json.Unmarshal(body, &checkpoint); err != nil {
			return out, err
		}
		out = append(out, checkpoint)
	}
	return out, nil
}

func (l *Lifecycle) AdmitCheckpointDelete(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || command.Action != operation.ActionCheckpointDelete || command.CheckpointID == "" {
		return empty, fmt.Errorf("%w: checkpoint deletion requires tenant authority, checkpoint and key", errs.ErrInvalid)
	}
	guard := l.guards.TenantRequest(principal.TenantID, key)
	guard.Lock()
	defer guard.Unlock()
	mac := l.commands.Identity.CommandMAC(principal, command)
	if cached, found, err := l.existing(ctx, principal, key, &ClaimAdmission{
		MAC: mac,
	}); found || err != nil {
		return cached, err
	}
	checkpoint, err := l.catalog.GetCheckpoint(ctx, principal, command.CheckpointID)
	if err != nil {
		return empty, err
	}
	l.admission.Lock()
	defer l.admission.Unlock()
	if err = l.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := l.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadOwnedCheckpoint(ctx, tx, checkpoint.ID, checkpoint.TenantID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(current, checkpoint) {
		return empty, fmt.Errorf("%w: checkpoint binding changed", errs.ErrConflict)
	}
	publication, err := l.proofs.Snapshots().SnapshotPublication(ctx, tx, checkpoint.TenantID, checkpoint.SnapshotID)
	if err != nil {
		return empty, err
	}
	record := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "succeeded",
		Result: operation.Result{
			CheckpointID: checkpoint.ID,
			SnapshotID:   checkpoint.SnapshotID,
			SandboxID:    checkpoint.SandboxID,
		},
	}
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Command
	}{operation.ActionCheckpointDelete, operation.SanitizedCommand(command)})
	input := operation.Encode(struct {
		Checkpoint  Checkpoint
		Publication machine.SnapshotSource
	}{checkpoint, publication})
	operations := operation.New(tx)
	references := quota.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        record.ID,
		ScopeKind: "tenant",
		ScopeID:   principal.TenantID,
		Key:       key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "succeeded",
		Body:      operation.Encode(record.Result),
	}); err != nil {
		return empty, err
	}
	if err = operations.InsertCredential(ctx, record.ID, mac); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         record.ID,
		StepKey:             "checkpoint_delete",
		Action:              operation.ActionCheckpointDelete,
		InputHash:           operation.Digest(input),
		Input:               input,
		CandidateResourceID: checkpoint.SnapshotID,
		ExpectedInstance:    "",
		Status:              "succeeded",
		Body:                operation.Encode("complete"),
	})); err != nil {
		return empty, err
	}
	if err = unbindCheckpoint(ctx, tx, checkpoint); err != nil {
		return empty, err
	}
	if err = references.RemoveResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         checkpoint.SnapshotID,
		HolderKind: "checkpoint_binding",
		HolderID:   checkpoint.ID,
	}); err != nil {
		return empty, err
	}
	if err = references.RemoveCostReference(ctx, quota.CostReference{
		TenantID:   checkpoint.TenantID,
		Kind:       "snapshot",
		Key:        checkpoint.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "checkpoint-binding:" + checkpoint.ID,
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "checkpoint:" + record.ID,
		OperationID:  record.ID,
		ResourceKind: "snapshot",
		ResourceID:   checkpoint.SnapshotID,
		Body:         operation.Encode(publication),
	})); err != nil {
		return empty, err
	}
	if err = l.fault("checkpoint_delete_before_commit"); err != nil {
		return empty, err
	}
	if err = l.committer.Commit(tx); err != nil {
		return empty, err
	}
	return record, l.store.Checkpoint()
}

// A paused checkpoint adds a binding to the proven sleep publication. It does
// not launch, capture or renew the source and cannot infer a missing pause receipt.
func (l *Lifecycle) admitPausedCheckpoint(ctx context.Context, parent operation.Record, input LifecycleInput, mac string) (operation.Record, error) {
	var empty operation.Record
	l.admission.Lock()
	defer l.admission.Unlock()
	if err := l.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := l.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	current, err := LoadSandbox(ctx, tx, input.Sandbox.ID, input.OwnerTenantID)
	if err != nil {
		return empty, err
	}
	settings, err := LoadSettings(ctx, tx, current.ID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(current, input.Sandbox) || !reflect.DeepEqual(settings, input.Settings) || current.State != "paused" || current.VMID != "" || current.Instance != "" {
		return empty, fmt.Errorf("%w: paused checkpoint source changed", errs.ErrConflict)
	}
	if _, err = LifecycleFence(ctx, tx, current.ID); err == nil {
		return empty, fmt.Errorf("%w: paused checkpoint source is fenced", errs.ErrConflict)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return empty, err
	}
	source, err := machine.LoadResourceStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return empty, err
	}
	if source.Hash != input.Source.InputHash || source.State.Witness != input.SourceInstance {
		return empty, fmt.Errorf("%w: paused checkpoint original generation changed", errs.ErrManualRecovery)
	}
	proof, err := pausedReceipt(ctx, tx, current, source)
	if err != nil {
		return empty, err
	}
	input.PauseOperation = proof
	snapshot, err := l.proofs.Snapshots().LoadRestoreSource(ctx, tx, input.OwnerTenantID, current.SnapshotID)
	if err != nil {
		return empty, err
	}
	cp := Checkpoint{
		ID:         input.CheckpointID,
		TenantID:   input.OwnerTenantID,
		SandboxID:  current.ID,
		TemplateID: current.Identity.TemplateID,
		Shape:      snapshot.Snapshot.Manifest.Config.Shape,
		SnapshotID: current.SnapshotID,
		CreatedAt:  time.Now().UTC(),
	}
	parent.Status = "succeeded"
	parent.Result.SnapshotID = cp.SnapshotID
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Command
	}{"checkpoint", input.Command})
	payload := operation.Encode(input)
	operations := operation.New(tx)
	references := quota.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        parent.ID,
		ScopeKind: "tenant",
		ScopeID:   input.RequestTenantID,
		Key:       parent.Key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "succeeded",
		Body:      operation.Encode(parent.Result),
	}); err != nil {
		return empty, err
	}
	if err = operations.InsertCredential(ctx, parent.ID, mac); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         parent.ID,
		StepKey:             "lifecycle",
		Action:              "checkpoint",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: current.ID,
		ExpectedInstance:    "",
		Status:              "succeeded",
		Body: operation.Encode(LifecycleState{
			Phase:      "complete",
			SnapshotID: cp.SnapshotID,
		}),
	})); err != nil {
		return empty, err
	}
	if err = bindCheckpoint(ctx, tx, cp, parent.ID); err != nil {
		return empty, err
	}
	if err = references.AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         cp.SnapshotID,
		HolderKind: "checkpoint_binding",
		HolderID:   cp.ID,
	}); err != nil {
		return empty, err
	}
	if err = references.AddCostReference(ctx, quota.CostReference{
		TenantID:   cp.TenantID,
		Kind:       "snapshot",
		Key:        cp.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "checkpoint-binding:" + cp.ID,
	}); err != nil {
		return empty, err
	}
	if err = l.fault("paused_checkpoint_before_commit"); err != nil {
		return empty, err
	}
	if err = l.committer.Commit(tx); err != nil {
		return empty, err
	}
	return parent, l.store.Checkpoint()
}
