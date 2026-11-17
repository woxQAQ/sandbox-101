package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// A paused checkpoint adds a binding to the proven sleep publication. It does
// not launch, capture or renew the source and cannot infer a missing pause receipt.
func (service *Lifecycle) admitPausedCheckpoint(ctx context.Context, parent operation.Record, input LifecycleInput, mac string) (operation.Record, error) {
	var empty operation.Record
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
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
	snapshot, err := service.proofs.Snapshots().LoadRestoreSource(ctx, tx, input.OwnerTenantID, current.SnapshotID)
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
	if err = service.fault("paused_checkpoint_before_commit"); err != nil {
		return empty, err
	}
	if err = service.committer.Commit(tx); err != nil {
		return empty, err
	}
	return parent, service.store.Checkpoint()
}
