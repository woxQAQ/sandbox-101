package machine

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (sd *SnapshotDeletion) AdmitSnapshotDelete(ctx context.Context, request SnapshotDeleteRequest) (operation.State, error) {
	if sd.usage == nil {
		return operation.State{}, fmt.Errorf("%w: required SnapshotDeletion capability port unavailable",
			errs.ErrManualRecovery,
		)
	}

	var empty operation.State
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.SnapshotID, "snap-") {
		return empty, fmt.Errorf("%w: snapshot delete requires scoped snapshot and key", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request SnapshotDeleteRequest
	}{"snapshot_delete", request})
	hash := operation.Digest(frozen)
	sd.storeWriteMu.Lock()
	defer sd.storeWriteMu.Unlock()
	closing := sd.lifetime.Closing() || sd.store.Reviewed
	if closing {
		return empty, fmt.Errorf("haven is closing")
	}
	tx, err := sd.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := operation.New(tx).Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			sd.dispatch(cached.ID)
		}
		return cached, err
	}
	source, err := sd.proofs.SnapshotPublication(ctx, tx, request.TenantID, request.SnapshotID)
	if err != nil {
		return empty, err
	}
	accepted := operation.State{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SnapshotID: request.SnapshotID,
		},
	}
	input := snapshotDeleteInput{
		Request: request,
		Source:  source,
	}
	payload := operation.Encode(input)
	step := SnapshotDeleteStep{
		OperationID: accepted.ID,
		Hash:        operation.Digest(payload),
		Status:      "pending",
		Phase:       "admitted",
		Input:       input,
	}
	if _, err = sd.snapshotDeleteProof(ctx, tx, step, true); err != nil {
		return empty, err
	}
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   request.TenantID,
		Key:       request.Key,
		Hash:      hash,
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "snapshot_delete",
		Action:              "snapshot_delete",
		InputHash:           step.Hash,
		Input:               payload,
		CandidateResourceID: request.SnapshotID,
		ExpectedInstance:    "",
		Status:              "pending",
		Body:                operation.Encode(step.Phase),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).MarkPublishedDeleting(ctx, dbgen.MarkPublishedDeletingParams{
		ID:   request.SnapshotID,
		Body: operation.Encode(source.Snapshot),
	})); err != nil {
		return empty, err
	}
	if err = quota.TransferResourceReference(ctx, tx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         request.SnapshotID,
		HolderKind: "checkpoint",
		HolderID:   request.SnapshotID,
	}, "cleanup", accepted.ID); err != nil {
		return empty, err
	}
	if err = quota.TransferCostReference(ctx, tx, quota.CostReference{
		TenantID:   request.TenantID,
		Kind:       "snapshot",
		Key:        request.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "checkpoint:" + request.SnapshotID,
	}, "cleanup", accepted.ID+":snapshot_delete"); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "snapshot_delete:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: "snapshot",
		ResourceID:   request.SnapshotID,
		Body:         payload,
	})); err != nil {
		return empty, err
	}
	if _, err = sd.snapshotDeleteProof(ctx, tx, step, false); err != nil {
		return empty, err
	}
	if err = sd.fault("snapshot_delete_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = sd.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = sd.fault("snapshot_delete_admitted"); err != nil {
		return accepted, err
	}
	sd.dispatch(accepted.ID)
	return accepted, nil
}
