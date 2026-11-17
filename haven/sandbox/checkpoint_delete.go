package sandbox

import (
	"context"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func (service *Lifecycle) AdmitCheckpointDelete(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || command.Action != "checkpoint_delete" || command.CheckpointID == "" {
		return empty, fmt.Errorf("%w: checkpoint deletion requires tenant authority, checkpoint and key", errs.ErrInvalid)
	}
	guard := service.guards.TenantRequest(principal.TenantID, key)
	guard.Lock()
	defer guard.Unlock()
	mac := service.commands.Identity.CommandMAC(principal, command)
	if cached, found, err := service.existing(ctx, principal, key, &ClaimAdmission{
		MAC: mac,
	}); found || err != nil {
		return cached, err
	}
	checkpoint, err := service.catalog.GetCheckpoint(ctx, principal, command.CheckpointID)
	if err != nil {
		return empty, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	if err = service.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
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
	publication, err := service.proofs.Snapshots().SnapshotPublication(ctx, tx, checkpoint.TenantID, checkpoint.SnapshotID)
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
	}{"checkpoint_delete", operation.SanitizedCommand(command)})
	input := operation.Encode(struct {
		Checkpoint  Checkpoint
		Publication machine.Source
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
		Action:              "checkpoint_delete",
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
	if err = service.fault("checkpoint_delete_before_commit"); err != nil {
		return empty, err
	}
	if err = service.committer.Commit(tx); err != nil {
		return empty, err
	}
	return record, service.store.Checkpoint()
}
