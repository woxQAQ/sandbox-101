package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// CompleteLifecycleActivation transfers active sandbox ownership in the same
// transaction that publishes running, after the resource child has committed.
func CompleteLifecycleActivation(ctx context.Context, tx *sql.Tx, activation ActivationStep) error {
	lifecycle, err := LoadLifecycle(ctx, tx, activation.OperationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	action := lifecycle.Input.Command.Action
	if action != "resume" && action != "rollback" {
		return nil
	}
	if operation.Terminal(lifecycle.Status) || lifecycle.Input.Sandbox.ID != activation.Input.SandboxID || lifecycle.Input.OwnerTenantID != activation.Input.TenantID {
		return fmt.Errorf("%w: activation no longer owns lifecycle handoff", errs.ErrConflict)
	}
	role := "clone"
	if action == "rollback" {
		role = "restore"
	}
	link, status, err := machine.LoadResourceLink(ctx, tx, lifecycle.OperationID, role)
	if err != nil {
		return err
	}
	if status != "succeeded" || link.ChildID != activation.Input.ResourceOperation {
		return fmt.Errorf("%w: activation clone link changed", errs.ErrConflict)
	}
	snapshot := lifecycle.Input.Sandbox.SnapshotID
	switch action {
	case "resume":
		if snapshot == "" {
			return fmt.Errorf("%w: resume sleep snapshot missing", errs.ErrManualRecovery)
		}
		if err = quota.New(tx).RemoveResourceReference(ctx, quota.ResourceReference{
			Kind:       "snapshot",
			ID:         snapshot,
			HolderKind: "sleep",
			HolderID:   activation.Input.SandboxID,
		}); err != nil {
			return err
		}
		if err = quota.New(tx).RemoveCostReference(ctx, quota.CostReference{
			TenantID:   activation.Input.TenantID,
			Kind:       "snapshot",
			Key:        snapshot,
			HolderKind: "active_use",
			HolderID:   "sleep:" + activation.Input.SandboxID,
		}); err != nil {
			return err
		}
	case "rollback":
		if err = RetireRollbackLease(ctx, tx, lifecycle); err != nil {
			return err
		}
		snapshot = lifecycle.Input.Rollback.Checkpoint.SnapshotID
	}
	return store.RequireOne(dbgen.New(tx).CompleteLiveStep(ctx, dbgen.CompleteLiveStepParams{
		OperationID: lifecycle.OperationID,
		StepKey:     "lifecycle",
		InputHash:   lifecycle.Hash,
		Body: operation.Encode(LifecycleState{
			Phase:      "complete",
			SnapshotID: snapshot,
		}),
	}))
}
