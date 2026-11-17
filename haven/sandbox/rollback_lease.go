package sandbox

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
)

func (admission *LifecycleAdmission) admitRollbackLease(ctx context.Context, tx *sql.Tx) error {
	parent, input, proofs := admission.Parent, admission.Input, admission.History
	if input.Command.Action != "rollback" || input.Rollback == nil {
		return fmt.Errorf("%w: rollback lease requires frozen source", errs.ErrInvalid)
	}
	checkpoint, err := LoadOwnedCheckpoint(ctx, tx, input.CheckpointID, input.OwnerTenantID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(checkpoint, input.Rollback.Checkpoint) || checkpoint.SandboxID != input.Sandbox.ID {
		return fmt.Errorf("%w: rollback checkpoint binding changed", errs.ErrConflict)
	}
	source, err := proofs.LoadRestoreSource(ctx, tx, input.OwnerTenantID, checkpoint.SnapshotID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(source, input.Rollback.Source) {
		return fmt.Errorf("%w: rollback source publication changed", errs.ErrConflict)
	}
	if err = checkRollbackCaptures(ctx, tx, input); err != nil {
		return err
	}
	return machine.PinSnapshotSource(ctx, tx, input.OwnerTenantID, machine.SourceLease{
		SnapshotID:  checkpoint.SnapshotID,
		OperationID: parent.ID,
		StepKey:     "lifecycle",
	})
}

func RetireRollbackLease(ctx context.Context, tx *sql.Tx, parent LifecycleStep) error {
	source := parent.Input.Rollback
	if source == nil {
		return fmt.Errorf("%w: rollback source lease proof missing", errs.ErrManualRecovery)
	}
	return machine.ReleaseSnapshotSource(ctx, tx, parent.Input.OwnerTenantID, machine.SourceLease{
		SnapshotID:  source.Checkpoint.SnapshotID,
		OperationID: parent.OperationID,
		StepKey:     "lifecycle",
	})
}
