package haven

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

func (service *Service) admitRollbackLease(ctx context.Context, tx *sql.Tx, parent Operation, input lifecycleInput) error {
	if input.Command.Action != "rollback" || input.Rollback == nil {
		return fmt.Errorf("%w: rollback lease requires frozen source", ErrInvalid)
	}
	checkpoint, err := loadCheckpoint(ctx, tx, input.CheckpointID, input.OwnerTenantID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(checkpoint, input.Rollback.Checkpoint) || checkpoint.SandboxID != input.Sandbox.ID {
		return fmt.Errorf("%w: rollback checkpoint binding changed", ErrConflict)
	}
	source, err := service.loadRestoreSource(ctx, tx, input.OwnerTenantID, checkpoint.SnapshotID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(source, input.Rollback.Source) {
		return fmt.Errorf("%w: rollback source publication changed", ErrConflict)
	}
	if err = service.checkRollbackCaptures(ctx, tx, input); err != nil {
		return err
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO source_leases VALUES(?,?,'lifecycle')", []any{
			checkpoint.SnapshotID,
			parent.ID,
		}},
		{"INSERT INTO resource_refs VALUES('snapshot',?,'step',?)", []any{
			checkpoint.SnapshotID,
			parent.ID + ":lifecycle",
		}},
		{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'engine_dependency',?)", []any{
			input.OwnerTenantID,
			checkpoint.SnapshotID,
			parent.ID + ":lifecycle",
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}

func (service *Service) retireRollbackLease(tx *sql.Tx, parent lifecycleStep) error {
	source := parent.Input.Rollback
	if source == nil {
		return fmt.Errorf("%w: rollback source lease proof missing", ErrManualRecovery)
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM source_leases WHERE snapshot_id=? AND operation_id=? AND step_key='lifecycle'", []any{
			source.Checkpoint.SnapshotID,
			parent.OperationID,
		}},
		{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{
			source.Checkpoint.SnapshotID,
			parent.OperationID + ":lifecycle",
		}},
		{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{
			parent.Input.OwnerTenantID,
			source.Checkpoint.SnapshotID,
			parent.OperationID + ":lifecycle",
		}},
	} {
		if err := requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}
