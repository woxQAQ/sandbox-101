package haven

import (
	"database/sql"
	"errors"
	"fmt"
)

// completeLifecycleActivation transfers active sandbox ownership in the same
// transaction that publishes running, after the resource child has committed.
func (service *Service) completeLifecycleActivation(tx *sql.Tx, activation activationStep) error {
	lifecycle, err := loadLifecycle(service.ctx, tx, activation.OperationID)
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
	if terminal(lifecycle.Status) || lifecycle.Input.Sandbox.ID != activation.Input.SandboxID || lifecycle.Input.OwnerTenantID != activation.Input.TenantID {
		return fmt.Errorf("%w: activation no longer owns lifecycle handoff", ErrConflict)
	}
	role := "clone"
	if action == "rollback" {
		role = "restore"
	}
	link, status, err := loadResourceLink(service.ctx, tx, lifecycle.OperationID, role)
	if err != nil {
		return err
	}
	if status != "succeeded" || link.ChildID != activation.Input.ResourceOperation {
		return fmt.Errorf("%w: activation clone link changed", ErrConflict)
	}
	snapshot := lifecycle.Input.Sandbox.SnapshotID
	switch action {
	case "resume":
		if snapshot == "" {
			return fmt.Errorf("%w: resume sleep snapshot missing", ErrManualRecovery)
		}
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='sleep' AND holder_id=?", []any{
				snapshot,
				activation.Input.SandboxID,
			}},
			{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{
				activation.Input.TenantID,
				snapshot,
				"sleep:" + activation.Input.SandboxID,
			}},
		} {
			if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
				return err
			}
		}
	case "rollback":
		if err = service.retireRollbackLease(tx, lifecycle); err != nil {
			return err
		}
		snapshot = lifecycle.Input.Rollback.Checkpoint.SnapshotID
	}
	return requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='lifecycle' AND input_hash=? AND status IN ('pending','running')", encode(lifecycleState{
		Phase:      "complete",
		SnapshotID: snapshot,
	}), lifecycle.OperationID, lifecycle.Hash))
}
