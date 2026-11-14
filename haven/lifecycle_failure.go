package haven

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// failLifecycle commits failure only from a durable child terminal result.
// An effect error with a pending child retains an unresolved accepted intent.
func (service *Service) failLifecycle(step lifecycleStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadLifecycle(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash {
		return fmt.Errorf("%w: lifecycle failure input changed", ErrManualRecovery)
	}
	var failed int
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM operation_steps WHERE operation_id=? AND status='failed' AND (action='resource_link' OR action='activate')", step.OperationID).Scan(&failed); err != nil {
		return err
	}
	if failed == 0 {
		return nil
	}
	var fence string
	if err = tx.QueryRowContext(service.ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", step.Input.Sandbox.ID).Scan(&fence); errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}
	if fence != step.OperationID {
		return nil
	}
	var body []byte
	var status string
	if err = tx.QueryRowContext(service.ctx, "SELECT status,body FROM operations WHERE id=?", step.OperationID).Scan(&status, &body); err != nil {
		return err
	}
	if status == "succeeded" {
		return fmt.Errorf("%w: successful lifecycle cannot adopt failed child", ErrManualRecovery)
	}
	next := current.State
	next.Phase = "failed"
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key='lifecycle' AND input_hash=? AND status IN ('pending','running')", encode(next), step.OperationID, step.Hash)); err != nil {
		return err
	}
	if !terminal(status) {
		var result operationResult
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "sandbox workflow failed; resource responsibility retained"
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", encode(result), step.OperationID)); err != nil {
			return err
		}
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	return service.store.checkpoint()
}
