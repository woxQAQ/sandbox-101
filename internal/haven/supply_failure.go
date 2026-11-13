package haven

import (
	"encoding/json"
	"fmt"
	"time"
)

func (service *Service) failSupply(step supplyStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadSupply(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash {
		return fmt.Errorf("supply failure input changed")
	}
	var pending int
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM operation_steps WHERE operation_id=? AND action='resource_link' AND status IN ('pending','running')", step.OperationID).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return nil
	}
	var body []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT body FROM operations WHERE id=? AND status IN ('pending','running')", step.OperationID).Scan(&body); err != nil {
		return err
	}
	var result operationResult
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	result.Error = "supply workflow failed; owned resources retained"
	next := current.State
	next.Phase = "failed"
	next.Error = result.Error
	next.FinishedAt = time.Now().UTC()
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key='supply' AND input_hash=? AND status IN ('pending','running')", []any{encode(next), step.OperationID, step.Hash}},
		{"UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", []any{encode(result), step.OperationID}},
		{"INSERT INTO cleanup_intents VALUES(?,?,'supply',?,'pending',?)", []any{"supply:" + step.OperationID, step.OperationID, step.Input.SandboxID, encode(step.Input)}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if step.Input.Command.Action == "refill" {
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE pool_slots SET status='cleanup' WHERE operation_id=? AND status='pending'", step.OperationID)); err != nil {
			return err
		}
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	return service.store.checkpoint()
}
