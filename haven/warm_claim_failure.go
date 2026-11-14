package haven

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

func (service *Service) failWarmClaim(step warmClaimStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadWarmClaim(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash {
		return fmt.Errorf("warm failure intent changed")
	}
	if terminal(current.Status) {
		return nil
	}
	known := false
	if _, status, lookup := loadResourceLink(service.ctx, tx, step.OperationID, "fallback"); lookup == nil {
		known = status == "failed"
	} else if !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	activation, err := loadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	known = known || (activation.Input.Attempt == "golden" && activation.Status == "failed")
	if !known {
		return nil
	}
	var fence string
	if err = tx.QueryRowContext(service.ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", step.Input.Pool.SandboxID).Scan(&fence); err != nil {
		return err
	}
	if fence != step.OperationID {
		return nil
	}
	var status string
	var body []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT status,body FROM operations WHERE id=?", step.OperationID).Scan(&status, &body); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key='warm_claim' AND input_hash=? AND status IN ('pending','running')", encode(warmClaimState{
		Phase: "failed",
	}), step.OperationID, step.Hash)); err != nil {
		return err
	}
	if !terminal(status) {
		var result operationResult
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "warm claim fallback failed; resource responsibility retained"
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", encode(result), step.OperationID)); err != nil {
			return err
		}
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO cleanup_intents VALUES(?,?,'warm_claim',?,'pending',?)", "warm_claim:"+step.OperationID, step.OperationID, step.Input.Pool.SandboxID, encode(step.Input))); err != nil {
		return err
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) retireSupersededWarm(tx *sql.Tx, release lifecycleStep) error {
	parent, err := loadWarmClaim(service.ctx, tx, release.Input.PriorFence)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if parent.Input.Pool.SandboxID != release.Input.Sandbox.ID || parent.Input.Pool.Template.TenantID != release.Input.OwnerTenantID {
		return fmt.Errorf("release superseded warm claim scope changed")
	}
	if parent.Status == "succeeded" {
		return fmt.Errorf("completed warm claim retained a configuration fence")
	}
	pool, status, err := loadPoolReservation(service.ctx, tx, parent.Input.PoolID)
	if err == nil {
		if status != "selected" || !reflect.DeepEqual(pool, parent.Input.Pool) || release.Input.Sandbox.VMID != pool.VMID {
			return fmt.Errorf("release superseded pool responsibility changed")
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM pool_slots WHERE operation_id=? AND status='selected' AND body=?", parent.Input.PoolID, encode(pool))); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = service.retireWarmClaimLease(tx, parent); err != nil {
		return err
	}
	if !terminal(parent.Status) {
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key='warm_claim' AND input_hash=? AND status IN ('pending','running')", encode(warmClaimState{
			Phase: "superseded",
		}), parent.OperationID, parent.Hash)); err != nil {
			return err
		}
	}
	var state string
	var body []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT status,body FROM operations WHERE id=?", parent.OperationID).Scan(&state, &body); err != nil {
		return err
	}
	if !terminal(state) {
		var result operationResult
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "warm claim superseded by release"
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", encode(result), parent.OperationID)); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(service.ctx, "UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind='warm_claim' AND status='pending'", "warm_claim:"+parent.OperationID, parent.OperationID)
	return err
}
