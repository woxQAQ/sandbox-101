package haven

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

func (service *Service) retireWarmClaimLease(tx *sql.Tx, parent warmClaimStep) error {
	snapshot := parent.Input.Restore.Snapshot.ID
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM source_leases WHERE snapshot_id=? AND operation_id=? AND step_key='warm_claim'", []any{
			snapshot,
			parent.OperationID,
		}},
		{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{
			snapshot,
			parent.OperationID + ":warm_claim",
		}},
		{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{
			parent.Input.Pool.Template.TenantID,
			snapshot,
			parent.OperationID + ":warm_claim",
		}},
	} {
		if err := requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}

func (service *Service) completeWarmClaim(tx *sql.Tx, activation activationStep) error {
	parent, err := loadWarmClaim(service.ctx, tx, activation.OperationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if terminal(parent.Status) || parent.Input.Pool.SandboxID != activation.Input.SandboxID || parent.Input.Pool.Template.TenantID != activation.Input.TenantID {
		return fmt.Errorf("warm completion current claim changed")
	}
	switch activation.Input.Attempt {
	case "warm":
		if parent.State.Phase != "selected" || activation.Input.ResourceOperation != parent.Input.Source.OperationID || activation.Input.ExpectedInstance != parent.Input.Pool.Instance {
			return fmt.Errorf("warm completion original selection changed")
		}
		pool, status, err := loadPoolReservation(service.ctx, tx, parent.Input.PoolID)
		if err != nil {
			return err
		}
		if status != "selected" || !reflect.DeepEqual(pool, parent.Input.Pool) {
			return fmt.Errorf("warm completion pool responsibility changed")
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM pool_slots WHERE operation_id=? AND status='selected' AND body=?", parent.Input.PoolID, encode(pool))); err != nil {
			return err
		}
	case "golden":
		link, status, err := loadResourceLink(service.ctx, tx, parent.OperationID, "fallback")
		if err != nil {
			return err
		}
		if status != "succeeded" || parent.State.Phase != "warm_retired" || link.ChildID != activation.Input.ResourceOperation {
			return fmt.Errorf("fallback completion lacks original clone handoff")
		}
		released, status, err := loadResourceLink(service.ctx, tx, parent.OperationID, "warm_release")
		if err != nil {
			return err
		}
		if status != "succeeded" {
			return fmt.Errorf("fallback completion lacks warm retirement")
		}
		retired, err := loadStep(service.ctx, tx, released.ChildID)
		if err != nil {
			return err
		}
		if retired.Status != "succeeded" || retired.State.Phase != "complete" || retired.Input.SourceOperation != parent.Input.Source.OperationID {
			return fmt.Errorf("fallback warm retirement receipt changed")
		}
		var count int
		if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM pool_slots WHERE operation_id=?", parent.Input.PoolID).Scan(&count); err != nil || count != 0 {
			return fmt.Errorf("fallback warm pool slot still occupied: %v", err)
		}
	default:
		return fmt.Errorf("unknown warm claim activation attempt")
	}
	if err = service.retireWarmClaimLease(tx, parent); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='warm_claim' AND input_hash=? AND body=? AND status IN ('pending','running')", encode(warmClaimState{
		Phase: "complete",
	}), parent.OperationID, parent.Hash, encode(parent.State))); err != nil {
		return err
	}
	resource, err := loadStep(service.ctx, tx, activation.Input.ResourceOperation)
	if err != nil {
		return err
	}
	var body []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT body FROM operations WHERE id=? AND status IN ('pending','running')", parent.OperationID).Scan(&body); err != nil {
		return err
	}
	var result operationResult
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	result.VMID = resource.Candidate
	result.ResourceReady = true
	return requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET body=? WHERE id=? AND status IN ('pending','running')", encode(result), parent.OperationID))
}
