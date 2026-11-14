package haven

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

func (service *Service) admitWarmResource(ctx context.Context, tx *sql.Tx, parent *warmClaimAdmission, child Operation, kind, candidate, hash string) error {
	step, err := loadWarmClaim(ctx, tx, parent.ParentID)
	if err != nil {
		return err
	}
	if terminal(step.Status) || !reflect.DeepEqual(step.Input, parent.Input) {
		return fmt.Errorf("%w: warm claim no longer owns resource admission", ErrConflict)
	}
	var fence string
	if err = tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", step.Input.Pool.SandboxID).Scan(&fence); err != nil {
		return err
	}
	if fence != step.OperationID {
		return fmt.Errorf("%w: warm claim was superseded", ErrConflict)
	}
	switch parent.Role {
	case "warm_release":
		activation, err := loadActivation(ctx, tx, parent.ParentID)
		if err != nil {
			return err
		}
		if kind != "release" || candidate != step.Input.Pool.VMID || step.State.Phase != "warm_failed" || activation.Status != "failed" || activation.Input.Attempt != "warm" || activation.Input.ResourceOperation != step.Input.Source.OperationID {
			return fmt.Errorf("%w: warm release lacks original failed attempt", ErrConflict)
		}
	case "fallback":
		if kind != "clone" || step.State.Phase != "warm_retired" {
			return fmt.Errorf("%w: fallback has no completed warm retirement", ErrConflict)
		}
	default:
		return fmt.Errorf("%w: unsupported warm claim resource role", ErrInvalid)
	}
	return insertResourceLink(ctx, tx, parent.ParentID, parent.Role, step.Input.Pool.Instance, child, kind, candidate, hash)
}

func (service *Service) warmSlotDestination(tx *sql.Tx, resource resourceStep) error {
	destination := resource.Input.SlotDestination
	parent, err := loadWarmClaim(service.ctx, tx, destination.OperationID)
	if err != nil {
		return err
	}
	if terminal(parent.Status) || parent.State.Phase != "warm_failed" || destination.Kind != "claim" || destination.ID != parent.Input.Pool.SandboxID || destination.ID != resource.Input.SandboxID || parent.Input.Source.OperationID != resource.Input.SourceOperation || parent.Input.Source.State.Witness != resource.ExpectedInstance || parent.Input.Pool.Template.TenantID != resource.Input.TenantID {
		return fmt.Errorf("warm pending slot destination changed")
	}
	link, _, err := loadResourceLink(service.ctx, tx, parent.OperationID, "warm_release")
	if err != nil {
		return err
	}
	if link.ChildID != resource.OperationID {
		return fmt.Errorf("warm retirement child changed")
	}
	activation, err := loadActivation(service.ctx, tx, parent.OperationID)
	if err != nil {
		return err
	}
	if activation.Status != "failed" || activation.Input.Attempt != "warm" || !activation.Input.AllowFallback {
		return fmt.Errorf("warm retirement failed activation proof missing")
	}
	return nil
}

func slotDestinationHolder(destination *quotaDestination) string {
	switch destination.Kind {
	case "sleep":
		return "sleep:" + destination.ID
	case "claim":
		return "claim:" + destination.OperationID
	default:
		return ""
	}
}

func (service *Service) retireSelectedPool(tx *sql.Tx, resource resourceStep) error {
	parent, err := loadWarmClaim(service.ctx, tx, resource.Input.SlotDestination.OperationID)
	if err != nil {
		return err
	}
	pool, status, err := loadPoolReservation(service.ctx, tx, parent.Input.PoolID)
	if err != nil {
		return err
	}
	if status != "selected" || !reflect.DeepEqual(pool, parent.Input.Pool) || pool.VMID != resource.Candidate {
		return fmt.Errorf("retired warm pool responsibility changed")
	}
	return requireWrite(tx.ExecContext(service.ctx, "DELETE FROM pool_slots WHERE operation_id=? AND status='selected' AND body=?", parent.Input.PoolID, encode(pool)))
}
