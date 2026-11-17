package sandbox

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/supply"
)

func (parent *WarmClaimAdmission) ValidateProvisionTarget(ctx context.Context, tx *sql.Tx, record ownership.Sandbox, source machine.Source, holder string) (*machine.QuotaDestination, error) {
	claim, err := LoadWarmClaim(ctx, tx, parent.ParentID)
	if err != nil {
		return nil, err
	}
	if claim.State.Phase != "warm_retired" || operation.Terminal(claim.Status) || record.State != "terminated" || holder != "claim:"+parent.ParentID || record.ID != claim.Input.Pool.SandboxID || source.Snapshot.ID != claim.Input.Restore.Snapshot.ID {
		return nil, fmt.Errorf("%w: fallback target differs from retired warm responsibility", errs.ErrConflict)
	}
	released, status, err := machine.LoadResourceLink(ctx, tx, parent.ParentID, "warm_release")
	if err != nil {
		return nil, err
	}
	if status != "succeeded" {
		return nil, fmt.Errorf("%w: fallback warm cleanup incomplete", errs.ErrConflict)
	}
	receipt, err := machine.LoadResourceStep(ctx, tx, released.ChildID)
	if err != nil {
		return nil, err
	}
	if receipt.Status != "succeeded" || receipt.State.Phase != "complete" || receipt.Input.SourceOperation != claim.Input.Source.OperationID || receipt.Input.SlotDestination == nil || receipt.Input.SlotDestination.Kind != "claim" || receipt.Input.SlotDestination.OperationID != parent.ParentID {
		return nil, fmt.Errorf("%w: fallback original warm retirement proof changed", errs.ErrManualRecovery)
	}
	return nil, nil
}

func admitWarmResource(ctx context.Context, tx *sql.Tx, parent *WarmClaimAdmission, child operation.Record, kind, candidate, hash string) error {
	step, err := LoadWarmClaim(ctx, tx, parent.ParentID)
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) || !reflect.DeepEqual(step.Input, parent.Input) {
		return fmt.Errorf("%w: warm claim no longer owns resource admission", errs.ErrConflict)
	}
	fence, err := LifecycleFence(ctx, tx, step.Input.Pool.SandboxID)
	if err != nil {
		return err
	}
	if fence != step.OperationID {
		return fmt.Errorf("%w: warm claim was superseded", errs.ErrConflict)
	}
	switch parent.Role {
	case "warm_release":
		activation, err := LoadActivation(ctx, tx, parent.ParentID)
		if err != nil {
			return err
		}
		if kind != "release" || candidate != step.Input.Pool.VMID || step.State.Phase != "warm_failed" || activation.Status != "failed" || activation.Input.Attempt != "warm" || activation.Input.ResourceOperation != step.Input.Source.OperationID {
			return fmt.Errorf("%w: warm release lacks original failed attempt", errs.ErrConflict)
		}
	case "fallback":
		if kind != "clone" || step.State.Phase != "warm_retired" {
			return fmt.Errorf("%w: fallback has no completed warm retirement", errs.ErrConflict)
		}
	default:
		return fmt.Errorf("%w: unsupported warm claim resource role", errs.ErrInvalid)
	}
	return machine.InsertResourceLink(ctx, tx, parent.ParentID, parent.Role, step.Input.Pool.Instance, child, kind, candidate, hash)
}

func validateWarmSlotDestination(ctx context.Context, tx *sql.Tx, resource machine.ResourceStep) error {
	destination := resource.Input.SlotDestination
	parent, err := LoadWarmClaim(ctx, tx, destination.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(parent.Status) || parent.State.Phase != "warm_failed" || destination.Kind != "claim" || destination.ID != parent.Input.Pool.SandboxID || destination.ID != resource.Input.SandboxID || parent.Input.Source.OperationID != resource.Input.SourceOperation || parent.Input.Source.State.Witness != resource.ExpectedInstance || parent.Input.Pool.Template.TenantID != resource.Input.TenantID {
		return fmt.Errorf("warm pending slot destination changed")
	}
	link, _, err := machine.LoadResourceLink(ctx, tx, parent.OperationID, "warm_release")
	if err != nil {
		return err
	}
	if link.ChildID != resource.OperationID {
		return fmt.Errorf("warm retirement child changed")
	}
	activation, err := LoadActivation(ctx, tx, parent.OperationID)
	if err != nil {
		return err
	}
	if activation.Status != "failed" || activation.Input.Attempt != "warm" || !activation.Input.AllowFallback {
		return fmt.Errorf("warm retirement failed activation proof missing")
	}
	return nil
}

func RetireSelectedPool(ctx context.Context, tx *sql.Tx, resource machine.ResourceStep) error {
	parent, err := LoadWarmClaim(ctx, tx, resource.Input.SlotDestination.OperationID)
	if err != nil {
		return err
	}
	reservation, status, err := supply.LoadPoolReservation(ctx, tx, parent.Input.PoolID)
	if err != nil {
		return err
	}
	if status != "selected" || !reflect.DeepEqual(reservation, parent.Input.Pool) || reservation.VMID != resource.Candidate {
		return fmt.Errorf("retired warm pool responsibility changed")
	}
	return supply.RetireSelectedReservation(ctx, tx, parent.Input.PoolID, operation.Encode(reservation))
}
