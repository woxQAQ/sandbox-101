package sandbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/machine"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

func ValidateReleaseSlotDestination(ctx context.Context, tx *sql.Tx, resource machine.ResourceStep) error {
	destination := resource.Input.SlotDestination
	if destination == nil {
		return nil
	}
	if destination.Kind == "claim" {
		return validateWarmSlotDestination(ctx, tx, resource)
	}
	parent, err := LoadLifecycle(ctx, tx, destination.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(parent.Status) || destination.Kind != "sleep" || destination.ID != resource.Input.SandboxID || parent.Input.Sandbox.ID != destination.ID || parent.Input.OwnerTenantID != resource.Input.TenantID {
		return fmt.Errorf("release slot handoff owner changed")
	}
	role := "release"
	switch parent.Input.Command.Action {
	case "pause":
		if parent.State.Phase != "captured" || destination.SnapshotID != parent.State.SnapshotID || parent.Input.Source.OperationID != resource.Input.SourceOperation || parent.Input.Sandbox.Instance != resource.ExpectedInstance {
			return fmt.Errorf("pause release handoff source changed")
		}
	case "resume":
		role = "compensate"
		if _, err = ResumeDestination(ctx, tx, destination); err != nil {
			return err
		}
		activation, err := LoadActivation(ctx, tx, parent.OperationID)
		if err != nil {
			return err
		}
		if activation.Status != "failed" || activation.Input.ResourceOperation != resource.Input.SourceOperation || activation.Input.ExpectedInstance != resource.ExpectedInstance {
			return fmt.Errorf("resume release handoff lacks failed activation proof")
		}
	default:
		return fmt.Errorf("unsupported release slot destination")
	}
	link, _, err := machine.LoadResourceLink(ctx, tx, parent.OperationID, role)
	if err != nil {
		return err
	}
	if link.ChildID != resource.OperationID {
		return fmt.Errorf("release slot handoff child changed")
	}
	if count, err := quota.New(tx).CountResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         destination.SnapshotID,
		HolderKind: "sleep",
		HolderID:   destination.ID,
	}); err != nil || count != 1 {
		return fmt.Errorf("pause sleep holder missing: %v", err)
	}
	if count, err := quota.New(tx).CountCostReference(ctx, quota.CostReference{
		TenantID:   resource.Input.TenantID,
		Kind:       "snapshot",
		Key:        destination.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "sleep:" + destination.ID,
	}); err != nil || count != 1 {
		return fmt.Errorf("pause sleep fee holder missing: %v", err)
	}
	return nil
}
