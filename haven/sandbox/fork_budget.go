package sandbox

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type forkBudget struct {
	Bytes   int64
	Memory  int64
	Storage int64
	CPUs    int
}

func forkFunding(input ForkInput) forkBudget {
	shape := input.Template.Config.Shape
	count := int64(len(input.Targets))
	return forkBudget{
		Bytes:   count * ((2*(shape.MemoryMiB+shape.DiskMiB) + 64) << 20),
		Memory:  count * (shape.MemoryMiB + 128),
		Storage: count * shape.DiskMiB,
		CPUs:    int(count) * shape.VCPUs,
	}
}

func (parent *forkAdmission) reserveTargets(ctx context.Context, tx *sql.Tx) error {
	acceptedOperation, input := parent.Parent, parent.Input
	free, capacity := parent.Free, parent.capacity
	budget := forkFunding(input)
	tenant, err := authorization.New(tx).Limits(ctx, input.OwnerTenantID)
	if err != nil {
		return err
	}
	responsibilities := quota.New(tx)
	usage, err := responsibilities.Usage(ctx, input.OwnerTenantID)
	if err != nil {
		return err
	}
	var captureStorage, captureBytes int64
	if input.Source != nil {
		shape := input.Template.Config.Shape
		captureStorage = shape.MemoryMiB + shape.DiskMiB
		captureBytes = (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20
	}
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 || usage.Slots > tenant.MaxSandboxes || int64(len(input.Targets)) > tenant.MaxSandboxes-usage.Slots || usage.StorageMiB > tenant.MaxStorageMiB || budget.Storage+captureStorage > tenant.MaxStorageMiB-usage.StorageMiB {
		return fmt.Errorf("%w: fork batch exceeds tenant quota", errs.ErrExhausted)
	}
	reserved, err := responsibilities.ReservedCapacity(ctx)
	if err != nil {
		return err
	}
	if reserved.Bytes > free || capacity.SafetyBytes > free-reserved.Bytes || budget.Bytes+captureBytes > free-reserved.Bytes-capacity.SafetyBytes || reserved.MemoryMiB > capacity.MaxMemoryMiB || budget.Memory > capacity.MaxMemoryMiB-reserved.MemoryMiB || reserved.CPUs > int64(capacity.MaxVCPUs) || int64(budget.CPUs) > int64(capacity.MaxVCPUs)-reserved.CPUs {
		return fmt.Errorf("%w: fork batch exceeds host reservation", errs.ErrExhausted)
	}
	for _, target := range input.Targets {
		if err := store.RequireOne(dbgen.New(tx).InsertSandbox(ctx, dbgen.InsertSandboxParams{
			ID:     target.ID,
			Tenant: target.TenantID,
			State:  target.State,
			Body:   operation.Encode(target),
		})); err != nil {
			return err
		}
		if err := store.RequireOne(dbgen.New(tx).InsertSandboxSettings(ctx, dbgen.InsertSandboxSettingsParams{
			SandboxID: target.ID,
			Body:      operation.Encode(input.Settings),
		})); err != nil {
			return err
		}
		if err := responsibilities.AddCost(ctx, quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        target.TenantID,
				Kind:            "sandbox_slot",
				Key:             target.ID,
				SourceOperation: acceptedOperation.ID,
			},
			Slots: 1,
		}); err != nil {
			return err
		}
		if err := responsibilities.AddCostReference(ctx, quota.CostReference{
			TenantID:   target.TenantID,
			Kind:       "sandbox_slot",
			Key:        target.ID,
			HolderKind: "active_use",
			HolderID:   forkSlotHolder(acceptedOperation.ID, target.ID),
		}); err != nil {
			return err
		}
	}
	if err := responsibilities.Reserve(ctx, quota.Reservation{
		Owner:     "fork:" + acceptedOperation.ID,
		Bytes:     budget.Bytes,
		MemoryMiB: budget.Memory,
		CPUs:      int64(budget.CPUs),
	}); err != nil {
		return err
	}
	if err := responsibilities.AddCost(ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        input.OwnerTenantID,
			Kind:            "pending_storage",
			Key:             acceptedOperation.ID + ":fork_group",
			SourceOperation: acceptedOperation.ID,
		},
		StorageMiB: budget.Storage,
	}); err != nil {
		return err
	}
	return responsibilities.AddCostReference(ctx, quota.CostReference{
		TenantID:   input.OwnerTenantID,
		Kind:       "pending_storage",
		Key:        acceptedOperation.ID + ":fork_group",
		HolderKind: "engine_dependency",
		HolderID:   acceptedOperation.ID + ":fork_group",
	})
}

// consumeForkForecast removes only the forecast. Real branch reservations replace
// it in this same transaction, so neither storage nor host responsibility lapses.
func consumeForkForecast(ctx context.Context, tx *sql.Tx, step ForkStep) error {
	budget := forkFunding(step.Input)
	responsibilities := quota.New(tx)
	if err := responsibilities.ReleaseMatchingReservation(ctx, quota.Reservation{
		Owner:     "fork:" + step.OperationID,
		Bytes:     budget.Bytes,
		MemoryMiB: budget.Memory,
		CPUs:      int64(budget.CPUs),
	}); err != nil {
		return err
	}
	if err := responsibilities.RemoveCostReference(ctx, quota.CostReference{
		TenantID:   step.Input.OwnerTenantID,
		Kind:       "pending_storage",
		Key:        step.OperationID + ":fork_group",
		HolderKind: "engine_dependency",
		HolderID:   step.OperationID + ":fork_group",
	}); err != nil {
		return err
	}
	return responsibilities.RetireMatchingCost(ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        step.Input.OwnerTenantID,
			Kind:            "pending_storage",
			Key:             step.OperationID + ":fork_group",
			SourceOperation: step.OperationID,
		},
		StorageMiB: budget.Storage,
	})
}

func retireUnallocatedFork(ctx context.Context, tx *sql.Tx, step ForkStep) error {
	if len(step.State.Children) != 0 {
		return fmt.Errorf("allocated fork cannot retire a forecast")
	}
	if err := consumeForkForecast(ctx, tx, step); err != nil {
		return err
	}
	for _, target := range step.Input.Targets {
		current, err := LoadSandbox(ctx, tx, target.ID, target.TenantID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, target) {
			return fmt.Errorf("%w: unallocated fork placement changed", errs.ErrManualRecovery)
		}
		links, err := dbgen.New(tx).CountForkBranchLinks(ctx, step.OperationID)
		if err != nil {
			return err
		}
		if links != 0 {
			return fmt.Errorf("%w: unallocated fork retains a branch dependency", errs.ErrManualRecovery)
		}
		terminated := current
		terminated.State = "terminated"
		terminated.OperationID = ""
		responsibilities := quota.New(tx)
		if err = responsibilities.RemoveCostReference(ctx, quota.CostReference{
			TenantID:   target.TenantID,
			Kind:       "sandbox_slot",
			Key:        target.ID,
			HolderKind: "active_use",
			HolderID:   forkSlotHolder(step.OperationID, target.ID),
		}); err != nil {
			return err
		}
		if err = responsibilities.RetireMatchingCost(ctx, quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        target.TenantID,
				Kind:            "sandbox_slot",
				Key:             target.ID,
				SourceOperation: step.OperationID,
			},
			Slots: 1,
		}); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).TerminateUnallocatedSandbox(ctx, dbgen.TerminateUnallocatedSandboxParams{
			ID:           target.ID,
			Tenant:       target.TenantID,
			PreviousBody: operation.Encode(current),
			NextBody:     operation.Encode(terminated),
		})); err != nil {
			return err
		}
	}
	return nil
}
