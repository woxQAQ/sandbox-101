package sandbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/quota"
)

// provisionBudget is the checked allocation cost, reused unchanged by the
// subsequent intent, reservation and charge writes in the same transaction.
type provisionBudget struct {
	Slot         quota.SlotBudget
	ReuseSlot    bool
	VMBytes      int64
	RestoreBytes int64
}

func (service *VMProvisioning) checkProvisionBudget(ctx context.Context, tx *sql.Tx, admission ProvisionAdmission, free int64) (provisionBudget, error) {
	var empty provisionBudget
	request, target := admission.Request, admission.Target
	tenant, err := authorization.New(tx).Limits(ctx, request.TenantID)
	if err != nil {
		return empty, err
	}
	usage, err := quota.New(tx).Usage(ctx, request.TenantID)
	if err != nil {
		return empty, err
	}
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 {
		return empty, fmt.Errorf("invalid tenant budget")
	}
	budget := provisionBudget{
		Slot: quota.SlotBudget{
			Kind:  "sandbox_slot",
			Units: 1,
		},
		ReuseSlot: target != nil && target.Sandbox.ID != "",
	}
	if target != nil && target.Budget != nil {
		budget.Slot = *target.Budget
	}
	if (budget.Slot.Kind != "sandbox_slot" && budget.Slot.Kind != "build_slot") || budget.Slot.Units < 0 || budget.Slot.Units > 1 {
		return empty, fmt.Errorf("%w: invalid slot budget", errs.ErrInvalid)
	}
	newSlots := budget.Slot.Units
	if budget.ReuseSlot {
		newSlots = 0
	}
	shape := request.Template.Config.Shape
	if usage.Slots > tenant.MaxSandboxes || newSlots > tenant.MaxSandboxes-usage.Slots || usage.StorageMiB > tenant.MaxStorageMiB || shape.DiskMiB > tenant.MaxStorageMiB-usage.StorageMiB {
		return empty, fmt.Errorf("%w: tenant quota exhausted", errs.ErrExhausted)
	}
	reserved, err := quota.New(tx).ReservedCapacity(ctx)
	if err != nil {
		return empty, err
	}
	budget.VMBytes = shape.DiskMiB << 20
	if request.Restore != nil {
		budget.VMBytes += shape.MemoryMiB << 20
		budget.RestoreBytes = (shape.MemoryMiB + shape.DiskMiB + 64) << 20
	}
	if reserved.Bytes > free || service.capacity.SafetyBytes > free-reserved.Bytes || budget.VMBytes+budget.RestoreBytes > free-reserved.Bytes-service.capacity.SafetyBytes || reserved.MemoryMiB > service.capacity.MaxMemoryMiB || shape.MemoryMiB+128 > service.capacity.MaxMemoryMiB-reserved.MemoryMiB || reserved.CPUs > int64(service.capacity.MaxVCPUs) || int64(shape.VCPUs) > int64(service.capacity.MaxVCPUs)-reserved.CPUs {
		return empty, fmt.Errorf("%w: host reservation exhausted", errs.ErrExhausted)
	}
	return budget, nil
}
