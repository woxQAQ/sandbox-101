package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

type forkBudget struct {
	Bytes, Memory, Storage int64
	CPUs                   int
}

func forkFunding(input forkInput) forkBudget {
	shape := input.Template.Config.Shape
	count := int64(len(input.Targets))
	return forkBudget{Bytes: count * ((2*(shape.MemoryMiB+shape.DiskMiB) + 64) << 20), Memory: count * (shape.MemoryMiB + 128), Storage: count * shape.DiskMiB, CPUs: int(count) * shape.VCPUs}
}

func (service *Service) reserveForkTargets(ctx context.Context, tx *sql.Tx, operation Operation, input forkInput, free int64) error {
	budget := forkFunding(input)
	var tenant tenantRecord
	var body []byte
	if err := tx.QueryRowContext(ctx, "SELECT body FROM tenants WHERE id=?", input.OwnerTenantID).Scan(&body); err != nil {
		return err
	}
	if err := json.Unmarshal(body, &tenant); err != nil {
		return err
	}
	var slots, storage, held, memory int64
	var cpus int
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id=?", input.OwnerTenantID).Scan(&slots, &storage); err != nil {
		return err
	}
	var captureStorage, captureBytes int64
	if input.Source != nil {
		shape := input.Template.Config.Shape
		captureStorage = shape.MemoryMiB + shape.DiskMiB
		captureBytes = (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20
	}
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 || slots > tenant.MaxSandboxes || int64(len(input.Targets)) > tenant.MaxSandboxes-slots || storage > tenant.MaxStorageMiB || budget.Storage+captureStorage > tenant.MaxStorageMiB-storage {
		return loom.Fail(loom.Exhausted, "fork batch exceeds tenant quota")
	}
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(bytes),0),COALESCE(SUM(memory),0),COALESCE(SUM(cpus),0) FROM reservations").Scan(&held, &memory, &cpus); err != nil {
		return err
	}
	if held > free || service.opts.SafetyBytes > free-held || budget.Bytes+captureBytes > free-held-service.opts.SafetyBytes || memory > service.opts.MaxMemoryMiB || budget.Memory > service.opts.MaxMemoryMiB-memory || cpus > service.opts.MaxVCPUs || budget.CPUs > service.opts.MaxVCPUs-cpus {
		return loom.Fail(loom.Exhausted, "fork batch exceeds host reservation")
	}
	for _, target := range input.Targets {
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"INSERT INTO sandboxes VALUES(?,?,?,?)", []any{target.ID, target.TenantID, target.State, encode(target)}},
			{"INSERT INTO sandbox_settings VALUES(?,?)", []any{target.ID, encode(input.Settings)}},
			{"INSERT INTO quota_resources VALUES(?,'sandbox_slot',?,1,0,?)", []any{target.TenantID, target.ID, operation.ID}},
			{"INSERT INTO quota_refs VALUES(?,'sandbox_slot',?,'active_use',?)", []any{target.TenantID, target.ID, forkSlotHolder(operation.ID, target.ID)}},
		} {
			if err := requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
				return err
			}
		}
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO reservations VALUES(?,?,?,?)", []any{"fork:" + operation.ID, budget.Bytes, budget.Memory, budget.CPUs}},
		{"INSERT INTO quota_resources VALUES(?,'pending_storage',?,0,?,?)", []any{input.OwnerTenantID, operation.ID + ":fork_group", budget.Storage, operation.ID}},
		{"INSERT INTO quota_refs VALUES(?,'pending_storage',?,'engine_dependency',?)", []any{input.OwnerTenantID, operation.ID + ":fork_group", operation.ID + ":fork_group"}},
	} {
		if err := requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}

// consumeForkForecast removes only the forecast. Real branch reservations replace
// it in this same transaction, so neither storage nor host responsibility lapses.
func consumeForkForecast(ctx context.Context, tx *sql.Tx, step forkStep) error {
	budget := forkFunding(step.Input)
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM reservations WHERE owner=? AND bytes=? AND memory=? AND cpus=?", []any{"fork:" + step.OperationID, budget.Bytes, budget.Memory, budget.CPUs}},
		{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{step.Input.OwnerTenantID, step.OperationID + ":fork_group", step.OperationID + ":fork_group"}},
		{"DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND sandbox_slots=0 AND storage_mib=? AND source_operation_id=?", []any{step.Input.OwnerTenantID, step.OperationID + ":fork_group", budget.Storage, step.OperationID}},
	} {
		if err := requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}

func retireUnallocatedFork(ctx context.Context, tx *sql.Tx, step forkStep) error {
	if len(step.State.Children) != 0 {
		return fmt.Errorf("allocated fork cannot retire a forecast")
	}
	if err := consumeForkForecast(ctx, tx, step); err != nil {
		return err
	}
	for _, target := range step.Input.Targets {
		current, err := loadSandbox(ctx, tx, target.ID, target.TenantID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, target) {
			return loom.Fail(loom.ManualRecovery, "unallocated fork placement changed")
		}
		var links int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM operation_steps WHERE operation_id=? AND step_key LIKE 'link:branch:%'", step.OperationID).Scan(&links); err != nil {
			return err
		}
		if links != 0 {
			return loom.Fail(loom.ManualRecovery, "unallocated fork retains a branch dependency")
		}
		terminated := current
		terminated.State = "terminated"
		terminated.OperationID = ""
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{target.TenantID, target.ID, forkSlotHolder(step.OperationID, target.ID)}},
			{"DELETE FROM quota_resources WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND sandbox_slots=1 AND storage_mib=0 AND source_operation_id=?", []any{target.TenantID, target.ID, step.OperationID}},
			{"UPDATE sandboxes SET state='terminated',body=? WHERE id=? AND tenant=? AND state='provisioning' AND body=?", []any{encode(terminated), target.ID, target.TenantID, encode(current)}},
		} {
			if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
				return err
			}
		}
	}
	return nil
}
