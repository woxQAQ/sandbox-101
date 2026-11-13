package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

// admitProvisionIn joins resource intent and all charges to the caller's Haven
// transaction. A batch commits every destination before dispatching any effect.
func (service *Service) admitProvisionIn(ctx context.Context, tx *sql.Tx, tenantID, key string, frozen []byte, hash string, template templateRecord, image host.ImageManifest, restore *restoreSource, claim *claimAdmission, target *provisionTarget, free int64) (Operation, error) {
	var empty Operation
	request := ColdRequest{TenantID: tenantID, Key: key, TemplateID: template.ID}
	if claim != nil {
		current, err := loadOwnedTemplate(ctx, tx, tenantID, template.ID)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(current, template) {
			return empty, loom.Fail(loom.Conflict, "template publication changed during preflight")
		}
	}
	provisional := false
	if target != nil {
		if declared, ok := provisionalTemplate(target.Parent); ok {
			if !reflect.DeepEqual(declared, template) {
				return empty, loom.Fail(loom.Conflict, "provisional resource template differs from supply intent")
			}
			provisional = true
		}
	}
	if restore == nil && !provisional {
		loader := loadColdTemplate
		if claim != nil {
			loader = loadOwnedTemplate
		}
		current, err := loader(ctx, tx, request.TenantID, request.TemplateID)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(current, template) {
			return empty, loom.Fail(loom.Conflict, "template publication changed during preflight")
		}
	} else if restore != nil {
		current, err := service.loadRestoreSource(ctx, tx, tenantID, restore.Snapshot.ID)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(current, *restore) {
			return empty, loom.Fail(loom.Conflict, "snapshot publication changed during preflight")
		}
	}
	currentImage, err := loadImage(ctx, tx, template.Config.Image)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(currentImage, image) {
		return empty, loom.Fail(loom.Conflict, "image publication changed during preflight")
	}
	var tenant tenantRecord
	var body []byte
	if err = tx.QueryRowContext(ctx, "SELECT body FROM tenants WHERE id=?", request.TenantID).Scan(&body); err != nil {
		return empty, err
	}
	if err = json.Unmarshal(body, &tenant); err != nil {
		return empty, err
	}
	var slots, storage int64
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id=?", request.TenantID).Scan(&slots, &storage); err != nil {
		return empty, err
	}
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 {
		return empty, fmt.Errorf("invalid tenant budget")
	}
	reuseSlot := target != nil && target.Sandbox.ID != ""
	budget := slotBudget{Kind: "sandbox_slot", Units: 1}
	if target != nil && target.Budget != nil {
		budget = *target.Budget
	}
	if (budget.Kind != "sandbox_slot" && budget.Kind != "build_slot") || budget.Units < 0 || budget.Units > 1 {
		return empty, loom.Fail(loom.Invalid, "invalid slot budget")
	}
	newSlots := budget.Units
	if reuseSlot {
		newSlots = 0
	}
	if slots > tenant.MaxSandboxes || newSlots > tenant.MaxSandboxes-slots || storage > tenant.MaxStorageMiB || template.Config.Shape.DiskMiB > tenant.MaxStorageMiB-storage {
		return empty, loom.Fail(loom.Exhausted, "tenant quota exhausted")
	}
	var held, memory int64
	var cpus int
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(bytes),0),COALESCE(SUM(memory),0),COALESCE(SUM(cpus),0) FROM reservations").Scan(&held, &memory, &cpus); err != nil {
		return empty, err
	}
	shape := template.Config.Shape
	vmBytes := shape.DiskMiB << 20
	var restoreBytes int64
	if restore != nil {
		vmBytes += shape.MemoryMiB << 20
		restoreBytes = (shape.MemoryMiB + shape.DiskMiB + 64) << 20
	}
	if held > free || service.opts.SafetyBytes > free-held || vmBytes+restoreBytes > free-held-service.opts.SafetyBytes || memory > service.opts.MaxMemoryMiB || shape.MemoryMiB+128 > service.opts.MaxMemoryMiB-memory || cpus > service.opts.MaxVCPUs || shape.VCPUs > service.opts.MaxVCPUs-cpus {
		return empty, loom.Fail(loom.Exhausted, "host reservation exhausted")
	}
	rows, err := tx.QueryContext(ctx, "SELECT slot FROM vms ORDER BY slot")
	if err != nil {
		return empty, err
	}
	slot := 1
	for rows.Next() {
		var allocated int
		if err = rows.Scan(&allocated); err != nil {
			break
		}
		if slot == allocated {
			slot++
		}
	}
	if err = errorsJoinRows(err, rows); err != nil {
		return empty, err
	}
	if slot >= 32768 {
		return empty, loom.Fail(loom.Exhausted, "network address pool exhausted")
	}
	operation := Operation{ID: newID("op-"), Key: request.Key, Status: "pending", Result: operationResult{SandboxID: newID("sandbox_"), VMID: newID("vm-")}}
	if target != nil {
		if reuseSlot {
			operation.Result.SandboxID = target.Sandbox.ID
		} else if target.NewSandboxID != "" {
			operation.Result.SandboxID = target.NewSandboxID
		}
	}
	resource := loom.Resource{VMID: operation.Result.VMID, Instance: newID("run-"), Config: template.Config, Slot: slot, NetworkToken: newID("net-")}
	resource.Dir = filepath.Join(service.store.directory, "vms", resource.VMID)
	resource.Disk = filepath.Join(resource.Dir, "disks", operation.ID+".raw")
	resource.RunDir = filepath.Join(resource.Dir, "runs", resource.Instance)
	resource.ImageDir = filepath.Join(service.store.directory, "images", resource.Config.Image[7:])
	action := "cold_start"
	if restore != nil {
		action = "clone"
		resource.Disk = filepath.Join(resource.Dir, "versions", operation.ID, "disk.raw")
		resource.RestoreDir = filepath.Join(resource.Dir, "versions", operation.ID, "restore")
	}
	input := stepInput{SlotBudget: budget, DiskKey: resource.VMID, SlotOperation: operation.ID, RuntimeBytes: vmBytes, Resource: resource, Image: image, TemplateID: template.ID, TenantID: request.TenantID, SandboxID: operation.Result.SandboxID, Restore: restore}
	if reuseSlot {
		current, err := loadSandbox(ctx, tx, target.Sandbox.ID, tenantID)
		if err != nil {
			return empty, err
		}
		validator, ok := target.Parent.(provisionTargetValidator)
		if restore == nil || !ok || target.SlotOperation == "" || target.SlotHolder == "" || !reflect.DeepEqual(current, target.Sandbox) {
			return empty, loom.Fail(loom.Conflict, "provision target ownership changed")
		}
		failure, err := validator.validateProvisionTarget(ctx, service, tx, current, *restore, target.SlotHolder)
		if err != nil {
			return empty, err
		}
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND sandbox_slots=1 AND storage_mib=0 AND source_operation_id=?", tenantID, current.ID, target.SlotOperation).Scan(&count); err != nil || count != 1 {
			return empty, loom.Fail(loom.ManualRecovery, "provision slot responsibility missing")
		}
		input.SlotOperation = target.SlotOperation
		input.SlotAssignment = current.SlotAssignment
		input.FailureSlotDestination = failure
		if err = service.admitResourceParent(ctx, tx, target.Parent, operation, action, resource.VMID, hash); err != nil {
			return empty, err
		}
	}
	if restore != nil {
		input.BackingHolder = resource.VMID
	}
	provisionSource := "cold"
	if restore != nil {
		provisionSource = "golden"
	}
	identity := newSandboxIdentity(template, provisionSource)
	if reuseSlot {
		identity = target.Sandbox.Identity
	}
	if target != nil && target.Identity != nil {
		identity = *target.Identity
	}
	sandbox := sandboxRecord{Identity: identity, SlotAssignment: input.SlotAssignment, ID: input.SandboxID, TenantID: request.TenantID, OperationID: operation.ID, VMID: resource.VMID, State: "provisioning", SourceOperation: operation.ID}
	vm := vmRecord{Resource: resource}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{operation.ID, "tenant", request.TenantID, request.Key, hash, frozen, operation.Status, encode(operation.Result)}},
		{"INSERT INTO operation_steps VALUES(?,?,?,?,?,?,?,?,?)", []any{operation.ID, "resource", action, digest(encode(input)), encode(input), resource.VMID, "", "pending", encode(stepState{Phase: "admitted"})}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if !reuseSlot {
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO sandboxes VALUES(?,?,?,?)", sandbox.ID, sandbox.TenantID, sandbox.State, encode(sandbox))); err != nil {
			return empty, err
		}
	} else {
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE sandboxes SET state='provisioning',body=? WHERE id=? AND tenant=? AND state=? AND body=?", encode(sandbox), sandbox.ID, sandbox.TenantID, target.Sandbox.State, encode(target.Sandbox))); err != nil {
			return empty, err
		}
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE quota_refs SET holder_id=? WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", "sandbox:"+sandbox.ID, tenantID, sandbox.ID, target.SlotHolder)); err != nil {
			return empty, err
		}
	}
	if target != nil && !reuseSlot && target.Parent != nil {
		if err = service.admitResourceParent(ctx, tx, target.Parent, operation, action, resource.VMID, hash); err != nil {
			return empty, err
		}
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO vms VALUES(?,?,?,?,?,?)", []any{resource.VMID, slot, "", "creating", operation.ID, encode(vm)}},
		{"INSERT INTO instances VALUES(?,?,?,?)", []any{resource.Instance, resource.VMID, operation.ID, encode(vm)}},
		{"INSERT INTO resource_versions VALUES(?,?,?,?,?)", []any{operation.ID, resource.VMID, operation.ID, "resource", encode(resourceVersion{Resource: resource})}},
		{"INSERT INTO reservations VALUES(?,?,?,?)", []any{"vm:" + resource.VMID, vmBytes, shape.MemoryMiB + 128, shape.VCPUs}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	for _, quota := range []struct {
		kind, key          string
		slots, storage     int64
		holderKind, holder string
	}{
		{budget.Kind, sandbox.ID, budget.Units, 0, "active_use", "sandbox:" + sandbox.ID},
		{"pending_storage", operation.ID + ":resource", 0, shape.DiskMiB, "engine_dependency", operation.ID + ":resource"},
	} {
		if reuseSlot && quota.kind == budget.Kind {
			continue
		}
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO quota_resources VALUES(?,?,?,?,?,?)", request.TenantID, quota.kind, quota.key, quota.slots, quota.storage, operation.ID)); err != nil {
			return empty, err
		}
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO quota_refs VALUES(?,?,?,?,?)", request.TenantID, quota.kind, quota.key, quota.holderKind, quota.holder)); err != nil {
			return empty, err
		}
	}
	for _, reference := range []struct{ kind, id string }{{"vm", resource.VMID}, {"image", resource.Config.Image}} {
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO resource_refs VALUES(?,?,?,?)", reference.kind, reference.id, "step", operation.ID+":resource")); err != nil {
			return empty, err
		}
	}
	if restore != nil {
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"INSERT INTO reservations VALUES(?,?,0,0)", []any{"restore:" + operation.ID, restoreBytes}},
			{"INSERT INTO source_leases VALUES(?,?,'resource')", []any{restore.Snapshot.ID, operation.ID}},
			{"INSERT INTO resource_refs VALUES('snapshot',?,'step',?)", []any{restore.Snapshot.ID, operation.ID + ":resource"}},
			{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'engine_dependency',?)", []any{tenantID, restore.Snapshot.ID, operation.ID + ":resource"}},
		} {
			if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
				return empty, err
			}
		}
	}
	activation := claim
	if target != nil && target.Activation != nil {
		if activation != nil {
			return empty, errors.New("resource has two activation admissions")
		}
		activation = target.Activation
	}
	if activation != nil {
		if err = service.admitActivation(tx, operation, input, *activation); err != nil {
			return empty, err
		}
	}
	return operation, nil
}
