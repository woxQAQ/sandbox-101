package haven

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) checkProvisionIntent(tx *sql.Tx, step resourceStep) (vmRecord, sandboxRecord, operationResult, error) {
	var result operationResult
	var request, body []byte
	var hash, kind, tenant, status string
	err := tx.QueryRowContext(service.ctx, "SELECT request_hash,request,scope_kind,scope_id,status,body FROM operations WHERE id=?", step.OperationID).Scan(&hash, &request, &kind, &tenant, &status, &body)
	if err != nil {
		return vmRecord{}, sandboxRecord{}, result, err
	}
	if digest(request) != hash || kind != "tenant" || tenant != step.Input.TenantID || terminal(status) {
		return vmRecord{}, sandboxRecord{}, result, fmt.Errorf("cold operation identity or lifecycle changed")
	}
	switch step.Action {
	case "restore":
		var frozen struct {
			Action  string
			Request RestoreRequest
		}
		if err = json.Unmarshal(request, &frozen); err != nil {
			return vmRecord{}, sandboxRecord{}, result, err
		}
		if step.Input.Restore == nil || frozen.Action != "restore" || frozen.Request.TenantID != tenant || frozen.Request.SandboxID != step.Input.SandboxID || frozen.Request.SnapshotID != step.Input.Restore.Snapshot.ID || frozen.Request.ExpectedInstance != step.ExpectedInstance || frozen.Request.StopOperationID != step.Input.StopOperation {
			return vmRecord{}, sandboxRecord{}, result, fmt.Errorf("restore request differs from frozen predecessor")
		}
		stop, err := loadStop(service.ctx, tx, step.Input.StopOperation)
		if err != nil {
			return vmRecord{}, sandboxRecord{}, result, err
		}
		if stop.Status != "succeeded" || stop.Phase != "complete" || len(step.Input.Ancestors) == 0 || stop.Input.SourceOperation != step.Input.Ancestors[len(step.Input.Ancestors)-1].OperationID || stop.Input.Request.ExpectedInstance != step.ExpectedInstance {
			return vmRecord{}, sandboxRecord{}, result, fmt.Errorf("restore stop history changed")
		}
	case "clone":
		var frozen struct {
			Action  string
			Request CloneRequest
		}
		if err = json.Unmarshal(request, &frozen); err != nil {
			return vmRecord{}, sandboxRecord{}, result, err
		}
		if step.Input.Restore == nil || frozen.Action != "clone" || frozen.Request.TenantID != tenant || frozen.Request.SnapshotID != step.Input.Restore.Snapshot.ID {
			return vmRecord{}, sandboxRecord{}, result, fmt.Errorf("clone request differs from frozen source")
		}
	default:
		var frozen struct {
			Action  string
			Request ColdRequest
		}
		if err = json.Unmarshal(request, &frozen); err != nil {
			return vmRecord{}, sandboxRecord{}, result, err
		}
		if step.Action != "cold_start" || frozen.Action != "claim" || frozen.Request.TenantID != tenant || frozen.Request.TemplateID != step.Input.TemplateID {
			return vmRecord{}, sandboxRecord{}, result, fmt.Errorf("cold request differs from frozen step")
		}
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return vmRecord{}, sandboxRecord{}, result, err
	}
	if result.SandboxID != step.Input.SandboxID || result.VMID != step.Candidate {
		return vmRecord{}, sandboxRecord{}, result, fmt.Errorf("cold result destination changed")
	}
	sandbox, err := loadSandbox(service.ctx, tx, step.Input.SandboxID, tenant)
	if err != nil {
		return vmRecord{}, sandbox, result, err
	}
	if sandbox.OperationID != step.OperationID || sandbox.SourceOperation != step.OperationID || sandbox.VMID != step.Candidate || sandbox.State != "provisioning" {
		return vmRecord{}, sandbox, result, fmt.Errorf("cold sandbox destination changed")
	}
	vm, instance, owner, err := service.loadVM(service.ctx, tx, step)
	if err != nil {
		return vm, sandbox, result, err
	}
	if owner != step.OperationID || vm.Deleting || instance != "" && !expectedCurrentInstance(step, instance) {
		return vm, sandbox, result, fmt.Errorf("cold VM ownership changed")
	}
	if _, err = service.generationHistory(service.ctx, tx, step); err != nil {
		return vm, sandbox, result, err
	}
	var count int
	if step.Action == "cold_start" {
		if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM template_intents WHERE template_id=? AND tenant_id=?", step.Input.TemplateID, tenant).Scan(&count); err != nil || count != 1 {
			return vm, sandbox, result, fmt.Errorf("template owner proof missing: %v", err)
		}
	} else if err = service.restoreDependencies(tx, step); err != nil {
		return vm, sandbox, result, err
	}
	image, err := loadImage(service.ctx, tx, vm.Resource.Config.Image)
	if err != nil || !reflect.DeepEqual(image, step.Input.Image) {
		return vm, sandbox, result, fmt.Errorf("image publication proof changed: %v", err)
	}
	var candidateBody, versionBody []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT body FROM instances WHERE id=? AND vm_id=? AND operation_id=?", vm.Resource.Instance, vm.Resource.VMID, step.OperationID).Scan(&candidateBody); err != nil {
		return vm, sandbox, result, err
	}
	var candidate vmRecord
	if err = json.Unmarshal(candidateBody, &candidate); err != nil || !reflect.DeepEqual(candidate.Resource, vm.Resource) {
		return vm, sandbox, result, fmt.Errorf("instance candidate changed: %v", err)
	}
	if err = tx.QueryRowContext(service.ctx, "SELECT body FROM resource_versions WHERE id=? AND vm_id=? AND operation_id=? AND step_key=?", step.OperationID, step.Candidate, step.OperationID, step.Key).Scan(&versionBody); err != nil {
		return vm, sandbox, result, err
	}
	var version resourceVersion
	if err = json.Unmarshal(versionBody, &version); err != nil || !reflect.DeepEqual(version.Resource, vm.Resource) || !reflect.DeepEqual(version.DiskIdentity, step.State.DiskIdentity) {
		return vm, sandbox, result, fmt.Errorf("resource version changed: %v", err)
	}
	shape := vm.Resource.Config.Shape
	vmBytes := step.Input.RuntimeBytes
	checks := []struct {
		query string
		args  []any
	}{
		{"SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=? AND cpus=?", []any{"vm:" + step.Candidate, vmBytes, shape.MemoryMiB + 128, shape.VCPUs}},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND sandbox_slots=0 AND storage_mib=? AND source_operation_id=?", []any{tenant, step.OperationID + ":resource", shape.DiskMiB, step.OperationID}},
		{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{tenant, step.OperationID + ":resource", step.OperationID + ":resource"}},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=? AND sandbox_slots=? AND storage_mib=0 AND source_operation_id=?", []any{tenant, step.Input.SlotBudget.Kind, sandbox.ID, step.Input.SlotBudget.Units, step.Input.SlotOperation}},
		{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{tenant, step.Input.SlotBudget.Kind, sandbox.ID, "sandbox:" + sandbox.ID}},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{step.Candidate, step.OperationID + ":resource"}},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{vm.Resource.Config.Image, step.OperationID + ":resource"}},
	}
	for _, check := range checks {
		if err = tx.QueryRowContext(service.ctx, check.query, check.args...).Scan(&count); err != nil || count != 1 {
			return vm, sandbox, result, fmt.Errorf("cold responsibility proof missing: %v", err)
		}
	}
	return vm, sandbox, result, nil
}

func (service *Service) completeCold(step resourceStep, observation loom.Observation) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadStep(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.Input, step.Input) || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "agent_ready" || current.State.DiskIdentity == nil || current.State.Witness != "" {
		return fmt.Errorf("cold completion lacks its original ready candidate")
	}
	vm, sandbox, result, err := service.checkProvisionIntent(tx, current)
	if err != nil {
		return err
	}
	vm.Verified, vm.AgentReady, vm.Observation = true, true, &observation
	r := vm.Resource
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE vms SET state='running',operation_id=NULL,body=? WHERE id=? AND current_instance=? AND operation_id=? AND state='created'", encode(vm), r.VMID, r.Instance, step.OperationID)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE instances SET body=? WHERE id=? AND vm_id=? AND operation_id=?", encode(vm), r.Instance, r.VMID, step.OperationID)); err != nil {
		return err
	}
	sandbox.Instance = r.Instance
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE sandboxes SET body=? WHERE id=? AND tenant=? AND state='provisioning'", encode(sandbox), sandbox.ID, sandbox.TenantID)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO quota_resources VALUES(?,'vm_disk',?,0,?,?)", sandbox.TenantID, current.Input.DiskKey, r.Config.Shape.DiskMiB, step.OperationID)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO quota_refs VALUES(?,'vm_disk',?,'active_use',?)", sandbox.TenantID, current.Input.DiskKey, "sandbox:"+sandbox.ID)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM quota_refs WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", sandbox.TenantID, step.OperationID+":resource", step.OperationID+":resource")); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?", sandbox.TenantID, step.OperationID+":resource")); err != nil {
		return err
	}
	for _, reference := range []struct{ kind, id, holderKind, holder string }{{"vm", r.VMID, "sandbox", sandbox.ID}, {"image", r.Config.Image, "vm", r.VMID}} {
		if current.Action != "restore" || reference.kind != "image" {
			if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO resource_refs VALUES(?,?,?,?)", reference.kind, reference.id, reference.holderKind, reference.holder)); err != nil {
				return err
			}
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM resource_refs WHERE resource_kind=? AND resource_id=? AND holder_kind='step' AND holder_id=?", reference.kind, reference.id, step.OperationID+":resource")); err != nil {
			return err
		}
	}
	next := current.State
	next.Phase, next.Witness, next.Observation = "complete", r.Instance, &observation
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key=? AND status='running' AND input_hash=?", encode(next), step.OperationID, step.Key, step.Hash)); err != nil {
		return err
	}
	result.ResourceReady = true
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='running',body=? WHERE id=? AND status IN ('pending','running')", encode(result), step.OperationID)); err != nil {
		return err
	}
	if err = service.fault("completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("step_completed")
}
