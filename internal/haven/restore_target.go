package haven

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

type RestoreRequest struct {
	TenantID         string `json:"tenant_id"`
	Key              string `json:"key"`
	SandboxID        string `json:"sandbox_id"`
	SnapshotID       string `json:"snapshot_id"`
	ExpectedInstance string `json:"expected_instance"`
	StopOperationID  string `json:"stop_operation_id"`
}

func (service *Service) AdmitRestore(ctx context.Context, request RestoreRequest) (Operation, error) {
	return service.admitRestore(ctx, request, nil)
}

func (service *Service) admitRestore(ctx context.Context, request RestoreRequest, parent resourceParent) (Operation, error) {
	link := lifecycleParent(parent)
	var empty Operation
	if request.TenantID == "" || request.SandboxID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.SnapshotID, "snap-") || !loom.ValidResourceID(request.ExpectedInstance, "run-") || !loom.ValidResourceID(request.StopOperationID, "op-") {
		return empty, loom.Fail(loom.Invalid, "restore requires scoped snapshot, stopped instance, stop proof and key")
	}
	frozen := encode(struct {
		Action  string
		Request RestoreRequest
	}{"restore", request})
	hash := digest(frozen)
	if cached, found, err := cachedOperation(ctx, service.store.db, request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	snapshot, err := service.loadRestoreSource(ctx, service.store.db, request.TenantID, request.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = service.opts.Backend.Check(ctx, snapshot.Snapshot.Manifest.Config); err != nil {
		return empty, err
	}
	image, err := loadImage(ctx, service.store.db, snapshot.Snapshot.Manifest.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = verifyImageFiles(image, filepath.Join(service.store.directory, "images", image.Digest[7:])); err != nil {
		return empty, err
	}
	free, err := loom.FreeBytes(service.store.directory)
	if err != nil {
		return empty, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	service.mu.Lock()
	closing := service.closing || service.store.reviewed
	service.mu.Unlock()
	if closing {
		return empty, fmt.Errorf("haven is closing")
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := cachedOperation(ctx, tx, request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	currentImage, err := loadImage(ctx, tx, image.Digest)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(currentImage, image) {
		return empty, loom.Fail(loom.Conflict, "image publication changed during preflight")
	}
	if link != nil && (link.Input.Rollback == nil || !reflect.DeepEqual(snapshot, link.Input.Rollback.Source)) {
		return empty, loom.Fail(loom.Conflict, "rollback frozen restore source changed")
	}
	currentSnapshot, err := service.loadRestoreSource(ctx, tx, request.TenantID, request.SnapshotID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(currentSnapshot, snapshot) {
		return empty, fmt.Errorf("restore source changed during preflight")
	}
	sandbox, err := loadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	if err = allocationCleanupFence(ctx, tx, sandbox.VMID, parent); err != nil {
		return empty, err
	}
	if (sandbox.State != "provisioning" && (link == nil || link.Input.Command.Action != "rollback" || sandbox.State != "running")) || sandbox.Instance != request.ExpectedInstance {
		return empty, fmt.Errorf("restore requires original undelivered stopped sandbox")
	}
	prior, err := loadStep(ctx, tx, sandbox.SourceOperation)
	if err != nil {
		return empty, err
	}
	if err = service.verifyGeneration(ctx, tx, prior); err != nil {
		return empty, err
	}
	if _, err = service.generationHistory(ctx, tx, prior); err != nil {
		return empty, err
	}
	stopped, err := loadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return empty, err
	}
	if stopped.Status != "succeeded" || stopped.Phase != "complete" || stopped.Input.SourceOperation != prior.OperationID || stopped.Input.SourceHash != prior.Hash || stopped.Input.Request.ExpectedInstance != request.ExpectedInstance || stopped.Input.Request.TenantID != request.TenantID || stopped.Input.Request.SandboxID != sandbox.ID {
		return empty, fmt.Errorf("restore stop proof differs from current generation")
	}
	vm, current, owner, err := service.loadVM(ctx, tx, prior)
	if err != nil {
		return empty, err
	}
	var phase string
	if err = tx.QueryRowContext(ctx, "SELECT state FROM vms WHERE id=?", prior.Candidate).Scan(&phase); err != nil {
		return empty, err
	}
	if owner != "" || phase != "stopped" || current != request.ExpectedInstance || current != prior.State.Witness || !vm.Verified || vm.AgentReady || vm.Deleting {
		return empty, fmt.Errorf("restore current stopped ownership changed")
	}
	config := snapshot.Snapshot.Manifest.Config
	if !reflect.DeepEqual(vm.Resource.Config.Shape, config.Shape) || vm.Resource.Config.Image != config.Image {
		return empty, fmt.Errorf("restore target shape/image mismatch")
	}
	var refs, instances, versions int
	if err = tx.QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=?),(SELECT COUNT(*) FROM instances WHERE vm_id=?),(SELECT COUNT(*) FROM resource_versions WHERE vm_id=?)", prior.Candidate, prior.Candidate, prior.Candidate).Scan(&refs, &instances, &versions); err != nil {
		return empty, err
	}
	if refs != 1 || instances != 1+len(prior.Input.Ancestors) || versions != instances {
		return empty, fmt.Errorf("restore target has unrecorded resource users or versions")
	}
	var tenant tenantRecord
	var body []byte
	if err = tx.QueryRowContext(ctx, "SELECT body FROM tenants WHERE id=?", request.TenantID).Scan(&body); err != nil {
		return empty, err
	}
	if err = json.Unmarshal(body, &tenant); err != nil {
		return empty, err
	}
	var cost, slots, held int64
	if err = tx.QueryRowContext(ctx, "SELECT SUM(storage_mib),SUM(sandbox_slots) FROM quota_resources WHERE tenant_id=?", request.TenantID).Scan(&cost, &slots); err != nil {
		return empty, err
	}
	shape := config.Shape
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 || slots > tenant.MaxSandboxes || cost > tenant.MaxStorageMiB || shape.DiskMiB > tenant.MaxStorageMiB-cost {
		return empty, fmt.Errorf("restore tenant peak quota exhausted")
	}
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(bytes),0) FROM reservations").Scan(&held); err != nil {
		return empty, err
	}
	reserve := (shape.MemoryMiB + shape.DiskMiB + 64) << 20
	if held > free || service.opts.SafetyBytes > free-held || reserve > free-held-service.opts.SafetyBytes {
		return empty, fmt.Errorf("restore host peak reservation exhausted")
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operationResult{
			SandboxID:  sandbox.ID,
			VMID:       prior.Candidate,
			SnapshotID: request.SnapshotID,
		},
	}
	resource := prior.Input.Resource
	resource.Instance = newID("run-")
	resource.Config = config
	resource.RunDir = filepath.Join(resource.Dir, "runs", resource.Instance)
	resource.RestoreDir = filepath.Join(resource.Dir, "versions", operation.ID, "restore")
	resource.Disk = filepath.Join(resource.Dir, "versions", operation.ID, "disk.raw")
	ancestors := append([]generationRef(nil), prior.Input.Ancestors...)
	ancestors = append(ancestors, generationRef{
		OperationID: prior.OperationID,
		InputHash:   prior.Hash,
	})
	budget, err := service.currentSlotBudget(ctx, tx, sandbox, prior)
	if err != nil {
		return empty, err
	}
	input := stepInput{
		SlotAssignment: sandbox.SlotAssignment,
		SlotBudget:     budget,
		TemplateID:     prior.Input.TemplateID,
		Resource:       resource,
		Image:          image,
		TenantID:       request.TenantID,
		SandboxID:      sandbox.ID,
		Restore:        &snapshot,
		Ancestors:      ancestors,
		DiskKey:        operation.ID,
		SlotOperation:  prior.Input.SlotOperation,
		RuntimeBytes:   prior.Input.RuntimeBytes,
		BackingHolder:  operation.ID,
		StopOperation:  request.StopOperationID,
	}
	if err = service.admitResourceParent(ctx, tx, parent, operation, "restore", resource.VMID, hash); err != nil {
		return empty, err
	}
	oldSandbox := sandbox
	vm = vmRecord{
		Resource: resource,
	}
	sandbox.State = "provisioning"
	sandbox.OperationID = operation.ID
	sandbox.SourceOperation = operation.ID
	sandbox.Instance = ""
	payload := encode(input)
	statements := []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{
			operation.ID,
			"tenant",
			request.TenantID,
			request.Key,
			hash,
			frozen,
			"pending",
			encode(operation.Result),
		}},
		{"INSERT INTO operation_steps VALUES(?,'resource','restore',?,?,?,?,'pending',?)", []any{
			operation.ID,
			digest(payload),
			payload,
			resource.VMID,
			request.ExpectedInstance,
			encode(stepState{
				Phase: "admitted",
			}),
		}},
		{"UPDATE vms SET operation_id=?,body=? WHERE id=? AND operation_id IS NULL AND current_instance=? AND state='stopped'", []any{
			operation.ID,
			encode(vm),
			resource.VMID,
			request.ExpectedInstance,
		}},
		{"UPDATE sandboxes SET state='provisioning',body=? WHERE id=? AND tenant=? AND state=? AND body=?", []any{
			encode(sandbox),
			sandbox.ID,
			request.TenantID,
			oldSandbox.State,
			encode(oldSandbox),
		}},
		{"INSERT INTO instances VALUES(?,?,?,?)", []any{
			resource.Instance,
			resource.VMID,
			operation.ID,
			encode(vm),
		}},
		{"INSERT INTO resource_versions VALUES(?, ?,?,'resource',?)", []any{
			operation.ID,
			resource.VMID,
			operation.ID,
			encode(resourceVersion{
				Resource: resource,
			}),
		}},
		{"INSERT INTO reservations VALUES(?,?,0,0)", []any{
			"restore:" + operation.ID,
			reserve,
		}},
		{"INSERT INTO quota_resources VALUES(?,'pending_storage',?,0,?,?)", []any{
			request.TenantID,
			operation.ID + ":resource",
			shape.DiskMiB,
			operation.ID,
		}},
		{"INSERT INTO quota_refs VALUES(?,'pending_storage',?,'engine_dependency',?)", []any{
			request.TenantID,
			operation.ID + ":resource",
			operation.ID + ":resource",
		}},
		{"INSERT INTO source_leases VALUES(?,?,'resource')", []any{
			request.SnapshotID,
			operation.ID,
		}},
		{"INSERT INTO resource_refs VALUES('snapshot',?,'step',?)", []any{
			request.SnapshotID,
			operation.ID + ":resource",
		}},
		{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'engine_dependency',?)", []any{
			request.TenantID,
			request.SnapshotID,
			operation.ID + ":resource",
		}},
		{"UPDATE resource_refs SET holder_kind='step',holder_id=? WHERE resource_kind='vm' AND resource_id=? AND holder_kind='sandbox' AND holder_id=?", []any{
			operation.ID + ":resource",
			resource.VMID,
			sandbox.ID,
		}},
		{"INSERT INTO resource_refs VALUES('image',?,'step',?)", []any{
			resource.Config.Image,
			operation.ID + ":resource",
		}},
	}
	for _, statement := range statements {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if err = service.fault("restore_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if err = service.fault("restore_admitted"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}
