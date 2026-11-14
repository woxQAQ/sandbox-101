package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

// AdmitCapture fixes the source instance, destination, authorization and peak
// responsibility before any pause. Sandbox checkpoint/pause publication is separate.
func (service *Service) AdmitCapture(ctx context.Context, request CaptureRequest) (Operation, error) {
	return service.admitCapture(ctx, request, nil)
}

func (service *Service) admitCapture(ctx context.Context, request CaptureRequest, parent resourceParent) (Operation, error) {
	var empty Operation
	if request.TenantID == "" || request.SandboxID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.ExpectedInstance, "run-") {
		return empty, loom.Fail(loom.Invalid, "capture requires tenant, sandbox, key and expected instance")
	}
	frozen := encode(struct {
		Action  string
		Request CaptureRequest
	}{"snapshot", request})
	hash := digest(frozen)
	if cached, found, err := cachedOperation(ctx, service.store.db, request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	free, err := loom.FreeBytes(service.store.directory)
	if err != nil {
		return empty, err
	}
	compatibility, cpu := service.opts.Backend.Compatibility(), host.CPUIdentity()
	service.admission.Lock()
	defer service.admission.Unlock()
	service.mu.Lock()
	closing := service.closing || service.store.reviewed
	service.mu.Unlock()
	if closing {
		return empty, loom.Fail(loom.Conflict, "Haven is closing")
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
	sandbox, err := loadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err == sql.ErrNoRows {
		return empty, loom.Fail(loom.NotFound, "sandbox not found")
	}
	if err != nil {
		return empty, err
	}
	if err = allocationCleanupFence(ctx, tx, sandbox.VMID, parent); err != nil {
		return empty, err
	}
	if sandbox.Instance != request.ExpectedInstance || (sandbox.State != "provisioning" && sandbox.State != "running") {
		return empty, loom.Fail(loom.Conflict, "capture source instance or state changed")
	}
	source, err := loadStep(ctx, tx, sandbox.SourceOperation)
	if err != nil {
		return empty, err
	}
	if err = service.validateStep(source); err != nil {
		return empty, err
	}
	if (source.Action != "cold_start" && source.Action != "clone") || source.Status != "succeeded" || source.State.Witness != request.ExpectedInstance || source.State.Observation == nil || source.State.DiskIdentity == nil || source.Input.TenantID != request.TenantID || source.Input.SandboxID != sandbox.ID || source.Candidate != sandbox.VMID {
		return empty, loom.Fail(loom.ManualRecovery, "capture requires original successful resource witness")
	}
	vm, instance, owner, err := service.loadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if owner != "" || instance != request.ExpectedInstance || vm.Deleting || !vm.Verified || !vm.AgentReady || !reflect.DeepEqual(vm.Resource, source.Input.Resource) {
		return empty, loom.Fail(loom.Conflict, "capture source is busy or unverified")
	}
	var tenant tenantRecord
	var body []byte
	if err = tx.QueryRowContext(ctx, "SELECT body FROM tenants WHERE id=?", request.TenantID).Scan(&body); err != nil {
		return empty, err
	}
	if err = json.Unmarshal(body, &tenant); err != nil {
		return empty, err
	}
	shape := vm.Resource.Config.Shape
	cost := shape.MemoryMiB + shape.DiskMiB
	var slots, storage, held int64
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id=?", request.TenantID).Scan(&slots, &storage); err != nil {
		return empty, err
	}
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 || slots > tenant.MaxSandboxes || storage > tenant.MaxStorageMiB || cost > tenant.MaxStorageMiB-storage {
		return empty, loom.Fail(loom.Exhausted, "capture tenant quota exhausted")
	}
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(bytes),0) FROM reservations").Scan(&held); err != nil {
		return empty, err
	}
	reserve := (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20
	if held > free || service.opts.SafetyBytes > free-held || reserve > free-held-service.opts.SafetyBytes {
		return empty, loom.Fail(loom.Exhausted, "capture host peak reservation exhausted")
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operationResult{
			SandboxID:  sandbox.ID,
			VMID:       sandbox.VMID,
			SnapshotID: newID("snap-"),
		},
	}
	snapshot := snapshotRecord{
		ID:             operation.Result.SnapshotID,
		TenantID:       request.TenantID,
		OperationID:    operation.ID,
		SourceVM:       sandbox.VMID,
		SourceInstance: request.ExpectedInstance,
		Dir:            filepath.Join(service.store.directory, "snapshots", operation.Result.SnapshotID),
	}
	input := captureInput{
		Request:      request,
		Source:       source,
		Sandbox:      sandbox,
		Snapshot:     snapshot,
		Backend:      compatibility,
		Architecture: runtime.GOARCH,
		CPU:          cpu,
	}
	payload := encode(input)
	if err = service.admitResourceParent(ctx, tx, parent, operation, "capture", snapshot.ID, hash); err != nil {
		return empty, err
	}
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
		{"INSERT INTO operation_steps VALUES(?,'capture','snapshot',?,?,?,?,'pending',?)", []any{
			operation.ID,
			digest(payload),
			payload,
			snapshot.ID,
			request.ExpectedInstance,
			encode(captureState{
				Phase: "admitted",
			}),
		}},
		{"INSERT INTO snapshots VALUES(?,?,0,0,?)", []any{
			snapshot.ID,
			sandbox.VMID,
			encode(snapshot),
		}},
		{"INSERT INTO snapshot_owners VALUES(?,?,?,'capture')", []any{
			snapshot.ID,
			request.TenantID,
			operation.ID,
		}},
		{"INSERT INTO reservations VALUES(?,?,0,0)", []any{
			"snapshot:" + snapshot.ID,
			reserve,
		}},
		{"INSERT INTO quota_resources VALUES(?,'pending_storage',?,0,?,?)", []any{
			request.TenantID,
			operation.ID + ":capture",
			cost,
			operation.ID,
		}},
		{"INSERT INTO quota_refs VALUES(?,'pending_storage',?,'engine_dependency',?)", []any{
			request.TenantID,
			operation.ID + ":capture",
			operation.ID + ":capture",
		}},
		{"INSERT INTO resource_refs VALUES('vm',?,'capture',?)", []any{
			sandbox.VMID,
			operation.ID,
		}},
		{"INSERT INTO resource_refs VALUES('image',?,'capture',?)", []any{
			vm.Resource.Config.Image,
			operation.ID,
		}},
		{"UPDATE vms SET operation_id=? WHERE id=? AND current_instance=? AND operation_id IS NULL AND state='running'", []any{
			operation.ID,
			sandbox.VMID,
			request.ExpectedInstance,
		}},
	}
	for _, statement := range statements {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	admitted, err := loadCapture(ctx, tx, operation.ID)
	if err != nil {
		return empty, err
	}
	if _, err = service.captureProof(ctx, tx, admitted); err != nil {
		return empty, err
	}
	if err = service.fault("capture_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if err = service.fault("capture_admitted"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}

func (service *Service) GetSnapshot(ctx context.Context, tenant, id string) (snapshotRecord, error) {
	var snapshot snapshotRecord
	var body []byte
	if err := service.store.db.QueryRowContext(ctx, "SELECT s.body FROM snapshots s JOIN snapshot_owners o ON o.snapshot_id=s.id WHERE s.id=? AND o.tenant_id=?", id, tenant).Scan(&body); err != nil {
		return snapshot, err
	}
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return snapshot, err
	}
	if snapshot.ID != id || snapshot.TenantID != tenant {
		return snapshot, fmt.Errorf("snapshot provenance inconsistent")
	}
	return snapshot, nil
}
