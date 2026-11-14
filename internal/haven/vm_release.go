package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

// AdmitRelease revokes the logical sandbox and gives cleanup an independent
// intent. This cold slice requires an immutable successful source witness;
// unresolved/failed generation attempts retain their responsibility for review.
func (service *Service) AdmitRelease(ctx context.Context, request ReleaseRequest) (Operation, error) {
	return service.admitRelease(ctx, request, nil)
}

func (service *Service) admitRelease(ctx context.Context, request ReleaseRequest, parent resourceParent) (Operation, error) {
	link := lifecycleParent(parent)
	var empty Operation
	if request.Key == "" || len(request.Key) > 256 || request.TenantID == "" || request.SandboxID == "" {
		return empty, loom.Fail(loom.Invalid, "tenant, sandbox and a key of 1..256 bytes are required")
	}
	frozen := encode(struct {
		Action  string
		Request ReleaseRequest
	}{"release", request})
	hash := digest(frozen)
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
	if errors.Is(err, sql.ErrNoRows) {
		return empty, loom.Fail(loom.NotFound, "sandbox not found")
	}
	if err != nil {
		return empty, err
	}
	if err = allocationCleanupFence(ctx, tx, sandbox.VMID, parent); err != nil {
		return empty, err
	}
	if sandbox.State == "terminated" {
		return empty, loom.Fail(loom.Conflict, "reuse the admitted release key")
	}
	source, err := loadStep(ctx, tx, sandbox.SourceOperation)
	if err != nil {
		return empty, err
	}
	if err = service.validateStep(source); err != nil {
		return empty, err
	}
	if (source.Action != "cold_start" && source.Action != "clone" && source.Action != "restore") || source.Status != "succeeded" || source.State.Phase != "complete" || source.State.Observation == nil || source.State.DiskIdentity == nil || source.State.Witness == "" || source.State.Witness != sandbox.Instance || source.State.Witness != source.Input.Resource.Instance || source.Input.SandboxID != sandbox.ID || source.Input.TenantID != sandbox.TenantID || source.Candidate != sandbox.VMID {
		return empty, loom.Fail(loom.ManualRecovery, "release requires the frozen successful generation witness")
	}
	vm, instance, owner, err := service.loadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if instance != source.State.Witness || owner != "" || !vm.Verified || vm.Deleting {
		return empty, loom.Fail(loom.ManualRecovery, "release current ownership differs from the historical witness")
	}
	var instances, versions, backing, warm, holders int
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM instances WHERE vm_id=?),(SELECT COUNT(*) FROM resource_versions WHERE vm_id=?),(SELECT COUNT(*) FROM backing WHERE vm_id=?),(SELECT COUNT(*) FROM warm WHERE vm_id=?),(SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=?)`, sandbox.VMID, sandbox.VMID, sandbox.VMID, sandbox.VMID, sandbox.VMID).Scan(&instances, &versions, &backing, &warm, &holders); err != nil {
		return empty, err
	}
	captures, err := service.releaseCaptures(ctx, tx, sandbox.VMID, source.State.Witness)
	if err != nil {
		return empty, err
	}
	generations, err := service.cleanupGenerations(ctx, tx, source, "active_use", "sandbox:"+sandbox.ID)
	if err != nil {
		return empty, err
	}
	if err = service.verifyGeneration(ctx, tx, source); err != nil {
		return empty, err
	}
	if err = checkGenerationCounts(ctx, tx, source, generations); err != nil {
		return empty, err
	}
	if instances != len(generations) || versions != len(generations) || warm != 0 || holders != 1+len(captures) {
		return empty, loom.Fail(loom.ManualRecovery, "cold release has additional or missing resource responsibilities")
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operationResult{
			SandboxID: sandbox.ID,
			VMID:      sandbox.VMID,
		},
	}
	budget, err := service.currentSlotBudget(ctx, tx, sandbox, source)
	if err != nil {
		return empty, err
	}
	input := source.Input
	input.SlotBudget = budget
	input.SlotAssignment = sandbox.SlotAssignment
	input.Captures = captures
	input.SourceOperation = source.OperationID
	input.DiskIdentity = source.State.DiskIdentity
	input.RestoreMaterials = copyRestoreMaterials(source.State.Materials)
	if warm, ok := parent.(*warmClaimAdmission); ok && warm != nil {
		if warm.Role != "warm_release" {
			return empty, loom.Fail(loom.Invalid, "warm claim release role invalid")
		}
		input.SlotDestination = &quotaDestination{
			OperationID: warm.ParentID,
			Kind:        "claim",
			ID:          sandbox.ID,
			SnapshotID:  warm.Input.Pool.Template.SnapshotID,
		}
	}
	if link != nil && link.Input.Command.Action == "pause" {
		parent, err := loadLifecycle(ctx, tx, link.Parent.ID)
		if err != nil {
			return empty, err
		}
		if parent.State.Phase != "captured" || parent.State.SnapshotID == "" {
			return empty, loom.Fail(loom.Conflict, "pause source release lacks published snapshot")
		}
		input.SlotDestination = &quotaDestination{
			OperationID: link.Parent.ID,
			Kind:        "sleep",
			ID:          sandbox.ID,
			SnapshotID:  parent.State.SnapshotID,
		}
	} else if link != nil && link.Input.Command.Action == "resume" && link.Role == "compensate" {
		activation, err := loadActivation(ctx, tx, link.Parent.ID)
		if err != nil {
			return empty, err
		}
		if activation.Status != "failed" || activation.Input.ResourceOperation != source.OperationID || activation.Input.ExpectedInstance != source.State.Witness {
			return empty, loom.Fail(loom.Conflict, "resume compensation lacks failed delivery proof")
		}
		input.SlotDestination = &quotaDestination{
			OperationID: link.Parent.ID,
			Kind:        "sleep",
			ID:          sandbox.ID,
			SnapshotID:  link.Input.Sandbox.SnapshotID,
		}
	}
	if err = service.admitResourceParent(ctx, tx, parent, operation, "release", sandbox.VMID, hash); err != nil {
		return empty, err
	}
	if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", operation.ID, "tenant", request.TenantID, request.Key, hash, frozen, operation.Status, encode(operation.Result))); err != nil {
		return empty, err
	}
	if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO operation_steps VALUES(?,?,?,?,?,?,?,?,?)", operation.ID, "resource", "release", digest(encode(input)), encode(input), sandbox.VMID, source.State.Witness, "pending", encode(stepState{
		Phase: "admitted",
	}))); err != nil {
		return empty, err
	}
	if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO cleanup_intents VALUES(?,?,?,?,?,?)", "cleanup:"+operation.ID, operation.ID, "vm", sandbox.VMID, "pending", encode(input))); err != nil {
		return empty, err
	}
	sandbox.State, sandbox.OperationID = "terminated", operation.ID
	if err = requireWrite(tx.ExecContext(ctx, "UPDATE sandboxes SET state='terminated',body=? WHERE id=? AND tenant=? AND state!='terminated'", encode(sandbox), sandbox.ID, sandbox.TenantID)); err != nil {
		return empty, err
	}
	vm.Deleting = true
	if err = requireWrite(tx.ExecContext(ctx, "UPDATE vms SET operation_id=?,body=? WHERE id=? AND current_instance=? AND operation_id IS NULL", operation.ID, encode(vm), sandbox.VMID, source.State.Witness)); err != nil {
		return empty, err
	}
	quotas := []struct {
		kind, key string
	}{
		{input.SlotBudget.Kind, sandbox.ID},
	}
	for _, generation := range generations {
		quotas = append(quotas, struct {
			kind, key string
		}{"vm_disk", generation.Input.DiskKey})
	}
	for _, quota := range quotas {
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE quota_refs SET holder_kind='cleanup',holder_id=? WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind='active_use' AND holder_id=?", operation.ID+":resource", sandbox.TenantID, quota.kind, quota.key, "sandbox:"+sandbox.ID)); err != nil {
			return empty, err
		}
	}
	if err = requireWrite(tx.ExecContext(ctx, "UPDATE resource_refs SET holder_kind='cleanup',holder_id=? WHERE resource_kind='vm' AND resource_id=? AND holder_kind='sandbox' AND holder_id=?", operation.ID+":resource", sandbox.VMID, sandbox.ID)); err != nil {
		return empty, err
	}
	for _, capture := range captures {
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND status='pending'", "capture:"+capture.OperationID, capture.OperationID)); err != nil {
			return empty, err
		}
		if err = requireWrite(tx.ExecContext(ctx, "DELETE FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='capture' AND holder_id=?", sandbox.VMID, capture.OperationID)); err != nil {
			return empty, err
		}
	}
	var parentStatus string
	var parentBody []byte
	if err = tx.QueryRowContext(ctx, "SELECT status,body FROM operations WHERE id=?", source.OperationID).Scan(&parentStatus, &parentBody); err != nil {
		return empty, err
	}
	if !terminal(parentStatus) {
		var result operationResult
		if err = json.Unmarshal(parentBody, &result); err != nil {
			return empty, err
		}
		result.Error = "released before sandbox delivery"
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", encode(result), source.OperationID)); err != nil {
			return empty, err
		}
	}
	if err = service.fault("release_before_commit"); err != nil {
		return empty, err
	}
	if err = commitPublications(tx); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if err = service.fault("release_admitted"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}

func (service *Service) releaseProof(tx *sql.Tx, step resourceStep) (vmRecord, error) {
	source, err := loadStep(service.ctx, tx, step.Input.SourceOperation)
	if err != nil {
		return vmRecord{}, err
	}
	if (source.Action != "cold_start" && source.Action != "clone" && source.Action != "restore") || source.Status != "succeeded" || source.State.Phase != "complete" || source.State.Observation == nil || source.State.DiskIdentity == nil || source.State.Witness == "" || source.State.Witness != step.ExpectedInstance || source.State.Witness != step.Input.Resource.Instance || source.Input.SourceOperation != "" || source.Input.TenantID != step.Input.TenantID || source.Input.SandboxID != step.Input.SandboxID || source.Candidate != step.Candidate {
		return vmRecord{}, fmt.Errorf("release lost its immutable source witness")
	}
	expected := source.Input
	expected.Captures = step.Input.Captures
	expected.SourceOperation = source.OperationID
	expected.DiskIdentity = source.State.DiskIdentity
	expected.RestoreMaterials = copyRestoreMaterials(source.State.Materials)
	expected.SlotDestination = step.Input.SlotDestination
	expected.SlotBudget = step.Input.SlotBudget
	expected.SlotAssignment = step.Input.SlotAssignment
	if err = service.releaseSlotDestination(tx, step); err != nil {
		return vmRecord{}, err
	}
	if !reflect.DeepEqual(expected, step.Input) {
		return vmRecord{}, fmt.Errorf("release input differs from its frozen source")
	}
	vm, instance, owner, err := service.loadVM(service.ctx, tx, step)
	if err != nil {
		return vm, err
	}
	if !vm.Deleting || owner != step.OperationID || instance != step.ExpectedInstance {
		return vm, fmt.Errorf("release lost current instance ownership")
	}
	sandbox, err := loadSandbox(service.ctx, tx, step.Input.SandboxID, step.Input.TenantID)
	if err != nil {
		return vm, err
	}
	budget, budgetErr := service.currentSlotBudget(service.ctx, tx, sandbox, source)
	if budgetErr != nil {
		return vmRecord{}, budgetErr
	}
	if budget != step.Input.SlotBudget || !reflect.DeepEqual(sandbox.SlotAssignment, step.Input.SlotAssignment) {
		return vmRecord{}, fmt.Errorf("release slot assignment differs from current owner")
	}
	if sandbox.OperationID != step.OperationID || sandbox.SourceOperation != step.Input.SourceOperation || sandbox.State != "terminated" || sandbox.Instance != step.ExpectedInstance || sandbox.VMID != step.Candidate {
		return vm, fmt.Errorf("release cleanup destination changed")
	}
	var cleanup []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT body FROM cleanup_intents WHERE id=? AND operation_id=? AND status='pending'", "cleanup:"+step.OperationID, step.OperationID).Scan(&cleanup); err != nil {
		return vm, err
	}
	if string(cleanup) != string(encode(step.Input)) {
		return vm, fmt.Errorf("cleanup frozen input changed")
	}
	var request, parentBody []byte
	var requestHash, kind, tenant, status string
	if err = tx.QueryRowContext(service.ctx, "SELECT request_hash,request,scope_kind,scope_id,status,body FROM operations WHERE id=?", step.OperationID).Scan(&requestHash, &request, &kind, &tenant, &status, &parentBody); err != nil {
		return vm, err
	}
	var frozen struct {
		Action  string
		Request ReleaseRequest
	}
	if err = json.Unmarshal(request, &frozen); err != nil {
		return vm, err
	}
	var result operationResult
	if err = json.Unmarshal(parentBody, &result); err != nil {
		return vm, err
	}
	if digest(request) != requestHash || kind != "tenant" || tenant != step.Input.TenantID || terminal(status) || frozen.Action != "release" || frozen.Request.TenantID != tenant || frozen.Request.SandboxID != sandbox.ID || result.SandboxID != sandbox.ID || result.VMID != step.Candidate {
		return vm, fmt.Errorf("release operation identity changed")
	}
	if err = service.validateStep(source); err != nil {
		return vm, err
	}
	var versionID, versionOwner, versionKey string
	var versionBody []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT id,operation_id,step_key,body FROM resource_versions WHERE vm_id=? AND id=?", step.Candidate, source.OperationID).Scan(&versionID, &versionOwner, &versionKey, &versionBody); err != nil {
		return vm, err
	}
	var version resourceVersion
	if err = json.Unmarshal(versionBody, &version); err != nil {
		return vm, err
	}
	if versionID != source.OperationID || versionOwner != source.OperationID || versionKey != source.Key || !reflect.DeepEqual(version, resourceVersion{
		Resource:     source.Input.Resource,
		DiskIdentity: source.State.DiskIdentity,
	}) {
		return vm, fmt.Errorf("release resource version birth proof changed")
	}
	var instanceOwner string
	var instanceBody []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT operation_id,body FROM instances WHERE id=? AND vm_id=?", step.ExpectedInstance, step.Candidate).Scan(&instanceOwner, &instanceBody); err != nil {
		return vm, err
	}
	var historical vmRecord
	if err = json.Unmarshal(instanceBody, &historical); err != nil {
		return vm, err
	}
	if instanceOwner != source.OperationID || !historical.Verified || !historical.AgentReady || historical.Deleting || !reflect.DeepEqual(historical.Resource, source.Input.Resource) || !reflect.DeepEqual(historical.Observation, source.State.Observation) {
		return vm, fmt.Errorf("release instance birth proof changed")
	}
	if err = service.checkTransferredCaptures(service.ctx, tx, step); err != nil {
		return vm, err
	}
	generations, err := service.cleanupGenerations(service.ctx, tx, source, "cleanup", step.OperationID+":resource")
	if err != nil {
		return vm, err
	}
	if err = checkGenerationCounts(service.ctx, tx, source, generations); err != nil {
		return vm, err
	}
	shape := step.Input.Resource.Config.Shape
	vmBytes := step.Input.RuntimeBytes
	expectedBacking := 0
	for _, generation := range generations {
		if generation.State.BackingReady {
			expectedBacking++
		}
	}
	checks := []struct {
		query string
		args  []any
		want  int
	}{
		{"SELECT COUNT(*) FROM instances WHERE vm_id=?", []any{
			step.Candidate,
		}, len(generations)},
		{"SELECT COUNT(*) FROM resource_versions WHERE vm_id=?", []any{
			step.Candidate,
		}, len(generations)},
		{"SELECT COUNT(*) FROM backing WHERE vm_id=?", []any{
			step.Candidate,
		}, expectedBacking},
		{"SELECT COUNT(*) FROM warm WHERE vm_id=?", []any{
			step.Candidate,
		}, 0},
		{"SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind='vm' AND resource_id=? AND status!='done'", []any{
			step.Candidate,
		}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=?", []any{
			step.Candidate,
		}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='cleanup' AND holder_id=?", []any{
			step.Candidate,
			step.OperationID + ":resource",
		}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='vm' AND holder_id=?", []any{
			step.Input.Resource.Config.Image,
			step.Candidate,
		}, 1},
		{"SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=? AND cpus=?", []any{
			"vm:" + step.Candidate,
			vmBytes,
			shape.MemoryMiB + 128,
			shape.VCPUs,
		}, 1},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND sandbox_slots=0 AND storage_mib=? AND source_operation_id=?", []any{
			tenant,
			step.Input.DiskKey,
			shape.DiskMiB,
			step.Input.SourceOperation,
		}, 1},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=? AND sandbox_slots=? AND storage_mib=0 AND source_operation_id=?", []any{
			tenant,
			step.Input.SlotBudget.Kind,
			sandbox.ID,
			step.Input.SlotBudget.Units,
			step.Input.SlotOperation,
		}, 1},
	}
	for _, quota := range []struct {
		kind, key string
	}{
		{"vm_disk", step.Input.DiskKey},
		{step.Input.SlotBudget.Kind, sandbox.ID},
	} {
		checks = append(checks, struct {
			query string
			args  []any
			want  int
		}{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=?", []any{
			tenant,
			quota.kind,
			quota.key,
		}, 1})
		checks = append(checks, struct {
			query string
			args  []any
			want  int
		}{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind='cleanup' AND holder_id=?", []any{
			tenant,
			quota.kind,
			quota.key,
			step.OperationID + ":resource",
		}, 1})
	}
	for _, check := range checks {
		var count int
		if err = tx.QueryRowContext(service.ctx, check.query, check.args...).Scan(&count); err != nil || count != check.want {
			return vm, fmt.Errorf("release responsibility proof changed: %v", err)
		}
	}
	return vm, nil
}
