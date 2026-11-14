package haven

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/loom"
)

// AdmitCaptureDiscard keeps the source VM and its disk. The caller explicitly
// supplies a completed stop operation; cleanup never infers that writers exited.
func (service *Service) AdmitCaptureDiscard(ctx context.Context, request CaptureDiscardRequest) (Operation, error) {
	return service.admitCaptureDiscard(ctx, request, nil)
}

func (service *Service) admitCaptureDiscard(ctx context.Context, request CaptureDiscardRequest, parent resourceParent) (Operation, error) {
	link := lifecycleParent(parent)
	var empty Operation
	if request.TenantID == "" || request.SandboxID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.FailedOperationID, "op-") || !loom.ValidResourceID(request.ExpectedInstance, "run-") || !loom.ValidResourceID(request.StopOperationID, "op-") {
		return empty, loom.Fail(loom.Invalid, "capture discard requires tenant, sandbox, failed capture, stopped instance and key")
	}
	frozen := encode(struct {
		Action  string
		Request CaptureDiscardRequest
	}{"capture_discard", request})
	hash := digest(frozen)
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
	source, err := loadCapture(ctx, tx, request.FailedOperationID)
	if err != nil {
		return empty, err
	}
	if err = allocationCleanupFence(ctx, tx, source.Input.Source.Candidate, parent); err != nil {
		return empty, err
	}
	stop, err := loadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return empty, err
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operationResult{
			SandboxID:  request.SandboxID,
			VMID:       source.Input.Source.Candidate,
			SnapshotID: source.Input.Snapshot.ID,
		},
	}
	input := captureDiscardInput{
		Request:  request,
		Source:   source,
		StopHash: stop.Hash,
	}
	if link != nil {
		input.Lifecycle = link.Parent.ID
	}
	if err = service.admitResourceParent(ctx, tx, parent, operation, "capture_discard", source.Input.Snapshot.ID, hash); err != nil {
		return empty, err
	}
	payload := encode(input)
	step := captureDiscardStep{
		OperationID: operation.ID,
		Hash:        digest(payload),
		Input:       input,
		Phase:       "admitted",
		Status:      "pending",
	}
	if err = service.captureDiscardProof(ctx, tx, step, true); err != nil {
		return empty, err
	}
	for _, statement := range []struct {
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
		{"INSERT INTO operation_steps VALUES(?,'capture_discard','capture_discard',?,?,?,?,'pending',?)", []any{
			operation.ID,
			step.Hash,
			payload,
			source.Input.Snapshot.ID,
			request.ExpectedInstance,
			encode(step.Phase),
		}},
		{"UPDATE snapshots SET deleting=1 WHERE id=? AND published=0 AND deleting=0", []any{
			source.Input.Snapshot.ID,
		}},
		{"UPDATE vms SET operation_id=? WHERE id=? AND operation_id IS NULL AND state='stopped' AND current_instance=?", []any{
			operation.ID,
			source.Input.Source.Candidate,
			request.ExpectedInstance,
		}},
		{"UPDATE quota_refs SET holder_kind='cleanup',holder_id=? WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{
			operation.ID + ":capture_discard",
			request.TenantID,
			source.OperationID + ":capture",
			source.OperationID + ":capture",
		}},
		{"UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND status='blocked'", []any{
			"capture:" + source.OperationID,
			source.OperationID,
		}},
		{"INSERT INTO cleanup_intents VALUES(?,?,'snapshot',?,'pending',?)", []any{
			"capture_discard:" + operation.ID,
			operation.ID,
			source.Input.Snapshot.ID,
			encode(input),
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if err = service.captureDiscardProof(ctx, tx, step, false); err != nil {
		return empty, err
	}
	if err = service.fault("capture_discard_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if err = service.fault("capture_discard_admitted"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}
