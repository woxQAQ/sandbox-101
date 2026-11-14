package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/loom"
)

// AdmitDiscard is an explicit sandbox-scoped cleanup of one fixed failed intent.
// It never upgrades that failure to a successful generation or invents a witness.
func (service *Service) AdmitDiscard(ctx context.Context, request DiscardRequest) (Operation, error) {
	return service.admitDiscard(ctx, request, nil)
}

func (service *Service) admitDiscard(ctx context.Context, request DiscardRequest, parent resourceParent) (Operation, error) {
	var empty Operation
	if request.TenantID == "" || request.SandboxID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.FailedOperationID, "op-") || !loom.ValidResourceID(request.ExpectedInstance, "run-") {
		return empty, fmt.Errorf("%w: discard requires sandbox, failed operation, candidate instance and key", ErrInvalid)
	}
	frozen := encode(struct {
		Action  string
		Request DiscardRequest
	}{"discard", request})
	hash := digest(frozen)
	service.admission.Lock()
	defer service.admission.Unlock()
	service.mu.Lock()
	closing := service.closing || service.store.reviewed
	service.mu.Unlock()
	if closing {
		return empty, fmt.Errorf("%w: Haven is closing", ErrConflict)
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
		return empty, fmt.Errorf("%w: sandbox not found", ErrNotFound)
	}
	if err != nil {
		return empty, err
	}
	if err = allocationCleanupFence(ctx, tx, sandbox.VMID, parent); err != nil {
		return empty, err
	}
	if sandbox.State == "terminated" {
		return empty, fmt.Errorf("%w: reuse the admitted discard key", ErrConflict)
	}
	if sandbox.SourceOperation != request.FailedOperationID || sandbox.OperationID != request.FailedOperationID || sandbox.Instance != "" {
		return empty, fmt.Errorf("%w: discard source differs from original undelivered sandbox", ErrConflict)
	}
	source, err := loadStep(ctx, tx, request.FailedOperationID)
	if err != nil {
		return empty, err
	}
	vm, current, owner, err := service.loadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if owner != source.OperationID || vm.Deleting || vm.Verified || vm.AgentReady || source.Candidate != sandbox.VMID || current != "" && !expectedCurrentInstance(source, current) {
		return empty, fmt.Errorf("%w: discard candidate no longer belongs to failed intent", ErrManualRecovery)
	}
	input := discardInput{
		Request:         request,
		Source:          source,
		CurrentInstance: current,
	}
	if _, err = service.discardSource(ctx, tx, input); err != nil {
		return empty, err
	}
	if _, err = service.generationHistory(ctx, tx, source); err != nil {
		return empty, err
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operationResult{
			SandboxID: sandbox.ID,
			VMID:      source.Candidate,
		},
	}
	if err = service.admitResourceParent(ctx, tx, parent, operation, "discard", source.Candidate, hash); err != nil {
		return empty, err
	}
	payload := encode(input)
	vm.Deleting = true
	sandbox.State = "terminated"
	sandbox.OperationID = operation.ID
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
		{"INSERT INTO operation_steps VALUES(?,'discard','discard',?,?,?,?,'pending',?)", []any{
			operation.ID,
			digest(payload),
			payload,
			source.Candidate,
			request.ExpectedInstance,
			encode(discardState{
				Phase: "admitted",
			}),
		}},
		{"INSERT INTO cleanup_intents VALUES(?,?,'vm',?,'pending',?)", []any{
			"discard:" + operation.ID,
			operation.ID,
			source.Candidate,
			payload,
		}},
		{"UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND resource_kind='vm' AND resource_id=? AND status='blocked'", []any{
			"blocked:" + source.OperationID,
			source.OperationID,
			source.Candidate,
		}},
		{"UPDATE vms SET operation_id=?,body=? WHERE id=? AND operation_id=? AND current_instance=?", []any{
			operation.ID,
			encode(vm),
			source.Candidate,
			source.OperationID,
			current,
		}},
		{"UPDATE sandboxes SET state='terminated',body=? WHERE id=? AND tenant=? AND state!='terminated'", []any{
			encode(sandbox),
			sandbox.ID,
			request.TenantID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	for _, reference := range []struct {
		kind, id string
	}{
		{"vm", source.Candidate},
		{"image", source.Input.Resource.Config.Image},
	} {
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE resource_refs SET holder_kind='cleanup',holder_id=? WHERE resource_kind=? AND resource_id=? AND holder_kind='step' AND holder_id=?", operation.ID+":discard", reference.kind, reference.id, source.OperationID+":resource")); err != nil {
			return empty, err
		}
	}
	for _, quota := range []struct {
		kind, key, holderKind, holder string
	}{
		{"pending_storage", source.OperationID + ":resource", "engine_dependency", source.OperationID + ":resource"},
		{source.Input.SlotBudget.Kind, sandbox.ID, "active_use", "sandbox:" + sandbox.ID},
	} {
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE quota_refs SET holder_kind='cleanup',holder_id=? WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind=? AND holder_id=?", operation.ID+":discard", request.TenantID, quota.kind, quota.key, quota.holderKind, quota.holder)); err != nil {
			return empty, err
		}
	}
	for _, reference := range source.Input.Ancestors {
		prior, err := loadStep(ctx, tx, reference.OperationID)
		if err != nil {
			return empty, err
		}
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE quota_refs SET holder_kind='cleanup',holder_id=? WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", operation.ID+":discard", request.TenantID, prior.Input.DiskKey, "sandbox:"+sandbox.ID)); err != nil {
			return empty, err
		}
	}
	step, err := loadDiscard(ctx, tx, operation.ID)
	if err != nil {
		return empty, err
	}
	if _, err = service.discardProof(ctx, tx, step); err != nil {
		return empty, fmt.Errorf("discard handoff: %w", err)
	}
	if err = service.fault("discard_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if err = service.fault("discard_admitted"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}
