package haven

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) AdmitSnapshotDelete(ctx context.Context, request SnapshotDeleteRequest) (Operation, error) {
	var empty Operation
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.SnapshotID, "snap-") {
		return empty, loom.Fail(loom.Invalid, "snapshot delete requires scoped snapshot and key")
	}
	frozen := encode(struct {
		Action  string
		Request SnapshotDeleteRequest
	}{"snapshot_delete", request})
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
	source, err := service.snapshotPublication(ctx, tx, request.TenantID, request.SnapshotID)
	if err != nil {
		return empty, err
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operationResult{
			SnapshotID: request.SnapshotID,
		},
	}
	input := snapshotDeleteInput{
		Request: request,
		Source:  source,
	}
	payload := encode(input)
	step := snapshotDeleteStep{
		OperationID: operation.ID,
		Hash:        digest(payload),
		Status:      "pending",
		Phase:       "admitted",
		Input:       input,
	}
	if _, err = service.snapshotDeleteProof(ctx, tx, step, true); err != nil {
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
		{"INSERT INTO operation_steps VALUES(?,'snapshot_delete','snapshot_delete',?,?,?,'','pending',?)", []any{
			operation.ID,
			step.Hash,
			payload,
			request.SnapshotID,
			encode(step.Phase),
		}},
		{"UPDATE snapshots SET deleting=1 WHERE id=? AND published=1 AND deleting=0 AND body=?", []any{
			request.SnapshotID,
			encode(source.Snapshot),
		}},
		{"UPDATE resource_refs SET holder_kind='cleanup',holder_id=? WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='checkpoint' AND holder_id=?", []any{
			operation.ID,
			request.SnapshotID,
			request.SnapshotID,
		}},
		{"UPDATE quota_refs SET holder_kind='cleanup',holder_id=? WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{
			operation.ID + ":snapshot_delete",
			request.TenantID,
			request.SnapshotID,
			"checkpoint:" + request.SnapshotID,
		}},
		{"INSERT INTO cleanup_intents VALUES(?,?,'snapshot',?,'pending',?)", []any{
			"snapshot_delete:" + operation.ID,
			operation.ID,
			request.SnapshotID,
			encode(input),
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if _, err = service.snapshotDeleteProof(ctx, tx, step, false); err != nil {
		return empty, err
	}
	if err = service.fault("snapshot_delete_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if err = service.fault("snapshot_delete_admitted"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}
