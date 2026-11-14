package haven

import (
	"context"
	"fmt"
	"reflect"
)

func (service *Service) AdmitCheckpointDelete(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	var empty Operation
	if principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || command.Action != "checkpoint_delete" || command.CheckpointID == "" {
		return empty, fmt.Errorf("%w: checkpoint deletion requires tenant authority, checkpoint and key", ErrInvalid)
	}
	guard := service.vmLock("request:" + string(encode([]string{
		principal.TenantID,
		key,
	})))
	guard.Lock()
	defer guard.Unlock()
	mac := service.store.commandMAC(principal, command)
	if cached, found, err := service.existingSandboxOperation(ctx, principal, key, &claimAdmission{
		MAC: mac,
	}); found || err != nil {
		return cached, err
	}
	checkpoint, err := service.GetCheckpoint(ctx, principal, command.CheckpointID)
	if err != nil {
		return empty, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	if err = service.checkAdmissionOpen(); err != nil {
		return empty, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadCheckpoint(ctx, tx, checkpoint.ID, checkpoint.TenantID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(current, checkpoint) {
		return empty, fmt.Errorf("%w: checkpoint binding changed", ErrConflict)
	}
	publication, err := service.snapshotPublication(ctx, tx, checkpoint.TenantID, checkpoint.SnapshotID)
	if err != nil {
		return empty, err
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "succeeded",
		Result: operationResult{
			CheckpointID: checkpoint.ID,
			SnapshotID:   checkpoint.SnapshotID,
			SandboxID:    checkpoint.SandboxID,
		},
	}
	frozen := encode(struct {
		Action  string
		Request Command
	}{"checkpoint_delete", sanitizedCommand(command)})
	input := encode(struct {
		Checkpoint  Checkpoint
		Publication restoreSource
	}{checkpoint, publication})
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{
			operation.ID,
			"tenant",
			principal.TenantID,
			key,
			digest(frozen),
			frozen,
			"succeeded",
			encode(operation.Result),
		}},
		{"INSERT INTO operation_credentials VALUES(?,?)", []any{
			operation.ID,
			mac,
		}},
		{"INSERT INTO operation_steps VALUES(?,'checkpoint_delete','checkpoint_delete',?,?,?,?,'succeeded',?)", []any{
			operation.ID,
			digest(input),
			input,
			checkpoint.SnapshotID,
			"",
			encode("complete"),
		}},
		{"DELETE FROM checkpoint_bindings WHERE id=? AND tenant=? AND snapshot_id=? AND body=?", []any{
			checkpoint.ID,
			checkpoint.TenantID,
			checkpoint.SnapshotID,
			encode(checkpoint),
		}},
		{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='checkpoint_binding' AND holder_id=?", []any{
			checkpoint.SnapshotID,
			checkpoint.ID,
		}},
		{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{
			checkpoint.TenantID,
			checkpoint.SnapshotID,
			"checkpoint-binding:" + checkpoint.ID,
		}},
		{"INSERT INTO cleanup_intents VALUES(?,?,'snapshot',?,'pending',?)", []any{
			"checkpoint:" + operation.ID,
			operation.ID,
			checkpoint.SnapshotID,
			encode(publication),
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if err = service.fault("checkpoint_delete_before_commit"); err != nil {
		return empty, err
	}
	if err = commitPublications(tx); err != nil {
		return empty, err
	}
	return operation, service.store.checkpoint()
}
