package haven

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

type rollbackInput struct {
	RawCaptures []generationRef `json:"raw_captures,omitempty"`
	Checkpoint  Checkpoint      `json:"checkpoint"`
	Source      restoreSource   `json:"source"`
}

func (service *Service) AdmitRollback(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	var empty Operation
	if command.Action != "rollback" || command.SandboxID == "" || command.CheckpointID == "" || principal.SandboxID != "" || principal.TenantID == "" || key == "" || len(key) > 256 {
		return empty, loom.Fail(loom.Invalid, "rollback requires tenant authority, sandbox, checkpoint and key")
	}
	requestGuard := service.vmLock("request:" + string(encode([]string{
		principal.TenantID,
		key,
	})))
	requestGuard.Lock()
	defer requestGuard.Unlock()
	mac := service.store.commandMAC(principal, command)
	if operation, found, err := service.existingSandboxOperation(ctx, principal, key, &claimAdmission{
		MAC: mac,
	}); found || err != nil {
		return operation, err
	}
	guard := service.vmLock("configuration:" + command.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	sandbox, err := ownedSandbox(ctx, service.store.db, principal, command.SandboxID)
	if err != nil {
		return empty, err
	}
	if sandbox.State != "running" && sandbox.State != "paused" {
		return empty, loom.Fail(loom.Conflict, "rollback requires running or paused sandbox")
	}
	checkpoint, err := service.GetCheckpoint(ctx, principal, command.CheckpointID)
	if err != nil {
		return empty, err
	}
	if checkpoint.SandboxID != sandbox.ID || checkpoint.TenantID != sandbox.TenantID {
		return empty, loom.Fail(loom.Conflict, "rollback checkpoint differs from sandbox owner")
	}
	snapshot, err := service.loadRestoreSource(ctx, service.store.db, sandbox.TenantID, checkpoint.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = verifyRestoreSourceFiles(snapshot); err != nil {
		return empty, err
	}
	settings, err := loadSettings(ctx, service.store.db, sandbox.ID)
	if err != nil {
		return empty, err
	}
	source, err := loadStep(ctx, service.store.db, sandbox.SourceOperation)
	if err != nil {
		return empty, err
	}
	if !sameConfig(source.Input.Resource.Config, snapshot.Snapshot.Manifest.Config) {
		return empty, loom.Fail(loom.Conflict, "rollback shape/image differs from sandbox")
	}
	if sandbox.State == "running" && service.credentialsMissing(sandbox.ID, settings) {
		return empty, loom.Fail(loom.Conflict, "host credentials must be supplied again before rollback")
	}
	rawCaptures, err := pendingPublishedCaptures(ctx, service.store.db, source)
	if err != nil {
		return empty, err
	}
	parent := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "pending",
		Result: operationResult{
			SandboxID:    sandbox.ID,
			VMID:         sandbox.VMID,
			SnapshotID:   checkpoint.SnapshotID,
			CheckpointID: checkpoint.ID,
		},
	}
	input := lifecycleInput{
		RequestTenantID: principal.TenantID,
		OwnerTenantID:   sandbox.TenantID,
		Command:         sanitizedCommand(command),
		CheckpointID:    checkpoint.ID,
		Sandbox:         sandbox,
		Settings:        settings,
		Source: generationRef{
			OperationID: source.OperationID,
			InputHash:   source.Hash,
		},
		SourceInstance: source.State.Witness,
		Rollback: &rollbackInput{
			RawCaptures: rawCaptures,
			Checkpoint:  checkpoint,
			Source:      snapshot,
		},
	}
	if sandbox.State == "paused" {
		return service.admitPausedRollback(ctx, parent, input, mac)
	}
	admission := &lifecycleAdmission{
		Parent: parent,
		Input:  input,
		MAC:    mac,
		Role:   "stop",
	}
	child, err := service.admitStop(ctx, StopRequest{
		TenantID:         sandbox.TenantID,
		Key:              parent.ID + ":stop",
		SandboxID:        sandbox.ID,
		ExpectedInstance: sandbox.Instance,
	}, admission)
	if child.ID == "" {
		return empty, err
	}
	service.mu.Lock()
	service.opSecrets[parent.ID] = service.secrets[sandbox.ID]
	service.mu.Unlock()
	if err != nil {
		return parent, err
	}
	service.closeConnections(sandbox.ID)
	if err = service.revokeSandbox(sandbox.ID); err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Service) admitPausedRollback(ctx context.Context, parent Operation, input lifecycleInput, mac string) (Operation, error) {
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.checkAdmissionOpen(); err != nil {
		return Operation{}, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadSandbox(ctx, tx, input.Sandbox.ID, input.OwnerTenantID)
	if err != nil {
		return Operation{}, err
	}
	settings, err := loadSettings(ctx, tx, current.ID)
	if err != nil {
		return Operation{}, err
	}
	if !reflect.DeepEqual(current, input.Sandbox) || !reflect.DeepEqual(settings, input.Settings) {
		return Operation{}, loom.Fail(loom.Conflict, "paused rollback source changed")
	}
	var fence string
	if err = tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", current.ID).Scan(&fence); !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			return Operation{}, err
		}
		return Operation{}, loom.Fail(loom.Conflict, "paused rollback source is fenced")
	}
	checkpoint, err := loadCheckpoint(ctx, tx, input.CheckpointID, input.OwnerTenantID)
	if err != nil {
		return Operation{}, err
	}
	if !reflect.DeepEqual(checkpoint, input.Rollback.Checkpoint) {
		return Operation{}, loom.Fail(loom.Conflict, "paused rollback checkpoint changed")
	}
	source, err := service.loadRestoreSource(ctx, tx, input.OwnerTenantID, checkpoint.SnapshotID)
	if err != nil {
		return Operation{}, err
	}
	if !reflect.DeepEqual(source, input.Rollback.Source) {
		return Operation{}, loom.Fail(loom.Conflict, "paused rollback publication changed")
	}
	next := current
	next.SnapshotID = checkpoint.SnapshotID
	desired := settings
	desired.Generation = parent.ID
	frozen := encode(struct {
		Action  string
		Request Command
	}{input.Command.Action, input.Command})
	payload := encode(input)
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{
			parent.ID,
			"tenant",
			input.RequestTenantID,
			parent.Key,
			digest(frozen),
			frozen,
			"succeeded",
			encode(parent.Result),
		}},
		{"INSERT INTO operation_credentials VALUES(?,?)", []any{
			parent.ID,
			mac,
		}},
		{"INSERT INTO operation_steps VALUES(?,'lifecycle','rollback',?,?,?,?,'succeeded',?)", []any{
			parent.ID,
			digest(payload),
			payload,
			current.ID,
			"",
			encode(lifecycleState{
				Phase:      "complete",
				SnapshotID: checkpoint.SnapshotID,
			}),
		}},
		{"UPDATE sandboxes SET body=? WHERE id=? AND tenant=? AND state='paused' AND body=?", []any{
			encode(next),
			current.ID,
			input.OwnerTenantID,
			encode(current),
		}},
		{"UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", []any{
			encode(desired),
			current.ID,
			encode(settings),
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return Operation{}, err
		}
	}
	if current.SnapshotID != checkpoint.SnapshotID {
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"INSERT INTO resource_refs VALUES('snapshot',?,'sleep',?)", []any{
				checkpoint.SnapshotID,
				current.ID,
			}},
			{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'active_use',?)", []any{
				input.OwnerTenantID,
				checkpoint.SnapshotID,
				"sleep:" + current.ID,
			}},
			{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='sleep' AND holder_id=?", []any{
				current.SnapshotID,
				current.ID,
			}},
			{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{
				input.OwnerTenantID,
				current.SnapshotID,
				"sleep:" + current.ID,
			}},
		} {
			if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
				return Operation{}, err
			}
		}
	}
	if err = service.fault("paused_rollback_before_commit"); err != nil {
		return Operation{}, err
	}
	if err = tx.Commit(); err != nil {
		return Operation{}, err
	}
	parent.Status = "succeeded"
	return parent, service.store.checkpoint()
}

func (service *Service) runRollback(step lifecycleStep) error {
	stopped, err := service.runResourceLink(step.OperationID, "stop")
	if err != nil {
		return err
	}
	if err = service.cleanupRollbackCaptures(step, stopped.ChildID); err != nil {
		return err
	}
	if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, "restore"); errors.Is(lookup, sql.ErrNoRows) {
		link := &lifecycleAdmission{
			Parent: Operation{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "restore",
		}
		_, err = service.admitRestore(service.ctx, RestoreRequest{
			TenantID:         step.Input.OwnerTenantID,
			Key:              step.OperationID + ":restore",
			SandboxID:        step.Input.Sandbox.ID,
			SnapshotID:       step.Input.Rollback.Checkpoint.SnapshotID,
			ExpectedInstance: step.Input.SourceInstance,
			StopOperationID:  stopped.ChildID,
		}, link)
		if err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	restored, err := service.runResourceLink(step.OperationID, "restore")
	if err != nil {
		return err
	}
	child, err := loadStep(service.ctx, service.store.db, restored.ChildID)
	if err != nil {
		return err
	}
	if err = service.restoreActivation(step, child); err != nil {
		return err
	}
	lock := service.vmLock(child.Candidate)
	lock.Lock()
	defer lock.Unlock()
	return service.continueActivation(step.OperationID)
}
