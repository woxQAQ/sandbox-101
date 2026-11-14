package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

func pausedReceipt(ctx context.Context, q rowsQueryer, sandbox sandboxRecord, source resourceStep) (string, error) {
	rows, err := q.QueryContext(ctx, "SELECT operation_id FROM operation_steps WHERE step_key='lifecycle' AND action='pause' AND candidate_resource_id=? AND status='succeeded' ORDER BY rowid DESC", sandbox.ID)
	if err != nil {
		return "", err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return "", err
	}
	for _, id := range ids {
		parent, err := loadLifecycle(ctx, q, id)
		if err != nil {
			return "", err
		}
		if parent.Input.Source.OperationID != source.OperationID {
			continue
		}
		if parent.Input.Source.InputHash != source.Hash || parent.Input.SourceInstance != source.State.Witness || parent.Input.Sandbox.ID != sandbox.ID || parent.Input.OwnerTenantID != sandbox.TenantID || parent.State.Phase != "complete" {
			return "", fmt.Errorf("%w: paused release history changed", ErrManualRecovery)
		}
		link, status, err := loadResourceLink(ctx, q, id, "release")
		if err != nil {
			return "", err
		}
		if status != "succeeded" {
			return "", fmt.Errorf("%w: paused release link has no successful deletion", ErrManualRecovery)
		}
		released, err := loadStep(ctx, q, link.ChildID)
		if err != nil {
			return "", err
		}
		if released.Status != "succeeded" || released.State.Phase != "complete" || released.ExpectedInstance != source.State.Witness || released.Input.SourceOperation != source.OperationID || released.Input.SlotDestination == nil || released.Input.SlotDestination.OperationID != id || released.Input.SlotDestination.ID != sandbox.ID {
			return "", fmt.Errorf("%w: paused release original deletion receipt changed", ErrManualRecovery)
		}
		return id, nil
	}
	return "", fmt.Errorf("%w: paused sandbox lacks original successful VM deletion proof", ErrManualRecovery)
}

func (service *Service) admitPausedRelease(ctx context.Context, parent Operation, input lifecycleInput, mac string, upper resourceParent) (Operation, error) {
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
	if !reflect.DeepEqual(current, input.Sandbox) || !reflect.DeepEqual(settings, input.Settings) || current.State != "paused" || current.VMID != "" || current.Instance != "" {
		return Operation{}, fmt.Errorf("%w: paused release source changed", ErrConflict)
	}
	var fence string
	fenceErr := tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", current.ID).Scan(&fence)
	if fenceErr != nil && !errors.Is(fenceErr, sql.ErrNoRows) {
		return Operation{}, fenceErr
	}
	if fence != input.PriorFence {
		return Operation{}, fmt.Errorf("%w: paused release source fence changed", ErrConflict)
	}
	source, err := loadStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return Operation{}, err
	}
	if source.Hash != input.Source.InputHash || source.State.Witness != input.SourceInstance || source.Status != "succeeded" {
		return Operation{}, fmt.Errorf("%w: paused release original source changed", ErrManualRecovery)
	}
	proof, err := pausedReceipt(ctx, tx, current, source)
	if err != nil {
		return Operation{}, err
	}
	input.PauseOperation = proof
	snapshot, err := service.snapshotPublication(ctx, tx, input.OwnerTenantID, current.SnapshotID)
	if err != nil {
		return Operation{}, err
	}
	desired := settings
	desired.Generation = parent.ID
	desired.ActiveEpoch = ""
	desired.Instance = ""
	desired.Deadline = time.Time{}
	desired.PausedUntil = time.Time{}
	next := current
	next.State = "terminated"
	next.OperationID = ""
	parent.Result.SnapshotID = current.SnapshotID
	frozen := encode(struct {
		Action  string
		Request Command
	}{"release", input.Command})
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
		{"INSERT INTO operation_steps VALUES(?,'lifecycle','release',?,?,?,?,'succeeded',?)", []any{
			parent.ID,
			digest(payload),
			payload,
			current.ID,
			"",
			encode(lifecycleState{
				Phase:      "complete",
				SnapshotID: current.SnapshotID,
			}),
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
		{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{
			input.OwnerTenantID,
			current.ID,
			"sleep:" + current.ID,
		}},
		{"DELETE FROM quota_resources WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND sandbox_slots=1 AND storage_mib=0 AND source_operation_id=?", []any{
			input.OwnerTenantID,
			current.ID,
			source.Input.SlotOperation,
		}},
		{"UPDATE sandboxes SET state='terminated',body=? WHERE id=? AND tenant=? AND state='paused' AND body=?", []any{
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
		{"INSERT INTO cleanup_intents VALUES(?,?,'snapshot',?,'pending',?)", []any{
			"sleep:" + parent.ID,
			parent.ID,
			current.SnapshotID,
			encode(snapshot),
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return Operation{}, err
		}
	}
	if input.PriorFence != "" {
		if err = requireWrite(tx.ExecContext(ctx, "DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?", current.ID, input.PriorFence)); err != nil {
			return Operation{}, err
		}
	}
	if upper != nil {
		if err = upper.admitResource(ctx, service, tx, parent, "lifecycle", input.Sandbox.ID, digest(frozen)); err != nil {
			return Operation{}, err
		}
	}
	if err = service.fault("paused_release_before_commit"); err != nil {
		return Operation{}, err
	}
	if err = commitPublications(tx); err != nil {
		return Operation{}, err
	}
	service.mu.Lock()
	delete(service.secrets, current.ID)
	service.mu.Unlock()
	parent.Status = "succeeded"
	return parent, service.store.checkpoint()
}

func (service *Service) retireSupersededLifecycle(tx *sql.Tx, step lifecycleStep) error {
	id := step.Input.PriorFence
	if id == "" {
		return nil
	}
	prior, err := loadLifecycle(service.ctx, tx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return service.retireSupersededWarm(tx, step)
	}
	if err != nil {
		return err
	}
	if prior.Input.Sandbox.ID != step.Input.Sandbox.ID || prior.Input.OwnerTenantID != step.Input.OwnerTenantID {
		return fmt.Errorf("release superseded lifecycle scope changed")
	}
	if prior.Status == "succeeded" {
		return fmt.Errorf("%w: release fence referred to completed lifecycle", ErrManualRecovery)
	}
	sleepSnapshot := ""
	if prior.Input.Command.Action == "resume" {
		sleepSnapshot = prior.Input.Sandbox.SnapshotID
	} else if prior.Input.Command.Action == "pause" && prior.State.Phase == "captured" {
		sleepSnapshot = prior.State.SnapshotID
	}
	if sleepSnapshot != "" {
		source, err := service.snapshotPublication(service.ctx, tx, step.Input.OwnerTenantID, sleepSnapshot)
		if err != nil {
			return err
		}
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='sleep' AND holder_id=?", []any{
				sleepSnapshot,
				step.Input.Sandbox.ID,
			}},
			{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{
				step.Input.OwnerTenantID,
				sleepSnapshot,
				"sleep:" + step.Input.Sandbox.ID,
			}},
			{"INSERT INTO cleanup_intents VALUES(?,?,'snapshot',?,'pending',?)", []any{
				"sleep:" + step.OperationID,
				step.OperationID,
				sleepSnapshot,
				encode(source),
			}},
		} {
			if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
				return err
			}
		}
	}
	if prior.Input.Rollback != nil {
		if err = service.retireRollbackLease(tx, prior); err != nil {
			return err
		}
	}
	if !terminal(prior.Status) {
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key='lifecycle' AND input_hash=? AND status IN ('pending','running')", encode(lifecycleState{
			Phase: "superseded",
		}), id, prior.Hash)); err != nil {
			return err
		}
	}
	var status string
	var body []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT status,body FROM operations WHERE id=?", id).Scan(&status, &body); err != nil {
		return err
	}
	if !terminal(status) {
		var result operationResult
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "sandbox operation superseded by release"
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", encode(result), id)); err != nil {
			return err
		}
	}
	return nil
}
