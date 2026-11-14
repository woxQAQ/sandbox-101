package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/loom"
)

type Checkpoint struct {
	TemplateID string     `json:"template_id,omitempty"`
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	SandboxID  string     `json:"sandbox_id"`
	SnapshotID string     `json:"snapshot_id"`
	Shape      loom.Shape `json:"shape"`
	CreatedAt  time.Time  `json:"created_at"`
}

func (service *Service) AdmitLifecycle(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	return service.admitLifecycle(ctx, principal, key, command, nil)
}

func (service *Service) admitLifecycle(ctx context.Context, principal Principal, key string, command Command, upper resourceParent) (Operation, error) {
	var empty Operation
	if key == "" || len(key) > 256 || command.SandboxID == "" || principal.TenantID == "" || (principal.SandboxID != "" && (principal.SandboxID != command.SandboxID || command.Action != "release")) {
		return empty, fmt.Errorf("%w: lifecycle request requires scoped sandbox and key", ErrInvalid)
	}
	if command.Action != "release" && command.Action != "checkpoint" && command.Action != "pause" {
		return empty, fmt.Errorf("%w: unsupported lifecycle action", ErrInvalid)
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
	settings, err := loadSettings(ctx, service.store.db, sandbox.ID)
	if err != nil {
		return empty, err
	}
	if !command.LeaseDeadline.IsZero() {
		deadline := settings.Deadline
		if sandbox.State == "paused" {
			deadline = settings.PausedUntil
		}
		if !deadline.Equal(command.LeaseDeadline) || deadline.IsZero() || time.Now().Before(deadline) {
			return empty, fmt.Errorf("%w: expiration lease was renewed or is not due", ErrConflict)
		}
	}
	allowed := sandbox.State == "running" || (sandbox.State == "paused" && command.Action == "checkpoint") || (command.Action == "release" && (sandbox.State == "provisioning" || sandbox.State == "paused"))
	if !allowed {
		return empty, fmt.Errorf("%w: sandbox state cannot enter requested lifecycle", ErrConflict)
	}
	if sandbox.SourceOperation == "" {
		return empty, fmt.Errorf("%w: fork placement has not committed its allocation", ErrConflict)
	}
	source, err := loadStep(ctx, service.store.db, sandbox.SourceOperation)
	if err != nil {
		return empty, err
	}
	if source.Status == "failed" && source.Input.FailureSlotDestination != nil {
		return empty, fmt.Errorf("%w: resume compensation must finish before release", ErrConflict)
	}
	var priorFence string
	if command.Action == "release" {
		if err = service.store.db.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", sandbox.ID).Scan(&priorFence); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return empty, err
		}
	}
	input := lifecycleInput{
		PriorFence:      priorFence,
		SourceInstance:  source.State.Witness,
		RequestTenantID: principal.TenantID,
		OwnerTenantID:   sandbox.TenantID,
		Command:         sanitizedCommand(command),
		Sandbox:         sandbox,
		Settings:        settings,
		Source: generationRef{
			OperationID: source.OperationID,
			InputHash:   source.Hash,
		},
	}
	if command.Action == "release" && sandbox.State != "paused" && source.Status == "succeeded" {
		refs, err := failedCaptureReferences(ctx, service.store.db, source)
		if err != nil {
			return empty, err
		}
		input.CaptureFailures = refs
	}
	if command.Action == "checkpoint" {
		input.CheckpointID = newID("cp_")
	}
	parent := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "pending",
		Result: operationResult{
			SandboxID:    sandbox.ID,
			VMID:         sandbox.VMID,
			CheckpointID: input.CheckpointID,
		},
	}
	if command.Action == "checkpoint" && sandbox.State == "paused" {
		return service.admitPausedCheckpoint(ctx, parent, input, mac)
	}
	if command.Action == "release" && sandbox.State == "paused" {
		return service.admitPausedRelease(ctx, parent, input, mac, upper)
	}
	admission := &lifecycleAdmission{
		Upper:  upper,
		Parent: parent,
		Input:  input,
		MAC:    mac,
		Role:   "capture",
	}
	var child Operation
	if command.Action == "release" {
		admission.Role = "release"
		if len(input.CaptureFailures) > 0 {
			admission.Role = "stop"
			child, err = service.admitStop(ctx, StopRequest{
				TenantID:         sandbox.TenantID,
				Key:              parent.ID + ":stop",
				SandboxID:        sandbox.ID,
				ExpectedInstance: source.State.Witness,
			}, admission)
		} else if source.Status == "failed" {
			child, err = service.admitDiscard(ctx, DiscardRequest{
				TenantID:          sandbox.TenantID,
				Key:               parent.ID + ":release",
				SandboxID:         sandbox.ID,
				FailedOperationID: source.OperationID,
				ExpectedInstance:  source.Input.Resource.Instance,
			}, admission)
		} else {
			child, err = service.admitRelease(ctx, ReleaseRequest{
				TenantID:  sandbox.TenantID,
				Key:       parent.ID + ":release",
				SandboxID: sandbox.ID,
			}, admission)
		}
	} else {
		child, err = service.admitCapture(ctx, CaptureRequest{
			TenantID:         sandbox.TenantID,
			Key:              parent.ID + ":capture",
			SandboxID:        sandbox.ID,
			ExpectedInstance: sandbox.Instance,
		}, admission)
	}
	if child.ID == "" {
		return empty, err
	}
	if err != nil && !errors.Is(err, ErrInterrupted) {
		return parent, err
	}
	parent.Result.SnapshotID = child.Result.SnapshotID
	if command.Action == "release" {
		service.closeConnections(sandbox.ID)
		if revokeErr := service.revokeSandbox(sandbox.ID); revokeErr != nil {
			return parent, revokeErr
		}
	}
	if err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Service) lifecycleProof(ctx context.Context, tx *sql.Tx, step lifecycleStep) error {
	current, err := loadLifecycle(ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("lifecycle current intent changed")
	}
	var fence string
	if err = tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", step.Input.Sandbox.ID).Scan(&fence); err != nil {
		return err
	}
	if fence != step.OperationID {
		return fmt.Errorf("%w: lifecycle was superseded", ErrConflict)
	}
	settings, err := loadSettings(ctx, tx, step.Input.Sandbox.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(settings, step.Input.Settings) {
		return fmt.Errorf("%w: lifecycle source settings changed", ErrConflict)
	}
	source, err := loadStep(ctx, tx, step.Input.Source.OperationID)
	if err != nil {
		return err
	}
	sourceTerminal := source.Status == "succeeded" && source.State.Witness != ""
	if step.Input.Command.Action == "release" && source.Status == "failed" && source.State.Witness == "" && step.Input.Sandbox.State == "provisioning" {
		sourceTerminal = true
	}
	if source.Hash != step.Input.Source.InputHash || source.Input.SandboxID != step.Input.Sandbox.ID || source.Input.TenantID != step.Input.OwnerTenantID || source.State.Witness != step.Input.SourceInstance || !sourceTerminal {
		return fmt.Errorf("lifecycle original resource witness changed")
	}
	var payload []byte
	var scope, tenant, status, hash string
	if err = tx.QueryRowContext(ctx, "SELECT scope_kind,scope_id,status,request_hash,request FROM operations WHERE id=?", step.OperationID).Scan(&scope, &tenant, &status, &hash, &payload); err != nil {
		return err
	}
	var request struct {
		Action  string
		Request Command
	}
	if err = json.Unmarshal(payload, &request); err != nil {
		return err
	}
	if scope != "tenant" || tenant != step.Input.RequestTenantID || terminal(status) || digest(payload) != hash || request.Action != step.Input.Command.Action || !reflect.DeepEqual(request.Request, step.Input.Command) {
		return fmt.Errorf("lifecycle parent identity changed")
	}
	return nil
}

func (service *Service) completeCheckpoint(step lifecycleStep, capture captureStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.lifecycleProof(service.ctx, tx, step); err != nil {
		return err
	}
	current, err := loadCapture(service.ctx, tx, capture.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, capture) || capture.Status != "succeeded" || capture.State.Phase != "complete" || capture.State.ResumeRequired || !capture.State.ArtifactsSynced {
		return fmt.Errorf("checkpoint lacks original capture publication")
	}
	cp := Checkpoint{
		TemplateID: capture.Input.Source.Input.TemplateID,
		ID:         step.Input.CheckpointID,
		TenantID:   step.Input.OwnerTenantID,
		SandboxID:  step.Input.Sandbox.ID,
		SnapshotID: capture.Input.Snapshot.ID,
		Shape:      capture.Input.Source.Input.Resource.Config.Shape,
		CreatedAt:  time.Now().UTC(),
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO checkpoint_bindings VALUES(?,?,?,?,?)", []any{
			cp.ID,
			cp.TenantID,
			cp.SnapshotID,
			step.OperationID,
			encode(cp),
		}},
		{"INSERT INTO resource_refs VALUES('snapshot',?,'checkpoint_binding',?)", []any{
			cp.SnapshotID,
			cp.ID,
		}},
		{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'active_use',?)", []any{
			cp.TenantID,
			cp.SnapshotID,
			"checkpoint-binding:" + cp.ID,
		}},
		{"DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?", []any{
			cp.SandboxID,
			step.OperationID,
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='lifecycle' AND status IN ('pending','running') AND input_hash=?", []any{
			encode(lifecycleState{
				Phase:      "complete",
				SnapshotID: cp.SnapshotID,
			}),
			step.OperationID,
			step.Hash,
		}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{
			step.OperationID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("checkpoint_completion_before_commit"); err != nil {
		return err
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) completeReleaseLifecycle(step lifecycleStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.lifecycleProof(service.ctx, tx, step); err != nil {
		return err
	}
	link, status, err := loadResourceLink(service.ctx, tx, step.OperationID, "release")
	if err != nil {
		return err
	}
	if status != "succeeded" {
		return ErrInterrupted
	}
	switch link.Kind {
	case "release":
		released, err := loadStep(service.ctx, tx, link.ChildID)
		if err != nil {
			return err
		}
		if released.Status != "succeeded" || released.State.Phase != "complete" || released.ExpectedInstance != step.Input.Sandbox.Instance || released.Input.SourceOperation != step.Input.Source.OperationID {
			return fmt.Errorf("lifecycle release lacks exact deletion proof")
		}
	case "discard":
		discarded, err := loadDiscard(service.ctx, tx, link.ChildID)
		if err != nil {
			return err
		}
		if discarded.Status != "succeeded" || discarded.State.Phase != "complete" || discarded.Input.Source.OperationID != step.Input.Source.OperationID || discarded.Input.Request.SandboxID != step.Input.Sandbox.ID {
			return fmt.Errorf("lifecycle release lacks exact failed-candidate reclamation proof")
		}
	default:
		return fmt.Errorf("unsupported release child kind")
	}
	if err = service.retireSupersededLifecycle(tx, step); err != nil {
		return err
	}
	tombstone, err := loadSandbox(service.ctx, tx, step.Input.Sandbox.ID, step.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if tombstone.State != "terminated" || tombstone.OperationID != link.ChildID || tombstone.SourceOperation != step.Input.Source.OperationID || tombstone.VMID != step.Input.Sandbox.VMID || tombstone.Instance != step.Input.Sandbox.Instance {
		return fmt.Errorf("%w: release business ownership changed after reclamation", ErrManualRecovery)
	}
	retired := tombstone
	retired.VMID, retired.Instance, retired.OperationID = "", "", ""
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE sandboxes SET body=? WHERE id=? AND tenant=? AND state='terminated' AND body=?", encode(retired), retired.ID, retired.TenantID, encode(tombstone))); err != nil {
		return err
	}
	desired := step.Input.Settings
	desired.Generation = step.OperationID
	desired.ActiveEpoch = ""
	desired.Instance = ""
	desired.Deadline = time.Time{}
	desired.PausedUntil = time.Time{}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", encode(desired), step.Input.Sandbox.ID, encode(step.Input.Settings))); err != nil {
		return err
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?", []any{
			step.Input.Sandbox.ID,
			step.OperationID,
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='lifecycle' AND status IN ('pending','running') AND input_hash=?", []any{
			encode(lifecycleState{
				Phase: "complete",
			}),
			step.OperationID,
			step.Hash,
		}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{
			step.OperationID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	service.mu.Lock()
	delete(service.secrets, step.Input.Sandbox.ID)
	delete(service.opSecrets, step.OperationID)
	service.mu.Unlock()
	return service.store.checkpoint()
}

func (service *Service) runLifecycle(step lifecycleStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, ErrInterrupted) && service.ctx.Err() == nil {
			runErr = errors.Join(runErr, service.failLifecycle(step))
		}
	}()
	if terminal(step.Status) {
		return nil
	}
	switch step.Input.Command.Action {
	case "release":
		service.closeConnections(step.Input.Sandbox.ID)
		if err := service.revokeSandbox(step.Input.Sandbox.ID); err != nil {
			return err
		}
		if len(step.Input.CaptureFailures) > 0 {
			if err := service.runReleaseCaptures(step); err != nil {
				return err
			}
		}
		if _, err := service.runResourceLink(step.OperationID, "release"); err != nil {
			return err
		}
		return service.completeReleaseLifecycle(step)
	case "rollback":
		return service.runRollback(step)
	case "resume":
		return service.runResume(step)
	case "pause":
		return service.runPause(step)
	case "checkpoint":
		link, err := service.runResourceLink(step.OperationID, "capture")
		if err != nil {
			return err
		}
		capture, err := loadCapture(service.ctx, service.store.db, link.ChildID)
		if err != nil {
			return err
		}
		return service.completeCheckpoint(step, capture)
	default:
		return fmt.Errorf("%w: unsupported lifecycle workflow", ErrInvalid)
	}
}
