package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) AdmitResume(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	var empty Operation
	if command.Action != "resume" || command.SandboxID == "" || key == "" || len(key) > 256 || principal.TenantID == "" || principal.SandboxID != "" {
		return empty, loom.Fail(loom.Invalid, "resume requires tenant authority, sandbox and key")
	}
	service.mu.Lock()
	credentials := service.secrets[command.SandboxID]
	service.mu.Unlock()
	requestGuard := service.vmLock("request:" + string(encode([]string{
		principal.TenantID,
		key,
	})))
	requestGuard.Lock()
	defer requestGuard.Unlock()
	mac := service.store.commandMAC(principal, command)
	if operation, found, err := service.existingSandboxOperation(ctx, principal, key, &claimAdmission{
		MAC:     mac,
		Secrets: credentials,
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
	if sandbox.State != "paused" || sandbox.SnapshotID == "" {
		return empty, loom.Fail(loom.Conflict, "sandbox is not paused")
	}
	settings, err := loadSettings(ctx, service.store.db, sandbox.ID)
	if err != nil {
		return empty, err
	}
	if !settings.PausedUntil.IsZero() && !time.Now().Before(settings.PausedUntil) {
		return empty, loom.Fail(loom.Conflict, "paused sandbox expired")
	}
	if service.credentialsMissing(sandbox.ID, settings) {
		return empty, loom.Fail(loom.Conflict, "host credentials must be supplied again before resume")
	}
	source, err := service.loadRestoreSource(ctx, service.store.db, sandbox.TenantID, sandbox.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = verifyRestoreSourceFiles(source); err != nil {
		return empty, err
	}
	if err = service.opts.Backend.Check(ctx, source.Snapshot.Manifest.Config); err != nil {
		return empty, err
	}
	prior, err := loadStep(ctx, service.store.db, sandbox.SourceOperation)
	if err != nil {
		return empty, err
	}
	image, err := loadImage(ctx, service.store.db, source.Snapshot.Manifest.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = verifyImageFiles(image, filepath.Join(service.store.directory, "images", image.Digest[7:])); err != nil {
		return empty, err
	}
	input := lifecycleInput{
		RequestTenantID: principal.TenantID,
		OwnerTenantID:   sandbox.TenantID,
		Command:         sanitizedCommand(command),
		Sandbox:         sandbox,
		Settings:        settings,
		Source: generationRef{
			OperationID: prior.OperationID,
			InputHash:   prior.Hash,
		},
		SourceInstance: prior.State.Witness,
	}
	parent := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "pending",
		Result: operationResult{
			SandboxID:  sandbox.ID,
			SnapshotID: sandbox.SnapshotID,
		},
	}
	admission := &lifecycleAdmission{
		Parent: parent,
		Input:  input,
		MAC:    mac,
		Role:   "clone",
	}
	request := CloneRequest{
		TenantID:   sandbox.TenantID,
		Key:        parent.ID + ":clone",
		SnapshotID: sandbox.SnapshotID,
	}
	frozen := encode(struct {
		Action  string
		Request CloneRequest
	}{"clone", request})
	child, err := service.admitProvision(ctx, sandbox.TenantID, request.Key, frozen, digest(frozen), templateRecord{
		ID:       prior.Input.TemplateID,
		TenantID: sandbox.TenantID,
		Config:   source.Snapshot.Manifest.Config,
	}, image, &source, nil, &provisionTarget{
		SlotHolder:    "sleep:" + sandbox.ID,
		Sandbox:       sandbox,
		SlotOperation: prior.Input.SlotOperation,
		Parent:        admission,
	})
	if child.ID == "" {
		return empty, err
	}
	parent.Result.VMID = child.Result.VMID
	service.mu.Lock()
	service.opSecrets[parent.ID] = credentials
	service.mu.Unlock()
	if err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Service) restoreActivation(step lifecycleStep, child resourceStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = loadActivation(service.ctx, tx, step.OperationID); err == nil {
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = service.lifecycleProof(service.ctx, tx, step); err != nil {
		return err
	}
	expectedSnapshot := step.Input.Sandbox.SnapshotID
	if step.Input.Rollback != nil {
		expectedSnapshot = step.Input.Rollback.Checkpoint.SnapshotID
	}
	current, err := loadStep(service.ctx, tx, child.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, child) || child.Status != "succeeded" || child.State.Witness == "" || child.Input.SandboxID != step.Input.Sandbox.ID || child.Input.Restore == nil || child.Input.Restore.Snapshot.ID != expectedSnapshot {
		return loom.Fail(loom.ManualRecovery, "resume lacks frozen successful clone witness")
	}
	settings := step.Input.Settings
	settings.Generation = step.OperationID
	settings.ActiveEpoch = ""
	settings.Instance = ""
	settings.Deadline = time.Time{}
	settings.PausedUntil = time.Time{}
	input := activationInput{
		RequestTenantID:   step.Input.RequestTenantID,
		TenantID:          step.Input.OwnerTenantID,
		SandboxID:         step.Input.Sandbox.ID,
		ResourceOperation: child.OperationID,
		ResourceHash:      child.Hash,
		ExpectedInstance:  child.State.Witness,
		Settings:          settings,
		ResetIdentity:     false,
	}
	payload := encode(input)
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", []any{
			encode(settings),
			input.SandboxID,
			encode(step.Input.Settings),
		}},
		{"INSERT INTO operation_steps VALUES(?,'activation','activate',?,?,?,?,'pending',?)", []any{
			step.OperationID,
			digest(payload),
			payload,
			child.Candidate,
			child.State.Witness,
			encode("admitted"),
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) runResume(step lifecycleStep) error {
	link, err := service.runResourceLink(step.OperationID, "clone")
	if err != nil {
		if !errors.Is(err, ErrInterrupted) && link.ChildID != "" {
			if candidate, lookup := loadStep(service.ctx, service.store.db, link.ChildID); lookup == nil && candidate.Status == "failed" {
				return service.compensateResumeCandidate(step, candidate)
			}
		}
		return err
	}
	child, err := loadStep(service.ctx, service.store.db, link.ChildID)
	if err != nil {
		return err
	}
	if err = service.restoreActivation(step, child); err != nil {
		return err
	}
	lock := service.vmLock(child.Candidate)
	lock.Lock()
	err = service.continueActivation(step.OperationID)
	lock.Unlock()
	if activation, lookup := loadActivation(service.ctx, service.store.db, step.OperationID); lookup == nil && activation.Status == "failed" {
		return service.compensateResumeCandidate(step, child)
	}
	return err
}

func (service *Service) GetCheckpoint(ctx context.Context, principal Principal, id string) (Checkpoint, error) {
	var checkpoint Checkpoint
	var body []byte
	if err := service.store.db.QueryRowContext(ctx, "SELECT body FROM checkpoint_bindings WHERE id=?", id).Scan(&body); err != nil {
		return checkpoint, err
	}
	if err := json.Unmarshal(body, &checkpoint); err != nil {
		return checkpoint, err
	}
	if principal.SandboxID != "" || (!principal.Root && checkpoint.TenantID != principal.TenantID) {
		return Checkpoint{}, loom.Fail(loom.NotFound, "not found")
	}
	return checkpoint, nil
}
