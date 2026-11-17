package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

func (service *Lifecycle) AdmitResume(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if command.Action != "resume" || command.SandboxID == "" || key == "" || len(key) > 256 || principal.TenantID == "" || principal.SandboxID != "" {
		return empty, fmt.Errorf("%w: resume requires tenant authority, sandbox and key", errs.ErrInvalid)
	}
	credentials := service.access.SandboxSecrets(command.SandboxID)
	requestGuard := service.guards.TenantRequest(principal.TenantID, key)
	requestGuard.Lock()
	defer requestGuard.Unlock()
	mac := service.commands.Identity.CommandMAC(principal, command)
	if operation, found, err := service.existing(ctx, principal, key, &ClaimAdmission{
		MAC:     mac,
		Secrets: credentials,
	}); found || err != nil {
		return operation, err
	}
	guard := service.guards.Configuration(command.SandboxID)
	guard.Lock()
	defer guard.Unlock()

	sandboxRecord, err := service.catalog.OwnedRecord(ctx, principal, command.SandboxID)
	if err != nil {
		return empty, err
	}
	if sandboxRecord.State != "paused" || sandboxRecord.SnapshotID == "" {
		return empty, fmt.Errorf("%w: sandbox is not paused", errs.ErrConflict)
	}
	settings, err := LoadSettings(ctx, service.store.Database, sandboxRecord.ID)
	if err != nil {
		return empty, err
	}
	if !settings.PausedUntil.IsZero() && !time.Now().Before(settings.PausedUntil) {
		return empty, fmt.Errorf("%w: paused sandbox expired", errs.ErrConflict)
	}
	if service.commands.CredentialsMissing(sandboxRecord.ID, settings) {
		return empty, fmt.Errorf("%w: host credentials must be supplied again before resume", errs.ErrConflict)
	}
	source, err := service.proofs.Snapshots().LoadRestoreSource(ctx, service.store.Database, sandboxRecord.TenantID, sandboxRecord.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = machine.VerifyRestoreSourceFiles(source); err != nil {
		return empty, err
	}
	if err = service.opts.Backend.Check(ctx, source.Snapshot.Manifest.Config); err != nil {
		return empty, err
	}
	prior, err := machine.LoadResourceStep(ctx, service.store.Database, sandboxRecord.SourceOperation)
	if err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, service.store.Database, source.Snapshot.Manifest.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	input := LifecycleInput{
		RequestTenantID: principal.TenantID,
		OwnerTenantID:   sandboxRecord.TenantID,
		Command:         operation.SanitizedCommand(command),
		Sandbox:         sandboxRecord,
		Settings:        settings,
		Source: ownership.GenerationRef{
			OperationID: prior.OperationID,
			InputHash:   prior.Hash,
		},
		SourceInstance: prior.State.Witness,
	}
	parent := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:  sandboxRecord.ID,
			SnapshotID: sandboxRecord.SnapshotID,
		},
	}
	admission := &LifecycleAdmission{
		History: service.proofs.Snapshots(),
		Parent:  parent,
		Input:   input,
		MAC:     mac,
		Role:    "clone",
	}
	request := machine.CloneRequest{
		TenantID:   sandboxRecord.TenantID,
		Key:        parent.ID + ":clone",
		SnapshotID: sandboxRecord.SnapshotID,
	}
	frozen := operation.Encode(struct {
		Action  string
		Request machine.CloneRequest
	}{"clone", request})
	child, err := service.provisioning.Admit(ctx, sandboxRecord.TenantID, request.Key, frozen, operation.Digest(frozen), supply.TemplateRecord{
		ID:       prior.Input.TemplateID,
		TenantID: sandboxRecord.TenantID,
		Config:   source.Snapshot.Manifest.Config,
	}, manifest, &source, nil, &ProvisionTarget{
		SlotHolder:    "sleep:" + sandboxRecord.ID,
		Sandbox:       sandboxRecord,
		SlotOperation: prior.Input.SlotOperation,
		Parent:        admission,
	})
	if child.ID == "" {
		return empty, err
	}
	parent.Result.VMID = child.Result.VMID
	service.access.SetOperationSecrets(parent.ID, credentials)
	if err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Lifecycle) restoreActivation(step LifecycleStep, child machine.ResourceStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = LoadActivation(service.ctx, tx, step.OperationID); err == nil {
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
	current, err := machine.LoadResourceStep(service.ctx, tx, child.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, child) || child.Status != "succeeded" || child.State.Witness == "" || child.Input.SandboxID != step.Input.Sandbox.ID || child.Input.Restore == nil || child.Input.Restore.Snapshot.ID != expectedSnapshot {
		return fmt.Errorf("%w: resume lacks frozen successful clone witness", errs.ErrManualRecovery)
	}
	settings := step.Input.Settings
	settings.Generation = step.OperationID
	settings.ActiveEpoch = ""
	settings.Instance = ""
	settings.Deadline = time.Time{}
	settings.PausedUntil = time.Time{}
	input := ActivationInput{
		RequestTenantID:   step.Input.RequestTenantID,
		TenantID:          step.Input.OwnerTenantID,
		SandboxID:         step.Input.Sandbox.ID,
		ResourceOperation: child.OperationID,
		ResourceHash:      child.Hash,
		ExpectedInstance:  child.State.Witness,
		Settings:          settings,
		ResetIdentity:     false,
	}
	payload := operation.Encode(input)
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(service.ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    input.SandboxID,
		PreviousBody: operation.Encode(step.Input.Settings),
		NextBody:     operation.Encode(settings),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(service.ctx, dbgen.InsertStepParams{
		OperationID:         step.OperationID,
		StepKey:             "activation",
		Action:              "activate",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: child.Candidate,
		ExpectedInstance:    child.State.Witness,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.Checkpoint()
}

func (service *Lifecycle) runResume(step LifecycleStep) error {
	link, err := service.runResourceLink(step.OperationID, "clone")
	if err != nil {
		if !errors.Is(err, errs.ErrInterrupted) && link.ChildID != "" {
			if candidate, lookup := machine.LoadResourceStep(service.ctx, service.store.Database, link.ChildID); lookup == nil && candidate.Status == "failed" {
				return service.compensateResumeCandidate(step, candidate)
			}
		}
		return err
	}
	child, err := machine.LoadResourceStep(service.ctx, service.store.Database, link.ChildID)
	if err != nil {
		return err
	}
	if err = service.restoreActivation(step, child); err != nil {
		return err
	}
	lock := service.guards.VM(child.Candidate)
	lock.Lock()
	err = service.activations.Continue(step.OperationID)
	lock.Unlock()
	if activation, lookup := LoadActivation(service.ctx, service.store.Database, step.OperationID); lookup == nil && activation.Status == "failed" {
		return service.compensateResumeCandidate(step, child)
	}
	return err
}
