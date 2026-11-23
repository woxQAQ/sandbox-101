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
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

func (l *Lifecycle) AdmitResume(ctx context.Context, principal authorization.Principal, key string, command operation.Intent) (operation.State, error) {
	var empty operation.State
	if command.Action != "resume" || command.SandboxID == "" || key == "" || len(key) > 256 || principal.TenantID == "" || principal.SandboxID != "" {
		return empty, fmt.Errorf("%w: resume requires tenant authority, sandbox and key", errs.ErrInvalid)
	}
	credentials := l.registry.SandboxSecrets(command.SandboxID)
	releaseKey := l.tenantIdempotencyLocks.Lock(principal.TenantID, key)
	defer releaseKey()
	mac := l.identity.CommandMAC(principal, command)
	if operation, found, err := l.existing(ctx, principal, key, operation.CommandCredentials{
		MAC:     mac,
		Secrets: credentials,
	}); found || err != nil {
		return operation, err
	}
	unlock := l.sandboxConfigLocks.Lock(command.SandboxID)
	defer unlock()

	sandboxRecord, err := l.catalog.OwnedRecord(ctx, principal, command.SandboxID)
	if err != nil {
		return empty, err
	}
	if sandboxRecord.State != "paused" || sandboxRecord.SnapshotID == "" {
		return empty, fmt.Errorf("%w: sandbox is not paused", errs.ErrConflict)
	}
	settings, err := LoadSettings(ctx, l.store.Database, sandboxRecord.ID)
	if err != nil {
		return empty, err
	}
	if !settings.PausedUntil.IsZero() && !time.Now().Before(settings.PausedUntil) {
		return empty, fmt.Errorf("%w: paused sandbox expired", errs.ErrConflict)
	}
	if l.admissionPolicy.CredentialsMissing(sandboxRecord.ID, settings) {
		return empty, fmt.Errorf("%w: host credentials must be supplied again before resume", errs.ErrConflict)
	}
	source, err := l.proofs.Snapshots().LoadSnapshotSource(ctx, l.store.Database, sandboxRecord.TenantID, sandboxRecord.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = machine.VerifyRestoreSourceFiles(source); err != nil {
		return empty, err
	}
	if err = l.opts.Backend.ValidateHostConfig(ctx, source.Snapshot.Manifest.Config); err != nil {
		return empty, err
	}
	prior, err := machine.LoadResourceStep(ctx, l.store.Database, sandboxRecord.SourceOperation)
	if err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, l.store.Database, source.Snapshot.Manifest.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(l.store.Directory, "images", manifest.Digest[7:])); err != nil {
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
	parent := operation.State{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:  sandboxRecord.ID,
			SnapshotID: sandboxRecord.SnapshotID,
		},
	}
	admission := &LifecycleAdmission{
		History: l.proofs.Snapshots(),
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
	frozen := machine.FreezeProvisionRequest(request, nil)
	child, err := l.provisioning.Admit(ctx, ProvisionAdmission{
		Request: supply.ProvisionRequest{
			TenantID: sandboxRecord.TenantID,
			Key:      request.Key,
			Frozen:   frozen,
			Template: supply.TemplateRecord{
				ID:       prior.Input.TemplateID,
				TenantID: sandboxRecord.TenantID,
				Config:   source.Snapshot.Manifest.Config,
			},
			Image:   manifest,
			Restore: &source,
		},
		Target: &ProvisionTarget{
			SlotHolder:    "sleep:" + sandboxRecord.ID,
			Sandbox:       sandboxRecord,
			SlotOperation: prior.Input.SlotOperation,
			Parent:        admission,
		},
	})
	if child.ID == "" {
		return empty, err
	}
	parent.Result.VMID = child.Result.VMID
	l.registry.SetOperationSecrets(parent.ID, credentials)
	if err != nil {
		return parent, err
	}
	l.dispatch(parent.ID)
	return parent, nil
}

func (l *Lifecycle) restoreActivation(step LifecycleStep, child machine.ResourceStep) error {
	l.storeWriteMu.Lock()
	defer l.storeWriteMu.Unlock()
	tx, err := l.store.Database.BeginTx(l.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = LoadActivation(l.ctx, tx, step.OperationID); err == nil {
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = l.lifecycleProof(l.ctx, tx, step); err != nil {
		return err
	}
	expectedSnapshot := step.Input.Sandbox.SnapshotID
	if step.Input.Rollback != nil {
		expectedSnapshot = step.Input.Rollback.Checkpoint.SnapshotID
	}
	current, err := machine.LoadResourceStep(l.ctx, tx, child.OperationID)
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
	if err = replaceSettings(l.ctx, tx, input.SandboxID, step.Input.Settings, settings); err != nil {
		return err
	}
	if err = insertActivationStep(l.ctx, tx, step.OperationID, child.Candidate, input); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return l.store.Checkpoint()
}

func (l *Lifecycle) runResume(step LifecycleStep) error {
	link, err := l.runResourceLink(step.OperationID, "clone")
	if err != nil {
		if !errors.Is(err, errs.ErrInterrupted) && link.ChildID != "" {
			if candidate, lookup := machine.LoadResourceStep(l.ctx, l.store.Database, link.ChildID); lookup == nil && candidate.Status == "failed" {
				return l.compensateResumeCandidate(step, candidate)
			}
		}
		return err
	}
	child, err := machine.LoadResourceStep(l.ctx, l.store.Database, link.ChildID)
	if err != nil {
		return err
	}
	if err = l.restoreActivation(step, child); err != nil {
		return err
	}
	err = l.activations.activateVM(child.Candidate, step.OperationID)
	if activation, lookup := LoadActivation(l.ctx, l.store.Database, step.OperationID); lookup == nil && activation.Status == "failed" {
		return l.compensateResumeCandidate(step, child)
	}
	return err
}

func (l *Lifecycle) finishResumeFailure(step LifecycleStep) error {
	l.storeWriteMu.Lock()
	defer l.storeWriteMu.Unlock()
	tx, err := l.store.Database.BeginTx(l.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadLifecycleStep(l.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.Input, step.Input) {
		return fmt.Errorf("resume failure intent changed")
	}

	sandboxRecord, err := LoadSandbox(l.ctx, tx, step.Input.Sandbox.ID, step.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if sandboxRecord.State != "paused" || sandboxRecord.SnapshotID != step.Input.Sandbox.SnapshotID || sandboxRecord.VMID != "" {
		return fmt.Errorf("%w: failed resume has no trusted paused destination", errs.ErrManualRecovery)
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteFence(l.ctx, dbgen.DeleteFenceParams{
		SandboxID:   sandboxRecord.ID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = transitionLifecycle(l.ctx, tx, current, LifecycleState{
		Phase:      "failed",
		SnapshotID: sandboxRecord.SnapshotID,
	}, "failed"); err != nil {
		return err
	}
	status, result, err := operations.StatusAndResult(l.ctx, step.OperationID)
	if err != nil {
		return err
	}
	var outcomes []operation.Outcome
	if !operation.Terminal(status) {
		result.Error = "resume failed; sleep snapshot retained"
		outcomes = append(outcomes, operation.Outcome{
			ID:     step.OperationID,
			Result: result,
			Failed: true,
		})
	}
	if err = l.committer.Complete(tx, outcomes...); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return l.store.Checkpoint()
}

func (l *Lifecycle) compensateResumeCandidate(step LifecycleStep, candidate machine.ResourceStep) error {
	role, kind := "discard", "discard"
	if candidate.Status == "succeeded" {
		role = "compensate"
		kind = "release"
	}
	_, _, err := machine.LoadResourceLink(l.ctx, l.store.Database, step.OperationID, role)
	if errors.Is(err, sql.ErrNoRows) {
		admission := &LifecycleAdmission{
			History: l.proofs.Snapshots(),
			Parent: operation.State{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  role,
		}
		if kind == "discard" {
			_, err = l.deletion.AdmitDiscardForParent(l.ctx, machine.DiscardRequest{
				TenantID:          step.Input.OwnerTenantID,
				Key:               step.OperationID + ":discard",
				SandboxID:         step.Input.Sandbox.ID,
				FailedOperationID: candidate.OperationID,
				ExpectedInstance:  candidate.Input.Resource.Instance,
			}, admission)
		} else {
			_, err = l.deletion.AdmitReleaseForParent(l.ctx, machine.ReleaseRequest{
				TenantID:  step.Input.OwnerTenantID,
				Key:       step.OperationID + ":compensate",
				SandboxID: step.Input.Sandbox.ID,
			}, admission)
		}
	}
	if err != nil {
		return err
	}
	if _, err = l.runResourceLink(step.OperationID, role); err != nil {
		return err
	}
	return l.finishResumeFailure(step)
}

func ResumeDestination(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination) (LifecycleStep, error) {
	var empty LifecycleStep
	if destination == nil || destination.Kind != "sleep" {
		return empty, fmt.Errorf("%w: resume slot destination missing", errs.ErrManualRecovery)
	}
	parent, err := LoadLifecycleStep(ctx, tx, destination.OperationID)
	if err != nil {
		return parent, err
	}
	if parent.Input.Command.Action != "resume" || parent.Status == "succeeded" || parent.Input.Sandbox.State != "paused" || parent.Input.Sandbox.ID != destination.ID || parent.Input.Sandbox.SnapshotID != destination.SnapshotID {
		return parent, fmt.Errorf("%w: resume slot destination differs from original paused sandbox", errs.ErrManualRecovery)
	}
	fence, err := LifecycleFence(ctx, tx, destination.ID)
	if err != nil {
		return parent, err
	}
	if fence != parent.OperationID {
		return parent, fmt.Errorf("%w: resume compensation was superseded", errs.ErrConflict)
	}
	if refs, err := quota.CountResourceReference(ctx, tx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         destination.SnapshotID,
		HolderKind: "sleep",
		HolderID:   destination.ID,
	}); err != nil || refs != 1 {
		return parent, fmt.Errorf("resume original sleep holder missing: %v", err)
	}
	if refs, err := quota.CountCostReference(ctx, tx, quota.CostReference{
		TenantID:   parent.Input.OwnerTenantID,
		Kind:       "snapshot",
		Key:        destination.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "sleep:" + destination.ID,
	}); err != nil || refs != 1 {
		return parent, fmt.Errorf("resume original sleep fee missing: %v", err)
	}
	return parent, nil
}

func RestoreResumeSlot(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination, cleanup string) error {
	parent, err := ResumeDestination(ctx, tx, destination)
	if err != nil {
		return err
	}
	return quota.TransferCostReference(ctx, tx, quota.CostReference{
		TenantID:   parent.Input.OwnerTenantID,
		Kind:       "sandbox_slot",
		Key:        destination.ID,
		HolderKind: "cleanup",
		HolderID:   cleanup,
	}, "active_use", "sleep:"+destination.ID)
}

func RestorePausedSandbox(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination) error {
	parent, err := ResumeDestination(ctx, tx, destination)
	if err != nil {
		return err
	}
	link, _, err := machine.LoadResourceLink(ctx, tx, parent.OperationID, "clone")
	if err != nil {
		return err
	}
	candidate, err := machine.LoadResourceStep(ctx, tx, link.ChildID)
	if err != nil {
		return err
	}

	current, err := LoadSandbox(ctx, tx, destination.ID, parent.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if current.State != "terminated" || current.SourceOperation != candidate.OperationID || current.VMID != candidate.Candidate {
		return fmt.Errorf("%w: resume candidate no longer owns sandbox compensation", errs.ErrConflict)
	}
	count, err := machine.CurrentVMCount(ctx, tx, candidate.Candidate)
	if err != nil || count != 0 {
		return fmt.Errorf("resume candidate physical responsibility still present: %v", err)
	}
	settings, err := LoadSettings(ctx, tx, destination.ID)
	if err != nil {
		return err
	}
	if settings.Generation != parent.Input.Settings.Generation && settings.Generation != parent.OperationID {
		return fmt.Errorf("%w: resume desired configuration changed", errs.ErrConflict)
	}
	restored := parent.Input.Sandbox
	desired := parent.Input.Settings
	desired.Generation = parent.OperationID
	if err = replaceSandbox(ctx, tx, current, restored); err != nil {
		return err
	}
	if err = replaceSettings(ctx, tx, destination.ID, settings, desired); err != nil {
		return err
	}
	return nil
}
