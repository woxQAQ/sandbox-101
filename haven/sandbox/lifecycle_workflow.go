package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// LifecycleRequest binds the authenticated scope, stable key and command before
// selecting any parent transaction participant.
type LifecycleRequest struct {
	Principal authorization.Principal
	Key       string
	Command   operation.Intent
}

func (l *Lifecycle) AdmitLifecycle(ctx context.Context, principal authorization.Principal, key string, command operation.Intent) (operation.State, error) {
	return l.AdmitForParent(ctx, LifecycleRequest{
		Principal: principal,
		Key:       key,
		Command:   command,
	}, nil)
}

func (l *Lifecycle) AdmitForParent(ctx context.Context, request LifecycleRequest, upper machine.ResourceParent) (operation.State, error) {
	principal, key, command := request.Principal, request.Key, request.Command
	var empty operation.State
	if key == "" || len(key) > 256 || command.SandboxID == "" || principal.TenantID == "" || (principal.SandboxID != "" && (principal.SandboxID != command.SandboxID || command.Action != "release")) {
		return empty, fmt.Errorf("%w: lifecycle request requires scoped sandbox and key", errs.ErrInvalid)
	}
	if command.Action != "release" && command.Action != "checkpoint" && command.Action != "pause" {
		return empty, fmt.Errorf("%w: unsupported lifecycle action", errs.ErrInvalid)
	}
	releaseKey := l.tenantIdempotencyLocks.Lock(principal.TenantID, key)
	defer releaseKey()
	mac := l.identity.CommandMAC(principal, command)
	if operation, found, err := l.existing(ctx, principal, key, &ClaimAdmission{
		MAC: mac,
	}); found || err != nil {
		return operation, err
	}
	unlock := l.sandboxConfigLocks.Lock(command.SandboxID)
	defer unlock()

	sandboxRecord, err := l.catalog.OwnedRecord(ctx, principal, command.SandboxID)
	if err != nil {
		return empty, err
	}
	settings, err := LoadSettings(ctx, l.store.Database, sandboxRecord.ID)
	if err != nil {
		return empty, err
	}
	if !command.LeaseDeadline.IsZero() {
		deadline := settings.Deadline
		if sandboxRecord.State == "paused" {
			deadline = settings.PausedUntil
		}
		if !deadline.Equal(command.LeaseDeadline) || deadline.IsZero() || time.Now().Before(deadline) {
			return empty, fmt.Errorf("%w: expiration lease was renewed or is not due", errs.ErrConflict)
		}
	}
	allowed := sandboxRecord.State == "running" || (sandboxRecord.State == "paused" && command.Action == "checkpoint") || (command.Action == "release" && (sandboxRecord.State == "provisioning" || sandboxRecord.State == "paused"))
	if !allowed {
		return empty, fmt.Errorf("%w: sandbox state cannot enter requested lifecycle", errs.ErrConflict)
	}
	if sandboxRecord.SourceOperation == "" {
		return empty, fmt.Errorf("%w: fork placement has not committed its allocation", errs.ErrConflict)
	}
	source, err := machine.LoadResourceStep(ctx, l.store.Database, sandboxRecord.SourceOperation)
	if err != nil {
		return empty, err
	}
	if source.Status == "failed" && source.Input.FailureSlotDestination != nil {
		return empty, fmt.Errorf("%w: resume compensation must finish before release", errs.ErrConflict)
	}
	var priorFence string
	if command.Action == "release" {
		if priorFence, err = LifecycleFence(ctx, l.store.Database, sandboxRecord.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return empty, err
		}
	}
	input := LifecycleInput{
		PriorFence:      priorFence,
		SourceInstance:  source.State.Witness,
		RequestTenantID: principal.TenantID,
		OwnerTenantID:   sandboxRecord.TenantID,
		Command:         operation.SanitizedCommand(command),
		Sandbox:         sandboxRecord,
		Settings:        settings,
		Source: ownership.GenerationRef{
			OperationID: source.OperationID,
			InputHash:   source.Hash,
		},
	}
	if command.Action == "release" && sandboxRecord.State != "paused" && source.Status == "succeeded" {
		refs, err := machine.FailedCaptureReferences(ctx, l.store.Database, source)
		if err != nil {
			return empty, err
		}
		input.CaptureFailures = refs
	}
	if command.Action == "checkpoint" {
		input.CheckpointID = operation.NewID("cp_")
	}
	parent := operation.State{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:    sandboxRecord.ID,
			VMID:         sandboxRecord.VMID,
			CheckpointID: input.CheckpointID,
		},
	}
	if command.Action == "checkpoint" && sandboxRecord.State == "paused" {
		return l.admitPausedCheckpoint(ctx, parent, input, mac)
	}
	admission := &LifecycleAdmission{
		History: l.proofs.Snapshots(),
		Upper:   upper,
		Parent:  parent,
		Input:   input,
		MAC:     mac,
		Role:    "capture",
	}
	if command.Action == "release" && sandboxRecord.State == "paused" {
		return l.admitPausedRelease(ctx, admission)
	}
	var child operation.State
	if command.Action == "release" {
		admission.Role = "release"
		if len(input.CaptureFailures) > 0 {
			admission.Role = "stop"
			stops := l.stops
			child, err = stops.AdmitForParent(ctx, machine.StopRequest{
				TenantID:         sandboxRecord.TenantID,
				Key:              parent.ID + ":stop",
				SandboxID:        sandboxRecord.ID,
				ExpectedInstance: source.State.Witness,
			}, admission)
		} else if source.Status == "failed" {
			child, err = l.deletion.AdmitDiscardForParent(ctx, machine.DiscardRequest{
				TenantID:          sandboxRecord.TenantID,
				Key:               parent.ID + ":release",
				SandboxID:         sandboxRecord.ID,
				FailedOperationID: source.OperationID,
				ExpectedInstance:  source.Input.Resource.Instance,
			}, admission)
		} else {
			child, err = l.deletion.AdmitReleaseForParent(ctx, machine.ReleaseRequest{
				TenantID:  sandboxRecord.TenantID,
				Key:       parent.ID + ":release",
				SandboxID: sandboxRecord.ID,
			}, admission)
		}
	} else {
		captures := l.captures
		child, err = captures.AdmitForParent(ctx, machine.CaptureRequest{
			TenantID:         sandboxRecord.TenantID,
			Key:              parent.ID + ":capture",
			SandboxID:        sandboxRecord.ID,
			ExpectedInstance: sandboxRecord.Instance,
		}, admission)
	}
	if child.ID == "" {
		return empty, err
	}
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		return parent, err
	}
	parent.Result.SnapshotID = child.Result.SnapshotID
	if command.Action == "release" {
		l.registry.CloseSandboxConnections(sandboxRecord.ID)
		if revokeErr := l.activations.RevokeSandbox(sandboxRecord.ID); revokeErr != nil {
			return parent, revokeErr
		}
	}
	if err != nil {
		return parent, err
	}
	l.dispatch(parent.ID)
	return parent, nil
}

func (l *Lifecycle) lifecycleProof(ctx context.Context, tx *sql.Tx, step LifecycleStep) error {
	current, err := LoadLifecycleStep(ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("lifecycle current intent changed")
	}
	fence, err := LifecycleFence(ctx, tx, step.Input.Sandbox.ID)
	if err != nil {
		return err
	}
	if fence != step.OperationID {
		return fmt.Errorf("%w: lifecycle was superseded", errs.ErrConflict)
	}
	settings, err := LoadSettings(ctx, tx, step.Input.Sandbox.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(settings, step.Input.Settings) {
		return fmt.Errorf("%w: lifecycle source settings changed", errs.ErrConflict)
	}
	source, err := machine.LoadResourceStep(ctx, tx, step.Input.Source.OperationID)
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
	identity, err := operation.New(tx).RequestIdentity(ctx, step.OperationID)
	if err != nil {
		return err
	}
	var request struct {
		Action  string
		Request operation.Intent
	}
	if err = json.Unmarshal(identity.Request, &request); err != nil {
		return err
	}
	if identity.ScopeKind != "tenant" || identity.ScopeID != step.Input.RequestTenantID || operation.Terminal(identity.Status) || operation.Digest(identity.Request) != identity.Hash || request.Action != step.Input.Command.Action || !reflect.DeepEqual(request.Request, step.Input.Command) {
		return fmt.Errorf("lifecycle parent identity changed")
	}
	return nil
}

func (l *Lifecycle) completeCheckpoint(step LifecycleStep, capture machine.CaptureStep) error {
	l.storeWriteMu.Lock()
	defer l.storeWriteMu.Unlock()
	tx, err := l.store.Database.BeginTx(l.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = l.lifecycleProof(l.ctx, tx, step); err != nil {
		return err
	}
	current, err := machine.LoadCapture(l.ctx, tx, capture.OperationID)
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

	if err = bindCheckpoint(l.ctx, tx, cp, step.OperationID); err != nil {
		return err
	}
	if err = quota.AddResourceReference(l.ctx, tx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         cp.SnapshotID,
		HolderKind: "checkpoint_binding",
		HolderID:   cp.ID,
	}); err != nil {
		return err
	}
	if err = quota.AddCostReference(l.ctx, tx, quota.CostReference{
		TenantID:   cp.TenantID,
		Kind:       "snapshot",
		Key:        cp.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "checkpoint-binding:" + cp.ID,
	}); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteFence(l.ctx, dbgen.DeleteFenceParams{
		SandboxID:   cp.SandboxID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteLiveStep(l.ctx, dbgen.CompleteLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "lifecycle",
		InputHash:   step.Hash,
		Body: operation.Encode(LifecycleState{
			Phase:      "complete",
			SnapshotID: cp.SnapshotID,
		}),
	})); err != nil {
		return err
	}
	if err = l.fault("checkpoint_completion_before_commit"); err != nil {
		return err
	}
	if err = l.committer.Complete(tx, operation.Completion{
		ID:          step.OperationID,
		AdoptResult: true,
	}); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return l.store.Checkpoint()
}

func (l *Lifecycle) completeReleaseLifecycle(step LifecycleStep) error {
	l.storeWriteMu.Lock()
	defer l.storeWriteMu.Unlock()
	tx, err := l.store.Database.BeginTx(l.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = l.lifecycleProof(l.ctx, tx, step); err != nil {
		return err
	}
	link, status, err := machine.LoadResourceLink(l.ctx, tx, step.OperationID, "release")
	if err != nil {
		return err
	}
	if status != "succeeded" {
		return errs.ErrInterrupted
	}
	switch link.Kind {
	case "release":
		released, err := machine.LoadResourceStep(l.ctx, tx, link.ChildID)
		if err != nil {
			return err
		}
		if released.Status != "succeeded" || released.State.Phase != "complete" || released.ExpectedInstance != step.Input.Sandbox.Instance || released.Input.SourceOperation != step.Input.Source.OperationID {
			return fmt.Errorf("lifecycle release lacks exact deletion proof")
		}
	case "discard":
		discarded, err := machine.LoadDiscard(l.ctx, tx, link.ChildID)
		if err != nil {
			return err
		}
		if discarded.Status != "succeeded" || discarded.State.Phase != "complete" || discarded.Input.Source.OperationID != step.Input.Source.OperationID || discarded.Input.Request.SandboxID != step.Input.Sandbox.ID {
			return fmt.Errorf("lifecycle release lacks exact failed-candidate reclamation proof")
		}
	default:
		return fmt.Errorf("unsupported release child kind")
	}
	superseded, err := l.retireSupersededLifecycle(tx, step)
	if err != nil {
		return err
	}
	tombstone, err := LoadSandbox(l.ctx, tx, step.Input.Sandbox.ID, step.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if tombstone.State != "terminated" || tombstone.OperationID != link.ChildID || tombstone.SourceOperation != step.Input.Source.OperationID || tombstone.VMID != step.Input.Sandbox.VMID || tombstone.Instance != step.Input.Sandbox.Instance {
		return fmt.Errorf("%w: release business ownership changed after reclamation", errs.ErrManualRecovery)
	}
	retired := tombstone
	retired.VMID, retired.Instance, retired.OperationID = "", "", ""
	if err = store.RequireOne(dbgen.New(tx).RetireTerminatedBody(l.ctx, dbgen.RetireTerminatedBodyParams{
		ID:           retired.ID,
		Tenant:       retired.TenantID,
		PreviousBody: operation.Encode(tombstone),
		NextBody:     operation.Encode(retired),
	})); err != nil {
		return err
	}
	desired := step.Input.Settings
	desired.Generation = step.OperationID
	desired.ActiveEpoch = ""
	desired.Instance = ""
	desired.Deadline = time.Time{}
	desired.PausedUntil = time.Time{}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(l.ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    step.Input.Sandbox.ID,
		PreviousBody: operation.Encode(step.Input.Settings),
		NextBody:     operation.Encode(desired),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteFence(l.ctx, dbgen.DeleteFenceParams{
		SandboxID:   step.Input.Sandbox.ID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteLiveStep(l.ctx, dbgen.CompleteLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "lifecycle",
		InputHash:   step.Hash,
		Body: operation.Encode(LifecycleState{
			Phase: "complete",
		}),
	})); err != nil {
		return err
	}
	if err = l.committer.Complete(tx, append(superseded, operation.Completion{
		ID:          step.OperationID,
		AdoptResult: true,
	})...); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	l.registry.RetireCredentials(step.Input.Sandbox.ID, step.OperationID)
	return l.store.Checkpoint()
}

func (l *Lifecycle) runLifecycle(step LifecycleStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, errs.ErrInterrupted) && l.ctx.Err() == nil {
			runErr = errors.Join(runErr, l.failLifecycle(step))
		}
	}()
	if operation.Terminal(step.Status) {
		return nil
	}
	switch step.Input.Command.Action {
	case "release":
		l.registry.CloseSandboxConnections(step.Input.Sandbox.ID)
		if err := l.activations.RevokeSandbox(step.Input.Sandbox.ID); err != nil {
			return err
		}
		if len(step.Input.CaptureFailures) > 0 {
			if err := l.runReleaseCaptures(step); err != nil {
				return err
			}
		}
		if _, err := l.runResourceLink(step.OperationID, "release"); err != nil {
			return err
		}
		return l.completeReleaseLifecycle(step)
	case "rollback":
		return l.runRollback(step)
	case "resume":
		return l.runResume(step)
	case "pause":
		return l.runPause(step)
	case "checkpoint":
		link, err := l.runResourceLink(step.OperationID, "capture")
		if err != nil {
			return err
		}
		capture, err := machine.LoadCapture(l.ctx, l.store.Database, link.ChildID)
		if err != nil {
			return err
		}
		return l.completeCheckpoint(step, capture)
	default:
		return fmt.Errorf("%w: unsupported lifecycle workflow", errs.ErrInvalid)
	}
}

func checkReleaseCaptures(ctx context.Context, tx *sql.Tx, input LifecycleInput) error {
	source, err := machine.LoadResourceStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return err
	}
	current, err := machine.FailedCaptureReferences(ctx, tx, source)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, input.CaptureFailures) {
		return fmt.Errorf("release failed capture responsibilities changed")
	}
	return nil
}

func checkRollbackCaptures(ctx context.Context, tx *sql.Tx, input LifecycleInput) error {
	source, err := machine.LoadResourceStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return err
	}
	refs, err := machine.PendingPublishedCaptures(ctx, tx, source)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(refs, input.Rollback.RawCaptures) {
		return fmt.Errorf("rollback original capture responsibilities changed")
	}
	for _, ref := range refs {
		capture, err := machine.LoadCapture(ctx, tx, ref.OperationID)
		if err != nil {
			return err
		}
		if err = machine.CaptureCleanupProof(ctx, tx, capture, machine.CaptureCleanupTarget{
			VMID:     source.Candidate,
			Instance: input.SourceInstance,
			Status:   "pending",
		}); err != nil {
			return err
		}
	}
	return nil
}

// CompleteLifecycleActivation transfers active sandbox ownership in the same
// transaction that publishes running, after the resource child has committed.
func CompleteLifecycleActivation(ctx context.Context, tx *sql.Tx, activation ActivationStep) error {
	lifecycle, err := LoadLifecycleStep(ctx, tx, activation.OperationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	action := lifecycle.Input.Command.Action
	if action != "resume" && action != "rollback" {
		return nil
	}
	if operation.Terminal(lifecycle.Status) || lifecycle.Input.Sandbox.ID != activation.Input.SandboxID || lifecycle.Input.OwnerTenantID != activation.Input.TenantID {
		return fmt.Errorf("%w: activation no longer owns lifecycle handoff", errs.ErrConflict)
	}
	role := "clone"
	if action == "rollback" {
		role = "restore"
	}
	link, status, err := machine.LoadResourceLink(ctx, tx, lifecycle.OperationID, role)
	if err != nil {
		return err
	}
	if status != "succeeded" || link.ChildID != activation.Input.ResourceOperation {
		return fmt.Errorf("%w: activation clone link changed", errs.ErrConflict)
	}
	snapshot := lifecycle.Input.Sandbox.SnapshotID
	switch action {
	case "resume":
		if snapshot == "" {
			return fmt.Errorf("%w: resume sleep snapshot missing", errs.ErrManualRecovery)
		}
		if err = quota.RemoveResourceReference(ctx, tx, quota.ResourceReference{
			Kind:       "snapshot",
			ID:         snapshot,
			HolderKind: "sleep",
			HolderID:   activation.Input.SandboxID,
		}); err != nil {
			return err
		}
		if err = quota.RemoveCostReference(ctx, tx, quota.CostReference{
			TenantID:   activation.Input.TenantID,
			Kind:       "snapshot",
			Key:        snapshot,
			HolderKind: "active_use",
			HolderID:   "sleep:" + activation.Input.SandboxID,
		}); err != nil {
			return err
		}
	case "rollback":
		if err = RetireRollbackLease(ctx, tx, lifecycle); err != nil {
			return err
		}
		snapshot = lifecycle.Input.Rollback.Checkpoint.SnapshotID
	}
	return store.RequireOne(dbgen.New(tx).CompleteLiveStep(ctx, dbgen.CompleteLiveStepParams{
		OperationID: lifecycle.OperationID,
		StepKey:     "lifecycle",
		InputHash:   lifecycle.Hash,
		Body: operation.Encode(LifecycleState{
			Phase:      "complete",
			SnapshotID: snapshot,
		}),
	}))
}

// failLifecycle commits failure only from a durable child terminal result.
// An effect error with a pending child retains an unresolved accepted intent.
func (l *Lifecycle) failLifecycle(step LifecycleStep) error {
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
	if current.Hash != step.Hash {
		return fmt.Errorf("%w: lifecycle failure input changed", errs.ErrManualRecovery)
	}
	operations := operation.New(tx)
	failed, err := dbgen.New(tx).CountFailedLifecycleChildren(l.ctx, step.OperationID)
	if err != nil {
		return err
	}
	if failed == 0 {
		return nil
	}
	fence, err := LifecycleFence(l.ctx, tx, step.Input.Sandbox.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}
	if fence != step.OperationID {
		return nil
	}
	status, body, err := operations.ResultState(l.ctx, step.OperationID)
	if err != nil {
		return err
	}
	if status == "succeeded" {
		return fmt.Errorf("%w: successful lifecycle cannot adopt failed child", errs.ErrManualRecovery)
	}
	next := current.State
	next.Phase = "failed"
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(l.ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "lifecycle",
		InputHash:   step.Hash,
		Body:        operation.Encode(next),
	})); err != nil {
		return err
	}
	var completed []operation.Completion
	if !operation.Terminal(status) {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "sandbox workflow failed; resource responsibility retained"
		completed = append(completed, operation.Completion{
			ID:     step.OperationID,
			Result: result,
			Failed: true,
		})
	}
	if err = l.committer.Complete(tx, completed...); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return l.store.Checkpoint()
}
