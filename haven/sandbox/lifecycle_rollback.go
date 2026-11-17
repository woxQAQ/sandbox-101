package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func (l *Lifecycle) AdmitRollback(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if command.Action != "rollback" || command.SandboxID == "" || command.CheckpointID == "" || principal.SandboxID != "" || principal.TenantID == "" || key == "" || len(key) > 256 {
		return empty, fmt.Errorf("%w: rollback requires tenant authority, sandbox, checkpoint and key", errs.ErrInvalid)
	}
	requestGuard := l.guards.TenantRequest(principal.TenantID, key)
	requestGuard.Lock()
	defer requestGuard.Unlock()
	mac := l.commands.Identity.CommandMAC(principal, command)
	if accepted, found, err := l.existing(ctx, principal, key, &ClaimAdmission{
		MAC: mac,
	}); found || err != nil {
		return accepted, err
	}
	guard := l.guards.Configuration(command.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	current, err := l.catalog.OwnedRecord(ctx, principal, command.SandboxID)
	if err != nil {
		return empty, err
	}
	if current.State != "running" && current.State != "paused" {
		return empty, fmt.Errorf("%w: rollback requires running or paused sandbox", errs.ErrConflict)
	}
	checkpoint, err := l.catalog.GetCheckpoint(ctx, principal, command.CheckpointID)
	if err != nil {
		return empty, err
	}
	if checkpoint.SandboxID != current.ID || checkpoint.TenantID != current.TenantID {
		return empty, fmt.Errorf("%w: rollback checkpoint differs from sandbox owner", errs.ErrConflict)
	}
	snapshot, err := l.proofs.Snapshots().LoadRestoreSource(ctx, l.store.Database, current.TenantID, checkpoint.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = machine.VerifyRestoreSourceFiles(snapshot); err != nil {
		return empty, err
	}
	settings, err := LoadSettings(ctx, l.store.Database, current.ID)
	if err != nil {
		return empty, err
	}
	source, err := machine.LoadResourceStep(ctx, l.store.Database, current.SourceOperation)
	if err != nil {
		return empty, err
	}
	if string(operation.Encode(source.Input.Resource.Config)) != string(operation.Encode(snapshot.Snapshot.Manifest.Config)) {
		return empty, fmt.Errorf("%w: rollback shape/image differs from sandbox", errs.ErrConflict)
	}
	if current.State == "running" && l.commands.CredentialsMissing(current.ID, settings) {
		return empty, fmt.Errorf("%w: host credentials must be supplied again before rollback", errs.ErrConflict)
	}
	rawCaptures, err := machine.PendingPublishedCaptures(ctx, l.store.Database, source)
	if err != nil {
		return empty, err
	}
	parent := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:    current.ID,
			VMID:         current.VMID,
			SnapshotID:   checkpoint.SnapshotID,
			CheckpointID: checkpoint.ID,
		},
	}
	input := LifecycleInput{
		RequestTenantID: principal.TenantID,
		OwnerTenantID:   current.TenantID,
		Command:         operation.SanitizedCommand(command),
		CheckpointID:    checkpoint.ID,
		Sandbox:         current,
		Settings:        settings,
		Source: ownership.GenerationRef{
			OperationID: source.OperationID,
			InputHash:   source.Hash,
		},
		SourceInstance: source.State.Witness,
		Rollback: &RollbackInput{
			RawCaptures: rawCaptures,
			Checkpoint:  checkpoint,
			Source:      snapshot,
		},
	}
	if current.State == "paused" {
		return l.admitPausedRollback(ctx, parent, input, mac)
	}
	admission := &LifecycleAdmission{
		History: l.proofs.Snapshots(),
		Parent:  parent,
		Input:   input,
		MAC:     mac,
		Role:    "stop",
	}
	child, err := l.stops.AdmitForParent(ctx, machine.StopRequest{
		TenantID:         current.TenantID,
		Key:              parent.ID + ":stop",
		SandboxID:        current.ID,
		ExpectedInstance: current.Instance,
	}, admission)
	if child.ID == "" {
		return empty, err
	}
	l.registry.StageSandboxSecrets(parent.ID, current.ID)
	if err != nil {
		return parent, err
	}
	l.registry.CloseSandboxConnections(current.ID)
	if err = l.activations.RevokeSandbox(current.ID); err != nil {
		return parent, err
	}
	l.dispatch(parent.ID)
	return parent, nil
}

func (l *Lifecycle) admitPausedRollback(ctx context.Context, parent operation.Record, input LifecycleInput, mac string) (operation.Record, error) {
	var empty operation.Record
	l.admission.Lock()
	defer l.admission.Unlock()
	if err := l.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := l.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	current, err := LoadSandbox(ctx, tx, input.Sandbox.ID, input.OwnerTenantID)
	if err != nil {
		return empty, err
	}
	settings, err := LoadSettings(ctx, tx, current.ID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(current, input.Sandbox) || !reflect.DeepEqual(settings, input.Settings) {
		return empty, fmt.Errorf("%w: paused rollback source changed", errs.ErrConflict)
	}
	if _, err = LifecycleFence(ctx, tx, current.ID); !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			return empty, err
		}
		return empty, fmt.Errorf("%w: paused rollback source is fenced", errs.ErrConflict)
	}
	checkpoint, err := LoadOwnedCheckpoint(ctx, tx, input.CheckpointID, input.OwnerTenantID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(checkpoint, input.Rollback.Checkpoint) {
		return empty, fmt.Errorf("%w: paused rollback checkpoint changed", errs.ErrConflict)
	}
	source, err := l.proofs.Snapshots().LoadRestoreSource(ctx, tx, input.OwnerTenantID, checkpoint.SnapshotID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(source, input.Rollback.Source) {
		return empty, fmt.Errorf("%w: paused rollback publication changed", errs.ErrConflict)
	}
	next := current
	next.SnapshotID = checkpoint.SnapshotID
	desired := settings
	desired.Generation = parent.ID
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Command
	}{input.Command.Action, input.Command})
	payload := operation.Encode(input)
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        parent.ID,
		ScopeKind: "tenant",
		ScopeID:   input.RequestTenantID,
		Key:       parent.Key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "succeeded",
		Body:      operation.Encode(parent.Result),
	}); err != nil {
		return empty, err
	}
	if err = operations.InsertCredential(ctx, parent.ID, mac); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         parent.ID,
		StepKey:             "lifecycle",
		Action:              "rollback",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: current.ID,
		ExpectedInstance:    "",
		Status:              "succeeded",
		Body: operation.Encode(LifecycleState{
			Phase:      "complete",
			SnapshotID: checkpoint.SnapshotID,
		}),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplacePausedBody(ctx, dbgen.ReplacePausedBodyParams{
		ID:           current.ID,
		Tenant:       input.OwnerTenantID,
		PreviousBody: operation.Encode(current),
		NextBody:     operation.Encode(next),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    current.ID,
		PreviousBody: operation.Encode(settings),
		NextBody:     operation.Encode(desired),
	})); err != nil {
		return empty, err
	}
	if current.SnapshotID != checkpoint.SnapshotID {
		responsibilities := quota.New(tx)
		if err = responsibilities.AddResourceReference(ctx, quota.ResourceReference{
			Kind:       "snapshot",
			ID:         checkpoint.SnapshotID,
			HolderKind: "sleep",
			HolderID:   current.ID,
		}); err != nil {
			return empty, err
		}
		if err = responsibilities.AddCostReference(ctx, quota.CostReference{
			TenantID:   input.OwnerTenantID,
			Kind:       "snapshot",
			Key:        checkpoint.SnapshotID,
			HolderKind: "active_use",
			HolderID:   "sleep:" + current.ID,
		}); err != nil {
			return empty, err
		}
		if err = responsibilities.RemoveResourceReference(ctx, quota.ResourceReference{
			Kind:       "snapshot",
			ID:         current.SnapshotID,
			HolderKind: "sleep",
			HolderID:   current.ID,
		}); err != nil {
			return empty, err
		}
		if err = responsibilities.RemoveCostReference(ctx, quota.CostReference{
			TenantID:   input.OwnerTenantID,
			Kind:       "snapshot",
			Key:        current.SnapshotID,
			HolderKind: "active_use",
			HolderID:   "sleep:" + current.ID,
		}); err != nil {
			return empty, err
		}
	}
	if err = l.fault("paused_rollback_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	parent.Status = "succeeded"
	return parent, l.store.Checkpoint()
}

func (l *Lifecycle) runRollback(step LifecycleStep) error {
	stopped, err := l.runResourceLink(step.OperationID, "stop")
	if err != nil {
		return err
	}
	if err = l.cleanupRollbackCaptures(step, stopped.ChildID); err != nil {
		return err
	}
	if _, _, lookup := machine.LoadResourceLink(l.ctx, l.store.Database, step.OperationID, "restore"); errors.Is(lookup, sql.ErrNoRows) {
		link := &LifecycleAdmission{
			History: l.proofs.Snapshots(),
			Parent: operation.Record{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "restore",
		}
		_, err = l.provisioning.AdmitRestoreWithParent(l.ctx, machine.RestoreRequest{
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
	restored, err := l.runResourceLink(step.OperationID, "restore")
	if err != nil {
		return err
	}
	child, err := machine.LoadResourceStep(l.ctx, l.store.Database, restored.ChildID)
	if err != nil {
		return err
	}
	if err = l.restoreActivation(step, child); err != nil {
		return err
	}
	lock := l.guards.VM(child.Candidate)
	lock.Lock()
	defer lock.Unlock()
	return l.activations.Continue(step.OperationID)
}

func (l *Lifecycle) cleanupRollbackCaptures(step LifecycleStep, stop string) error {
	for _, ref := range step.Input.Rollback.RawCaptures {
		role := "raw:" + ref.OperationID
		if _, _, lookup := machine.LoadResourceLink(l.ctx, l.store.Database, step.OperationID, role); errors.Is(lookup, sql.ErrNoRows) {
			link := &LifecycleAdmission{
				History: l.proofs.Snapshots(),
				Parent: operation.Record{
					ID: step.OperationID,
				},
				Input: step.Input,
				Role:  role,
			}
			if _, err := l.captureCleanup.Admit(l.ctx, machine.CaptureCleanupRequest{
				TenantID:           step.Input.OwnerTenantID,
				Key:                step.OperationID + ":" + role,
				CaptureOperationID: ref.OperationID,
				StopOperationID:    stop,
			}, link); err != nil {
				return err
			}
		} else if lookup != nil {
			return lookup
		}
		if _, err := l.runResourceLink(step.OperationID, role); err != nil {
			return err
		}
	}
	return nil
}

func (admission *LifecycleAdmission) admitRollbackLease(ctx context.Context, tx *sql.Tx) error {
	parent, input, proofs := admission.Parent, admission.Input, admission.History
	if input.Command.Action != "rollback" || input.Rollback == nil {
		return fmt.Errorf("%w: rollback lease requires frozen source", errs.ErrInvalid)
	}
	checkpoint, err := LoadOwnedCheckpoint(ctx, tx, input.CheckpointID, input.OwnerTenantID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(checkpoint, input.Rollback.Checkpoint) || checkpoint.SandboxID != input.Sandbox.ID {
		return fmt.Errorf("%w: rollback checkpoint binding changed", errs.ErrConflict)
	}
	source, err := proofs.LoadRestoreSource(ctx, tx, input.OwnerTenantID, checkpoint.SnapshotID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(source, input.Rollback.Source) {
		return fmt.Errorf("%w: rollback source publication changed", errs.ErrConflict)
	}
	if err = checkRollbackCaptures(ctx, tx, input); err != nil {
		return err
	}
	return machine.PinSnapshotSource(ctx, tx, input.OwnerTenantID, machine.SourceLease{
		SnapshotID:  checkpoint.SnapshotID,
		OperationID: parent.ID,
		StepKey:     "lifecycle",
	})
}

func RetireRollbackLease(ctx context.Context, tx *sql.Tx, parent LifecycleStep) error {
	source := parent.Input.Rollback
	if source == nil {
		return fmt.Errorf("%w: rollback source lease proof missing", errs.ErrManualRecovery)
	}
	return machine.ReleaseSnapshotSource(ctx, tx, parent.Input.OwnerTenantID, machine.SourceLease{
		SnapshotID:  source.Checkpoint.SnapshotID,
		OperationID: parent.OperationID,
		StepKey:     "lifecycle",
	})
}
