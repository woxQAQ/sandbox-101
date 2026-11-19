package sandbox

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func (l *Lifecycle) markPauseCaptured(step *LifecycleStep, capture machine.CaptureStep) error {
	l.admission.Lock()
	defer l.admission.Unlock()
	tx, err := l.store.Database.BeginTx(l.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = l.lifecycleProof(l.ctx, tx, *step); err != nil {
		return err
	}
	current, err := machine.LoadCapture(l.ctx, tx, capture.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, capture) || capture.Status != "succeeded" || capture.State.Phase != "complete" || capture.State.ResumeRequired || capture.Input.Source.State.Witness != step.Input.Sandbox.Instance {
		return fmt.Errorf("pause lacks original published capture")
	}
	snapshot := capture.Input.Snapshot
	next := LifecycleState{
		Phase:      "captured",
		SnapshotID: snapshot.ID,
	}
	responsibilities := quota.New(tx)
	if err = responsibilities.AddResourceReference(l.ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         snapshot.ID,
		HolderKind: "sleep",
		HolderID:   step.Input.Sandbox.ID,
	}); err != nil {
		return err
	}
	if err = responsibilities.AddCostReference(l.ctx, quota.CostReference{
		TenantID:   step.Input.OwnerTenantID,
		Kind:       "snapshot",
		Key:        snapshot.ID,
		HolderKind: "active_use",
		HolderID:   "sleep:" + step.Input.Sandbox.ID,
	}); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(l.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "lifecycle",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = next
	step.Status = "running"
	if err = l.store.Checkpoint(); err != nil {
		return err
	}
	return l.fault("pause_captured")
}

func (l *Lifecycle) runPause(step LifecycleStep) error {
	link, err := l.runResourceLink(step.OperationID, "capture")
	if err != nil {
		return err
	}
	capture, err := machine.LoadCapture(l.ctx, l.store.Database, link.ChildID)
	if err != nil {
		return err
	}
	if step.State.Phase == "admitted" {
		if err = l.markPauseCaptured(&step, capture); err != nil {
			return err
		}
	}
	if step.State.Phase != "captured" {
		return fmt.Errorf("unsupported pause phase")
	}
	l.registry.CloseSandboxConnections(step.Input.Sandbox.ID)
	if err = l.activations.RevokeSandbox(step.Input.Sandbox.ID); err != nil {
		return err
	}
	if _, _, lookup := machine.LoadResourceLink(l.ctx, l.store.Database, step.OperationID, "release"); lookup == sql.ErrNoRows {
		admission := &LifecycleAdmission{
			History: l.proofs.Snapshots(),
			Parent: operation.Record{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "release",
		}
		if _, err = l.deletion.AdmitReleaseForParent(l.ctx, machine.ReleaseRequest{
			TenantID:  step.Input.OwnerTenantID,
			Key:       step.OperationID + ":release",
			SandboxID: step.Input.Sandbox.ID,
		}, admission); err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	if _, err = l.runResourceLink(step.OperationID, "release"); err != nil {
		return err
	}
	return l.completePause(step)
}

func (l *Lifecycle) completePause(step LifecycleStep) error {
	l.admission.Lock()
	defer l.admission.Unlock()
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
	released, err := machine.LoadResourceStep(l.ctx, tx, link.ChildID)
	if err != nil {
		return err
	}
	if released.Status != "succeeded" || released.State.Phase != "complete" || released.Input.SlotDestination == nil || released.Input.SlotDestination.SnapshotID != step.State.SnapshotID {
		return fmt.Errorf("pause source deletion proof missing")
	}

	sandbox, err := LoadSandbox(l.ctx, tx, step.Input.Sandbox.ID, step.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if sandbox.State != "terminated" || sandbox.OperationID != released.OperationID || sandbox.Instance != step.Input.Sandbox.Instance || sandbox.SourceOperation != step.Input.Source.OperationID {
		return fmt.Errorf("pause current source ownership changed")
	}
	instances, err := machine.CurrentVMCount(l.ctx, tx, sandbox.VMID)
	if err != nil || instances != 0 {
		return fmt.Errorf("pause source resource still present: %v", err)
	}
	oldSandbox := sandbox
	sandbox.State = "paused"
	sandbox.SnapshotID = step.State.SnapshotID
	sandbox.VMID = ""
	sandbox.Instance = ""
	sandbox.OperationID = ""
	settings := step.Input.Settings
	settings.Generation = step.OperationID
	settings.Instance = ""
	settings.ActiveEpoch = ""
	settings.Deadline = time.Time{}
	settings.PausedUntil = time.Now().UTC().Add(l.opts.PausedTTL)
	next := step.State
	next.Phase = "complete"
	if err = store.RequireOne(dbgen.New(tx).RestorePausedSandbox(l.ctx, dbgen.RestorePausedSandboxParams{
		ID:           sandbox.ID,
		Tenant:       sandbox.TenantID,
		PreviousBody: operation.Encode(oldSandbox),
		NextBody:     operation.Encode(sandbox),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(l.ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    sandbox.ID,
		PreviousBody: operation.Encode(step.Input.Settings),
		NextBody:     operation.Encode(settings),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteFence(l.ctx, dbgen.DeleteFenceParams{
		SandboxID:   sandbox.ID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(l.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      "lifecycle",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = l.fault("pause_completion_before_commit"); err != nil {
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
