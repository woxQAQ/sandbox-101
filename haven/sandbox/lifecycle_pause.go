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

func (service *Lifecycle) markPauseCaptured(step *LifecycleStep, capture machine.CaptureStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.lifecycleProof(service.ctx, tx, *step); err != nil {
		return err
	}
	current, err := machine.LoadCapture(service.ctx, tx, capture.OperationID)
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
	if err = responsibilities.AddResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         snapshot.ID,
		HolderKind: "sleep",
		HolderID:   step.Input.Sandbox.ID,
	}); err != nil {
		return err
	}
	if err = responsibilities.AddCostReference(service.ctx, quota.CostReference{
		TenantID:   step.Input.OwnerTenantID,
		Kind:       "snapshot",
		Key:        snapshot.ID,
		HolderKind: "active_use",
		HolderID:   "sleep:" + step.Input.Sandbox.ID,
	}); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
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
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("pause_captured")
}

func (service *Lifecycle) runPause(step LifecycleStep) error {
	link, err := service.runResourceLink(step.OperationID, "capture")
	if err != nil {
		return err
	}
	capture, err := machine.LoadCapture(service.ctx, service.store.Database, link.ChildID)
	if err != nil {
		return err
	}
	if step.State.Phase == "admitted" {
		if err = service.markPauseCaptured(&step, capture); err != nil {
			return err
		}
	}
	if step.State.Phase != "captured" {
		return fmt.Errorf("unsupported pause phase")
	}
	service.registry.CloseSandboxConnections(step.Input.Sandbox.ID)
	if err = service.activations.RevokeSandbox(step.Input.Sandbox.ID); err != nil {
		return err
	}
	if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, "release"); lookup == sql.ErrNoRows {
		admission := &LifecycleAdmission{
			History: service.proofs.Snapshots(),
			Parent: operation.Record{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "release",
		}
		if _, err = service.deletion.AdmitReleaseForParent(service.ctx, machine.ReleaseRequest{
			TenantID:  step.Input.OwnerTenantID,
			Key:       step.OperationID + ":release",
			SandboxID: step.Input.Sandbox.ID,
		}, admission); err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	if _, err = service.runResourceLink(step.OperationID, "release"); err != nil {
		return err
	}
	return service.completePause(step)
}

func (service *Lifecycle) completePause(step LifecycleStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.lifecycleProof(service.ctx, tx, step); err != nil {
		return err
	}
	link, status, err := machine.LoadResourceLink(service.ctx, tx, step.OperationID, "release")
	if err != nil {
		return err
	}
	if status != "succeeded" {
		return errs.ErrInterrupted
	}
	released, err := machine.LoadResourceStep(service.ctx, tx, link.ChildID)
	if err != nil {
		return err
	}
	if released.Status != "succeeded" || released.State.Phase != "complete" || released.Input.SlotDestination == nil || released.Input.SlotDestination.SnapshotID != step.State.SnapshotID {
		return fmt.Errorf("pause source deletion proof missing")
	}

	sandbox, err := LoadSandbox(service.ctx, tx, step.Input.Sandbox.ID, step.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if sandbox.State != "terminated" || sandbox.OperationID != released.OperationID || sandbox.Instance != step.Input.Sandbox.Instance || sandbox.SourceOperation != step.Input.Source.OperationID {
		return fmt.Errorf("pause current source ownership changed")
	}
	instances, err := machine.CurrentVMCount(service.ctx, tx, sandbox.VMID)
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
	settings.PausedUntil = time.Now().UTC().Add(service.opts.PausedTTL)
	next := step.State
	next.Phase = "complete"
	if err = store.RequireOne(dbgen.New(tx).RestorePausedSandbox(service.ctx, dbgen.RestorePausedSandboxParams{
		ID:           sandbox.ID,
		Tenant:       sandbox.TenantID,
		PreviousBody: operation.Encode(oldSandbox),
		NextBody:     operation.Encode(sandbox),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(service.ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    sandbox.ID,
		PreviousBody: operation.Encode(step.Input.Settings),
		NextBody:     operation.Encode(settings),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteFence(service.ctx, dbgen.DeleteFenceParams{
		SandboxID:   sandbox.ID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(service.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      "lifecycle",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = operations.Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("pause_completion_before_commit"); err != nil {
		return err
	}
	if err = service.committer.Commit(tx); err != nil {
		return err
	}
	return service.store.Checkpoint()
}
