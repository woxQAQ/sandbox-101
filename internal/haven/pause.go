package haven

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

func (service *Service) markPauseCaptured(step *lifecycleStep, capture captureStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err = service.lifecycleProof(service.ctx, tx, *step); err != nil {
		return err
	}
	current, err := loadCapture(service.ctx, tx, capture.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, capture) || capture.Status != "succeeded" || capture.State.Phase != "complete" || capture.State.ResumeRequired || capture.Input.Source.State.Witness != step.Input.Sandbox.Instance {
		return fmt.Errorf("pause lacks original published capture")
	}
	snapshot := capture.Input.Snapshot
	next := lifecycleState{Phase: "captured", SnapshotID: snapshot.ID}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO resource_refs VALUES('snapshot',?,'sleep',?)", []any{snapshot.ID, step.Input.Sandbox.ID}},
		{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'active_use',?)", []any{step.Input.OwnerTenantID, snapshot.ID, "sleep:" + step.Input.Sandbox.ID}},
		{"UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='lifecycle' AND input_hash=? AND body=? AND status IN ('pending','running')", []any{encode(next), step.OperationID, step.Hash, encode(step.State)}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = next
	step.Status = "running"
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("pause_captured")
}

func (service *Service) releaseSlotDestination(tx *sql.Tx, resource resourceStep) error {
	destination := resource.Input.SlotDestination
	if destination == nil {
		return nil
	}
	if destination.Kind == "claim" {
		return service.warmSlotDestination(tx, resource)
	}
	parent, err := loadLifecycle(service.ctx, tx, destination.OperationID)
	if err != nil {
		return err
	}
	if terminal(parent.Status) || destination.Kind != "sleep" || destination.ID != resource.Input.SandboxID || parent.Input.Sandbox.ID != destination.ID || parent.Input.OwnerTenantID != resource.Input.TenantID {
		return fmt.Errorf("release slot handoff owner changed")
	}
	role := "release"
	switch parent.Input.Command.Action {
	case "pause":
		if parent.State.Phase != "captured" || destination.SnapshotID != parent.State.SnapshotID || parent.Input.Source.OperationID != resource.Input.SourceOperation || parent.Input.Sandbox.Instance != resource.ExpectedInstance {
			return fmt.Errorf("pause release handoff source changed")
		}
	case "resume":
		role = "compensate"
		if _, err = service.resumeDestination(tx, destination); err != nil {
			return err
		}
		activation, err := loadActivation(service.ctx, tx, parent.OperationID)
		if err != nil {
			return err
		}
		if activation.Status != "failed" || activation.Input.ResourceOperation != resource.Input.SourceOperation || activation.Input.ExpectedInstance != resource.ExpectedInstance {
			return fmt.Errorf("resume release handoff lacks failed activation proof")
		}
	default:
		return fmt.Errorf("unsupported release slot destination")
	}
	link, _, err := loadResourceLink(service.ctx, tx, parent.OperationID, role)
	if err != nil {
		return err
	}
	if link.ChildID != resource.OperationID {
		return fmt.Errorf("release slot handoff child changed")
	}
	var count int
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='sleep' AND holder_id=?", destination.SnapshotID, destination.ID).Scan(&count); err != nil || count != 1 {
		return fmt.Errorf("pause sleep holder missing: %v", err)
	}
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", resource.Input.TenantID, destination.SnapshotID, "sleep:"+destination.ID).Scan(&count); err != nil || count != 1 {
		return fmt.Errorf("pause sleep fee holder missing: %v", err)
	}
	return nil
}

func (service *Service) runPause(step lifecycleStep) error {
	link, err := service.runResourceLink(step.OperationID, "capture")
	if err != nil {
		return err
	}
	capture, err := loadCapture(service.ctx, service.store.db, link.ChildID)
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
	service.closeConnections(step.Input.Sandbox.ID)
	if err = service.revokeSandbox(step.Input.Sandbox.ID); err != nil {
		return err
	}
	if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, "release"); lookup == sql.ErrNoRows {
		admission := &lifecycleAdmission{Parent: Operation{ID: step.OperationID}, Input: step.Input, Role: "release"}
		if _, err = service.admitRelease(service.ctx, ReleaseRequest{TenantID: step.Input.OwnerTenantID, Key: step.OperationID + ":release", SandboxID: step.Input.Sandbox.ID}, admission); err != nil {
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

func (service *Service) completePause(step lifecycleStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
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
	released, err := loadStep(service.ctx, tx, link.ChildID)
	if err != nil {
		return err
	}
	if released.Status != "succeeded" || released.State.Phase != "complete" || released.Input.SlotDestination == nil || released.Input.SlotDestination.SnapshotID != step.State.SnapshotID {
		return fmt.Errorf("pause source deletion proof missing")
	}
	sandbox, err := loadSandbox(service.ctx, tx, step.Input.Sandbox.ID, step.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if sandbox.State != "terminated" || sandbox.OperationID != released.OperationID || sandbox.Instance != step.Input.Sandbox.Instance || sandbox.SourceOperation != step.Input.Source.OperationID {
		return fmt.Errorf("pause current source ownership changed")
	}
	var instances int
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM vms WHERE id=?", sandbox.VMID).Scan(&instances); err != nil || instances != 0 {
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
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"UPDATE sandboxes SET state='paused',body=? WHERE id=? AND tenant=? AND state='terminated' AND body=?", []any{encode(sandbox), sandbox.ID, sandbox.TenantID, encode(oldSandbox)}},
		{"UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", []any{encode(settings), sandbox.ID, encode(step.Input.Settings)}},
		{"DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?", []any{sandbox.ID, step.OperationID}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='lifecycle' AND input_hash=? AND body=? AND status='running'", []any{encode(next), step.OperationID, step.Hash, encode(step.State)}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{step.OperationID}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("pause_completion_before_commit"); err != nil {
		return err
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	return service.store.checkpoint()
}
