package haven

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) resumeDestination(tx *sql.Tx, destination *quotaDestination) (lifecycleStep, error) {
	var empty lifecycleStep
	if destination == nil || destination.Kind != "sleep" {
		return empty, loom.Fail(loom.ManualRecovery, "resume slot destination missing")
	}
	parent, err := loadLifecycle(service.ctx, tx, destination.OperationID)
	if err != nil {
		return parent, err
	}
	if parent.Input.Command.Action != "resume" || parent.Status == "succeeded" || parent.Input.Sandbox.State != "paused" || parent.Input.Sandbox.ID != destination.ID || parent.Input.Sandbox.SnapshotID != destination.SnapshotID {
		return parent, loom.Fail(loom.ManualRecovery, "resume slot destination differs from original paused sandbox")
	}
	var fence string
	if err = tx.QueryRowContext(service.ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", destination.ID).Scan(&fence); err != nil {
		return parent, err
	}
	if fence != parent.OperationID {
		return parent, loom.Fail(loom.Conflict, "resume compensation was superseded")
	}
	var refs int
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='sleep' AND holder_id=?", destination.SnapshotID, destination.ID).Scan(&refs); err != nil || refs != 1 {
		return parent, fmt.Errorf("resume original sleep holder missing: %v", err)
	}
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", parent.Input.OwnerTenantID, destination.SnapshotID, "sleep:"+destination.ID).Scan(&refs); err != nil || refs != 1 {
		return parent, fmt.Errorf("resume original sleep fee missing: %v", err)
	}
	return parent, nil
}

func (service *Service) restoreResumeSlot(tx *sql.Tx, destination *quotaDestination, cleanup string) error {
	parent, err := service.resumeDestination(tx, destination)
	if err != nil {
		return err
	}
	return requireWrite(tx.ExecContext(service.ctx, "UPDATE quota_refs SET holder_kind='active_use',holder_id=? WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND holder_kind='cleanup' AND holder_id=?", "sleep:"+destination.ID, parent.Input.OwnerTenantID, destination.ID, cleanup))
}

func (service *Service) restorePausedSandbox(tx *sql.Tx, destination *quotaDestination) error {
	parent, err := service.resumeDestination(tx, destination)
	if err != nil {
		return err
	}
	link, _, err := loadResourceLink(service.ctx, tx, parent.OperationID, "clone")
	if err != nil {
		return err
	}
	candidate, err := loadStep(service.ctx, tx, link.ChildID)
	if err != nil {
		return err
	}
	current, err := loadSandbox(service.ctx, tx, destination.ID, parent.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if current.State != "terminated" || current.SourceOperation != candidate.OperationID || current.VMID != candidate.Candidate {
		return loom.Fail(loom.Conflict, "resume candidate no longer owns sandbox compensation")
	}
	var count int
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM vms WHERE id=?", candidate.Candidate).Scan(&count); err != nil || count != 0 {
		return fmt.Errorf("resume candidate physical responsibility still present: %v", err)
	}
	settings, err := loadSettings(service.ctx, tx, destination.ID)
	if err != nil {
		return err
	}
	if settings.Generation != parent.Input.Settings.Generation && settings.Generation != parent.OperationID {
		return loom.Fail(loom.Conflict, "resume desired configuration changed")
	}
	restored := parent.Input.Sandbox
	desired := parent.Input.Settings
	desired.Generation = parent.OperationID
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE sandboxes SET state='paused',body=? WHERE id=? AND tenant=? AND state='terminated' AND body=?", encode(restored), destination.ID, parent.Input.OwnerTenantID, encode(current))); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", encode(desired), destination.ID, encode(settings))); err != nil {
		return err
	}
	return nil
}

func (service *Service) finishResumeFailure(step lifecycleStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadLifecycle(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.Input, step.Input) {
		return fmt.Errorf("resume failure intent changed")
	}
	sandbox, err := loadSandbox(service.ctx, tx, step.Input.Sandbox.ID, step.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if sandbox.State != "paused" || sandbox.SnapshotID != step.Input.Sandbox.SnapshotID || sandbox.VMID != "" {
		return loom.Fail(loom.ManualRecovery, "failed resume has no trusted paused destination")
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?", sandbox.ID, step.OperationID)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key='lifecycle' AND input_hash=? AND status IN ('pending','running')", encode(lifecycleState{
		Phase:      "failed",
		SnapshotID: sandbox.SnapshotID,
	}), step.OperationID, step.Hash)); err != nil {
		return err
	}
	var status string
	var body []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT status,body FROM operations WHERE id=?", step.OperationID).Scan(&status, &body); err != nil {
		return err
	}
	if !terminal(status) {
		var result operationResult
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "resume failed; sleep snapshot retained"
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", encode(result), step.OperationID)); err != nil {
			return err
		}
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) compensateResumeCandidate(step lifecycleStep, candidate resourceStep) error {
	role, kind := "discard", "discard"
	if candidate.Status == "succeeded" {
		role = "compensate"
		kind = "release"
	}
	_, _, err := loadResourceLink(service.ctx, service.store.db, step.OperationID, role)
	if errors.Is(err, sql.ErrNoRows) {
		admission := &lifecycleAdmission{
			Parent: Operation{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  role,
		}
		if kind == "discard" {
			_, err = service.admitDiscard(service.ctx, DiscardRequest{
				TenantID:          step.Input.OwnerTenantID,
				Key:               step.OperationID + ":discard",
				SandboxID:         step.Input.Sandbox.ID,
				FailedOperationID: candidate.OperationID,
				ExpectedInstance:  candidate.Input.Resource.Instance,
			}, admission)
		} else {
			_, err = service.admitRelease(service.ctx, ReleaseRequest{
				TenantID:  step.Input.OwnerTenantID,
				Key:       step.OperationID + ":compensate",
				SandboxID: step.Input.Sandbox.ID,
			}, admission)
		}
	}
	if err != nil {
		return err
	}
	if _, err = service.runResourceLink(step.OperationID, role); err != nil {
		return err
	}
	return service.finishResumeFailure(step)
}
