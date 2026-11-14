package haven

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) runForkGroup(step forkStep) error {
	for {
		switch step.State.Phase {
		case "admitted":
			link, err := service.runResourceLink(step.OperationID, "capture")
			if err != nil {
				if errors.Is(err, ErrInterrupted) || service.ctx.Err() != nil {
					return err
				}
				child, lookup := service.GetOperation(service.ctx, step.Input.OwnerTenantID, link.ChildID)
				if lookup != nil || !terminal(child.Status) {
					return err
				}
				if err = service.compensateFork(&step, "fork source capture failed"); err != nil {
					return err
				}
				continue
			}
			if err = service.captureForkSource(&step, link); err != nil {
				return err
			}
		case "captured":
			if err := service.allocateFork(&step); err != nil {
				if errors.Is(err, ErrInterrupted) || service.ctx.Err() != nil {
					return err
				}
				if err = service.compensateFork(&step, "fork batch allocation could not be admitted"); err != nil {
					return err
				}
			}
		case "allocated":
			service.forkSecrets(step)
			for _, id := range step.State.Children {
				service.dispatch(id)
			}
			failed := false
			for index, id := range step.State.Children {
				child, err := service.waitOperation(service.ctx, "tenant", step.Input.OwnerTenantID, id, "")
				if err != nil && !terminal(child.Status) {
					return err
				}
				link, _, err := loadResourceLink(service.ctx, service.store.db, step.OperationID, fmt.Sprintf("branch:%d", index))
				if err != nil {
					return err
				}
				if link.ChildID != id {
					return loom.Fail(loom.ManualRecovery, "fork branch dependency changed")
				}
				if err = service.completeResourceLink(step.OperationID, fmt.Sprintf("branch:%d", index), link); err != nil {
					return err
				}
				if child.Status != "succeeded" {
					failed = true
				}
			}
			if failed {
				if err := service.compensateFork(&step, "fork branch activation failed"); err != nil {
					return err
				}
				continue
			}
			return service.completeFork(step)
		case "compensating":
			if len(step.State.Children) > 0 {
				if err := service.retireForkBranches(step); err != nil {
					return err
				}
			}
			return service.completeFork(step)
		case "complete", "failed":
			return nil
		default:
			return loom.Fail(loom.ManualRecovery, "fork group phase is unknown")
		}
	}
}

func (service *Service) captureForkSource(step *forkStep, link resourceLink) error {
	capture, err := loadCapture(service.ctx, service.store.db, link.ChildID)
	if err != nil {
		return err
	}
	if capture.Status != "succeeded" || capture.State.Phase != "complete" || step.Input.Source == nil || !reflect.DeepEqual(capture.Input.Source, *step.Input.Source) {
		return loom.Fail(loom.ManualRecovery, "fork capture lacks its original completed source")
	}
	source, err := service.loadRestoreSource(service.ctx, service.store.db, step.Input.OwnerTenantID, capture.Input.Snapshot.ID)
	if err != nil {
		return err
	}
	if err = verifyRestoreSourceFiles(source); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadFork(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) {
		return loom.Fail(loom.Conflict, "fork capture handoff changed")
	}
	if err = pinForkSnapshot(service.ctx, tx, step.OperationID, step.Input.OwnerTenantID, source.Snapshot.ID); err != nil {
		return err
	}
	state := step.State
	state.Phase = "captured"
	state.Snapshot = &source
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='fork_group' AND input_hash=? AND status IN ('pending','running') AND body=?", encode(state), step.OperationID, step.Hash, encode(step.State))); err != nil {
		return err
	}
	if err = service.fault("fork_capture_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	return service.store.checkpoint()
}

func (service *Service) allocateFork(step *forkStep) error {
	if step.State.Snapshot == nil {
		return loom.Fail(loom.ManualRecovery, "fork allocation lacks frozen source")
	}
	source := *step.State.Snapshot
	if err := verifyRestoreSourceFiles(source); err != nil {
		return err
	}
	image, err := loadImage(service.ctx, service.store.db, step.Input.Template.Config.Image)
	if err != nil {
		return err
	}
	if err = verifyImageFiles(image, filepath.Join(service.store.directory, "images", image.Digest[7:])); err != nil {
		return err
	}
	free, err := loom.FreeBytes(service.store.directory)
	if err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	if err = service.checkAdmissionOpen(); err != nil {
		return err
	}
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadFork(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "captured" {
		return loom.Fail(loom.Conflict, "fork allocation intent changed")
	}
	if err = consumeForkForecast(service.ctx, tx, *step); err != nil {
		return err
	}
	var children []string
	for index, target := range step.Input.Targets {
		parent := &forkBranchAdmission{
			Group:  *step,
			Target: target,
			Index:  index,
		}
		request := CloneRequest{
			TenantID:   step.Input.OwnerTenantID,
			Key:        fmt.Sprintf("%s:branch:%d", step.OperationID, index),
			SnapshotID: source.Snapshot.ID,
		}
		command := step.Input.Command
		command.Action, command.SandboxID, command.CheckpointID, command.Count = "fork_branch", target.ID, "", 0
		frozen := encode(struct {
			Action  string
			Request CloneRequest
			Claim   Command
		}{"clone", request, command})
		activation := &claimAdmission{
			Command:  step.Input.Command,
			Settings: step.Input.Settings,
		}
		child, err := service.admitProvisionIn(service.ctx, tx, request.TenantID, request.Key, frozen, digest(frozen), step.Input.Template, image, &source, nil, &provisionTarget{
			Sandbox:       target,
			SlotOperation: step.OperationID,
			SlotHolder:    forkSlotHolder(step.OperationID, target.ID),
			Parent:        parent,
			Activation:    activation,
		}, free)
		if err != nil {
			return err
		}
		children = append(children, child.ID)
	}
	state := step.State
	state.Phase = "allocated"
	state.Children = children
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='fork_group' AND input_hash=? AND status IN ('pending','running') AND body=?", encode(state), step.OperationID, step.Hash, encode(step.State))); err != nil {
		return err
	}
	if err = service.fault("fork_allocation_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	service.forkSecrets(*step)
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("fork_allocated")
}

func (service *Service) compensateFork(step *forkStep, message string) error {
	state := step.State
	state.Phase = "compensating"
	if state.Error == "" {
		state.Error = message
	}
	return service.saveFork(step, state)
}

func (service *Service) completeFork(step forkStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadFork(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) {
		return loom.Fail(loom.Conflict, "fork completion changed")
	}
	failed := step.State.Phase == "compensating"
	if !failed && step.State.Phase != "allocated" {
		return loom.Fail(loom.Conflict, "fork cannot complete this phase")
	}
	if failed && len(step.State.Children) == 0 {
		if err = retireUnallocatedFork(service.ctx, tx, step); err != nil {
			return err
		}
	}
	for index, target := range step.Input.Targets {
		if failed {
			if len(step.State.Children) > 0 {
				link, status, err := loadResourceLink(service.ctx, tx, step.OperationID, fmt.Sprintf("retire:%d", index))
				if err != nil {
					return err
				}
				if status != "succeeded" {
					return loom.Fail(loom.ManualRecovery, "fork retirement has no successful child receipt")
				}
				if link.Kind == "discard" {
					if err = retireForkDiscard(service.ctx, tx, step, index, link); err != nil {
						return err
					}
				}
			}
			sandbox, err := loadSandbox(service.ctx, tx, target.ID, target.TenantID)
			if err != nil {
				return err
			}
			if sandbox.Identity != target.Identity || sandbox.State != "terminated" || sandbox.VMID != "" || sandbox.Instance != "" {
				return loom.Fail(loom.ManualRecovery, "fork compensation lacks verified retirement")
			}
		} else {
			child, err := service.getOperationIn(service.ctx, tx, "tenant", step.Input.OwnerTenantID, step.State.Children[index])
			if err != nil {
				return err
			}
			view, err := loadPublication(service.ctx, tx, child.ID)
			if err != nil {
				return err
			}
			if child.Status != "succeeded" || !child.Result.ResourceReady || child.Result.SandboxID != target.ID || view.Status != "succeeded" || view.Result.Sandbox == nil || view.Result.Sandbox.ID != target.ID || view.Result.Sandbox.TenantID != target.TenantID || view.Result.Sandbox.Epoch != target.Identity.Epoch || view.Result.Sandbox.VMID != child.Result.VMID || view.Result.Sandbox.Instance == "" {
				return loom.Fail(loom.ManualRecovery, "fork branch lacks its original successful activation")
			}
		}
	}
	if step.State.Snapshot != nil {
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"DELETE FROM source_leases WHERE snapshot_id=? AND operation_id=? AND step_key='fork_group'", []any{
				step.State.Snapshot.Snapshot.ID,
				step.OperationID,
			}},
			{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{
				step.State.Snapshot.Snapshot.ID,
				step.OperationID + ":fork_group",
			}},
			{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{
				step.Input.OwnerTenantID,
				step.State.Snapshot.Snapshot.ID,
				step.OperationID + ":fork_group",
			}},
		} {
			if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
				return err
			}
		}
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='step' AND holder_id=?", step.Input.Template.Config.Image, step.OperationID+":fork_group")); err != nil {
		return err
	}
	operation, err := service.getOperationIn(service.ctx, tx, "tenant", step.Input.RequestTenantID, step.OperationID)
	if err != nil {
		return err
	}
	if !sameForkTargets(operation.Result.SandboxIDs, forkTargetIDs(step.Input)) {
		return loom.Fail(loom.ManualRecovery, "fork result destinations changed")
	}
	result := operation.Result
	result.ResourceReady = !failed
	result.Error = step.State.Error
	state := step.State
	status := "succeeded"
	state.Phase = "complete"
	if failed {
		status = "failed"
		state.Phase = "failed"
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"UPDATE operation_steps SET status=?,body=? WHERE operation_id=? AND step_key='fork_group' AND input_hash=? AND status IN ('pending','running')", []any{
			status,
			encode(state),
			step.OperationID,
			step.Hash,
		}},
		{"UPDATE operations SET status=?,body=? WHERE id=? AND status IN ('pending','running')", []any{
			status,
			encode(result),
			step.OperationID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("fork_completion_before_commit"); err != nil {
		return err
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	service.mu.Lock()
	delete(service.opSecrets, step.OperationID)
	for _, child := range step.State.Children {
		delete(service.opSecrets, child)
	}
	service.mu.Unlock()
	return service.store.checkpoint()
}
