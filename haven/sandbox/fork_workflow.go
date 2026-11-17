package sandbox

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

func (service *Forks) runForkGroup(step ForkStep) error {
	for {
		switch step.State.Phase {
		case "admitted":
			link, err := service.runResourceLink(step.OperationID, "capture")
			if err != nil {
				if errors.Is(err, errs.ErrInterrupted) || service.ctx.Err() != nil {
					return err
				}
				child, lookup := service.operations.GetOperation(service.ctx, step.Input.OwnerTenantID, link.ChildID)
				if lookup != nil || !operation.Terminal(child.Status) {
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
				if errors.Is(err, errs.ErrInterrupted) || service.ctx.Err() != nil {
					return err
				}
				if err = service.compensateFork(&step, "fork batch allocation could not be admitted"); err != nil {
					return err
				}
			}
		case "allocated":
			service.forkSecrets(step)
			for _, id := range step.State.Children {
				service.dispatchBranch(id)
			}
			failed := false
			for index, id := range step.State.Children {
				child, err := service.operations.WaitAccepted(service.ctx, step.Input.OwnerTenantID, id)
				if err != nil && !operation.Terminal(child.Status) {
					return err
				}
				link, _, err := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, fmt.Sprintf("branch:%d", index))
				if err != nil {
					return err
				}
				if link.ChildID != id {
					return fmt.Errorf("%w: fork branch dependency changed", errs.ErrManualRecovery)
				}
				if err = service.links.Complete(step.OperationID, fmt.Sprintf("branch:%d", index), link); err != nil {
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
			return fmt.Errorf("%w: fork group phase is unknown", errs.ErrManualRecovery)
		}
	}
}

func (service *Forks) captureForkSource(step *ForkStep, link machine.ResourceLink) error {
	capture, err := machine.LoadCapture(service.ctx, service.store.Database, link.ChildID)
	if err != nil {
		return err
	}
	if capture.Status != "succeeded" || capture.State.Phase != "complete" || step.Input.Source == nil || !reflect.DeepEqual(capture.Input.Source, *step.Input.Source) {
		return fmt.Errorf("%w: fork capture lacks its original completed source", errs.ErrManualRecovery)
	}
	source, err := service.history.LoadRestoreSource(service.ctx, service.store.Database, step.Input.OwnerTenantID, capture.Input.Snapshot.ID)
	if err != nil {
		return err
	}
	if err = machine.VerifyRestoreSourceFiles(source); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadFork(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) {
		return fmt.Errorf("%w: fork capture handoff changed", errs.ErrConflict)
	}
	if err = machine.PinSnapshotSource(service.ctx, tx, step.Input.OwnerTenantID, machine.SourceLease{
		SnapshotID:  source.Snapshot.ID,
		OperationID: step.OperationID,
		StepKey:     "fork_group",
	}); err != nil {
		return err
	}
	state := step.State
	state.Phase = "captured"
	state.Snapshot = &source
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "fork_group",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(state),
	})); err != nil {
		return err
	}
	if err = service.fault("fork_capture_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	return service.store.Checkpoint()
}

func (service *Forks) allocateFork(step *ForkStep) error {
	if step.State.Snapshot == nil {
		return fmt.Errorf("%w: fork allocation lacks frozen source", errs.ErrManualRecovery)
	}
	source := *step.State.Snapshot
	if err := machine.VerifyRestoreSourceFiles(source); err != nil {
		return err
	}
	manifest, err := image.LoadManifest(service.ctx, service.store.Database, step.Input.Template.Config.Image)
	if err != nil {
		return err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return err
	}
	free, err := loom.FreeBytes(service.store.Directory)
	if err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	if err = service.gate.Check(); err != nil {
		return err
	}
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadFork(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "captured" {
		return fmt.Errorf("%w: fork allocation intent changed", errs.ErrConflict)
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
		request := machine.CloneRequest{
			TenantID:   step.Input.OwnerTenantID,
			Key:        fmt.Sprintf("%s:branch:%d", step.OperationID, index),
			SnapshotID: source.Snapshot.ID,
		}
		command := step.Input.Command
		command.Action, command.SandboxID, command.CheckpointID, command.Count = "fork_branch", target.ID, "", 0
		frozen := operation.Encode(struct {
			Action  string
			Request machine.CloneRequest
			Claim   operation.Command
		}{"clone", request, command})
		activation := &ClaimAdmission{
			Command:  step.Input.Command,
			Settings: step.Input.Settings,
		}
		child, err := service.provisioning.AdmitIn(service.ctx, tx, ProvisionAdmission{
			Request: supply.ProvisionRequest{
				TenantID: request.TenantID,
				Key:      request.Key,
				Frozen:   frozen,
				Template: step.Input.Template,
				Image:    manifest,
				Restore:  &source,
			},
			Target: &ProvisionTarget{
				Sandbox:       target,
				SlotOperation: step.OperationID,
				SlotHolder:    forkSlotHolder(step.OperationID, target.ID),
				Parent:        parent,
				Activation:    activation,
			},
		}, free)
		if err != nil {
			return err
		}
		children = append(children, child.ID)
	}
	state := step.State
	state.Phase = "allocated"
	state.Children = children
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "fork_group",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(state),
	})); err != nil {
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
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("fork_allocated")
}

func (service *Forks) compensateFork(step *ForkStep, message string) error {
	state := step.State
	state.Phase = "compensating"
	if state.Error == "" {
		state.Error = message
	}
	return service.saveFork(step, state)
}

func (service *Forks) completeFork(step ForkStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadFork(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) {
		return fmt.Errorf("%w: fork completion changed", errs.ErrConflict)
	}
	failed := step.State.Phase == "compensating"
	if !failed && step.State.Phase != "allocated" {
		return fmt.Errorf("%w: fork cannot complete this phase", errs.ErrConflict)
	}
	if failed && len(step.State.Children) == 0 {
		if err = retireUnallocatedFork(service.ctx, tx, step); err != nil {
			return err
		}
	}
	for index, target := range step.Input.Targets {
		if failed {
			if len(step.State.Children) > 0 {
				link, status, err := machine.LoadResourceLink(service.ctx, tx, step.OperationID, fmt.Sprintf("retire:%d", index))
				if err != nil {
					return err
				}
				if status != "succeeded" {
					return fmt.Errorf("%w: fork retirement has no successful child receipt", errs.ErrManualRecovery)
				}
				if link.Kind == "discard" {
					if err = (forkCleanupAdmission{
						Group: step,
						Index: index,
					}).retireDiscard(service.ctx, tx, link); err != nil {
						return err
					}
				}
			}
			sandbox, err := LoadSandbox(service.ctx, tx, target.ID, target.TenantID)
			if err != nil {
				return err
			}
			if sandbox.Identity != target.Identity || sandbox.State != "terminated" || sandbox.VMID != "" || sandbox.Instance != "" {
				return fmt.Errorf("%w: fork compensation lacks verified retirement", errs.ErrManualRecovery)
			}
		} else {
			child, err := operation.New(tx).Get(service.ctx, "tenant", step.Input.OwnerTenantID, step.State.Children[index])
			if err != nil {
				return err
			}
			view, err := operation.LoadPublication(service.ctx, tx, child.ID)
			if err != nil {
				return err
			}
			if child.Status != "succeeded" || !child.Result.ResourceReady || child.Result.SandboxID != target.ID || view.Status != "succeeded" || view.Result.Sandbox == nil || view.Result.Sandbox.ID != target.ID || view.Result.Sandbox.TenantID != target.TenantID || view.Result.Sandbox.Epoch != target.Identity.Epoch || view.Result.Sandbox.VMID != child.Result.VMID || view.Result.Sandbox.Instance == "" {
				return fmt.Errorf("%w: fork branch lacks its original successful activation", errs.ErrManualRecovery)
			}
		}
	}
	if step.State.Snapshot != nil {
		if err = machine.ReleaseSnapshotSource(service.ctx, tx, step.Input.OwnerTenantID, machine.SourceLease{
			SnapshotID:  step.State.Snapshot.Snapshot.ID,
			OperationID: step.OperationID,
			StepKey:     "fork_group",
		}); err != nil {
			return err
		}
	}
	if err = quota.New(tx).RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         step.Input.Template.Config.Image,
		HolderKind: "step",
		HolderID:   step.OperationID + ":fork_group",
	}); err != nil {
		return err
	}
	accepted, err := operation.New(tx).Get(service.ctx, "tenant", step.Input.RequestTenantID, step.OperationID)
	if err != nil {
		return err
	}
	if !sameForkTargets(accepted.Result.SandboxIDs, forkTargetIDs(step.Input)) {
		return fmt.Errorf("%w: fork result destinations changed", errs.ErrManualRecovery)
	}
	result := accepted.Result
	result.ResourceReady = !failed
	result.Error = step.State.Error
	state := step.State
	status := "succeeded"
	state.Phase = "complete"
	if failed {
		status = "failed"
		state.Phase = "failed"
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).FinishLiveStep(service.ctx, dbgen.FinishLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "fork_group",
		InputHash:   step.Hash,
		Status:      status,
		Body:        operation.Encode(state),
	})); err != nil {
		return err
	}
	if err = operations.Finish(service.ctx, step.OperationID, status, operation.Encode(result)); err != nil {
		return err
	}
	if err = service.fault("fork_completion_before_commit"); err != nil {
		return err
	}
	if err = service.committer.Commit(tx); err != nil {
		return err
	}
	service.access.DropOperationSecrets(append([]string{
		step.OperationID,
	}, step.State.Children...)...)
	return service.store.Checkpoint()
}
