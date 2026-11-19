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
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

func (f *Forks) runForkGroup(step ForkStep) error {
	for {
		switch step.State.Phase {
		case "admitted":
			link, err := f.runResourceLink(step.OperationID, "capture")
			if err != nil {
				if errors.Is(err, errs.ErrInterrupted) || f.ctx.Err() != nil {
					return err
				}
				child, lookup := f.operations.GetOperation(f.ctx, step.Input.OwnerTenantID, link.ChildID)
				if lookup != nil || !operation.Terminal(child.Status) {
					return err
				}
				if err = f.compensateFork(&step, "fork source capture failed"); err != nil {
					return err
				}
				continue
			}
			if err = f.captureForkSource(&step, link); err != nil {
				return err
			}
		case "captured":
			if err := f.allocateFork(&step); err != nil {
				if errors.Is(err, errs.ErrInterrupted) || f.ctx.Err() != nil {
					return err
				}
				if err = f.compensateFork(&step, "fork batch allocation could not be admitted"); err != nil {
					return err
				}
			}
		case "allocated":
			f.forkSecrets(step)
			for _, id := range step.State.Children {
				f.dispatchBranch(id)
			}
			failed := false
			for index, id := range step.State.Children {
				child, err := f.operations.WaitAccepted(f.ctx, step.Input.OwnerTenantID, id)
				if err != nil && !operation.Terminal(child.Status) {
					return err
				}
				link, _, err := machine.LoadResourceLink(f.ctx, f.store.Database, step.OperationID, fmt.Sprintf("branch:%d", index))
				if err != nil {
					return err
				}
				if link.ChildID != id {
					return fmt.Errorf("%w: fork branch dependency changed", errs.ErrManualRecovery)
				}
				if err = f.links.Complete(step.OperationID, fmt.Sprintf("branch:%d", index), link); err != nil {
					return err
				}
				if child.Status != "succeeded" {
					failed = true
				}
			}
			if failed {
				if err := f.compensateFork(&step, "fork branch activation failed"); err != nil {
					return err
				}
				continue
			}
			return f.completeFork(step)
		case "compensating":
			if len(step.State.Children) > 0 {
				if err := f.retireForkBranches(step); err != nil {
					return err
				}
			}
			return f.completeFork(step)
		case "complete", "failed":
			return nil
		default:
			return fmt.Errorf("%w: fork group phase is unknown", errs.ErrManualRecovery)
		}
	}
}

func (f *Forks) captureForkSource(step *ForkStep, link machine.ResourceLink) error {
	capture, err := machine.LoadCapture(f.ctx, f.store.Database, link.ChildID)
	if err != nil {
		return err
	}
	if capture.Status != "succeeded" || capture.State.Phase != "complete" || step.Input.Source == nil || !reflect.DeepEqual(capture.Input.Source, *step.Input.Source) {
		return fmt.Errorf("%w: fork capture lacks its original completed source", errs.ErrManualRecovery)
	}
	source, err := f.history.LoadRestoreSource(f.ctx, f.store.Database, step.Input.OwnerTenantID, capture.Input.Snapshot.ID)
	if err != nil {
		return err
	}
	if err = machine.VerifyRestoreSourceFiles(source); err != nil {
		return err
	}
	f.admission.Lock()
	defer f.admission.Unlock()
	tx, err := f.store.Database.BeginTx(f.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadFork(f.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) {
		return fmt.Errorf("%w: fork capture handoff changed", errs.ErrConflict)
	}
	if err = machine.PinSnapshotSource(f.ctx, tx, step.Input.OwnerTenantID, machine.SourceLease{
		SnapshotID:  source.Snapshot.ID,
		OperationID: step.OperationID,
		StepKey:     "fork_group",
	}); err != nil {
		return err
	}
	state := step.State
	state.Phase = "captured"
	state.Snapshot = &source
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(f.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "fork_group",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(state),
	})); err != nil {
		return err
	}
	if err = f.fault("fork_capture_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	return f.store.Checkpoint()
}

func (f *Forks) allocateFork(step *ForkStep) error {
	if step.State.Snapshot == nil {
		return fmt.Errorf("%w: fork allocation lacks frozen source", errs.ErrManualRecovery)
	}
	source := *step.State.Snapshot
	if err := machine.VerifyRestoreSourceFiles(source); err != nil {
		return err
	}
	manifest, err := image.LoadManifest(f.ctx, f.store.Database, step.Input.Template.Config.Image)
	if err != nil {
		return err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(f.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return err
	}
	free, err := loom.FreeBytes(f.store.Directory)
	if err != nil {
		return err
	}
	f.admission.Lock()
	defer f.admission.Unlock()
	if err = f.gate.Check(); err != nil {
		return err
	}
	tx, err := f.store.Database.BeginTx(f.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadFork(f.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "captured" {
		return fmt.Errorf("%w: fork allocation intent changed", errs.ErrConflict)
	}
	if err = consumeForkForecast(f.ctx, tx, *step); err != nil {
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
		child, err := f.provisioning.AdmitIn(f.ctx, tx, ProvisionAdmission{
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
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(f.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "fork_group",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(state),
	})); err != nil {
		return err
	}
	if err = f.fault("fork_allocation_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	f.forkSecrets(*step)
	if err = f.store.Checkpoint(); err != nil {
		return err
	}
	return f.fault("fork_allocated")
}

func (f *Forks) compensateFork(step *ForkStep, message string) error {
	state := step.State
	state.Phase = "compensating"
	if state.Error == "" {
		state.Error = message
	}
	return f.saveFork(step, state)
}

func (f *Forks) completeFork(step ForkStep) error {
	f.admission.Lock()
	defer f.admission.Unlock()
	tx, err := f.store.Database.BeginTx(f.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadFork(f.ctx, tx, step.OperationID)
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
		if err = retireUnallocatedFork(f.ctx, tx, step); err != nil {
			return err
		}
	}
	for index, target := range step.Input.Targets {
		if failed {
			if len(step.State.Children) > 0 {
				link, status, err := machine.LoadResourceLink(f.ctx, tx, step.OperationID, fmt.Sprintf("retire:%d", index))
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
					}).retireDiscard(f.ctx, tx, link); err != nil {
						return err
					}
				}
			}
			sandbox, err := LoadSandbox(f.ctx, tx, target.ID, target.TenantID)
			if err != nil {
				return err
			}
			if sandbox.Identity != target.Identity || sandbox.State != "terminated" || sandbox.VMID != "" || sandbox.Instance != "" {
				return fmt.Errorf("%w: fork compensation lacks verified retirement", errs.ErrManualRecovery)
			}
		} else {
			child, err := operation.New(tx).Get(f.ctx, "tenant", step.Input.OwnerTenantID, step.State.Children[index])
			if err != nil {
				return err
			}
			view, err := operation.LoadPublication(f.ctx, tx, child.ID)
			if err != nil {
				return err
			}
			if child.Status != "succeeded" || !child.Result.ResourceReady || child.Result.SandboxID != target.ID || view.Status != "succeeded" || view.Result.Sandbox == nil || view.Result.Sandbox.ID != target.ID || view.Result.Sandbox.TenantID != target.TenantID || view.Result.Sandbox.Epoch != target.Identity.Epoch || view.Result.Sandbox.VMID != child.Result.VMID || view.Result.Sandbox.Instance == "" {
				return fmt.Errorf("%w: fork branch lacks its original successful activation", errs.ErrManualRecovery)
			}
		}
	}
	if step.State.Snapshot != nil {
		if err = machine.ReleaseSnapshotSource(f.ctx, tx, step.Input.OwnerTenantID, machine.SourceLease{
			SnapshotID:  step.State.Snapshot.Snapshot.ID,
			OperationID: step.OperationID,
			StepKey:     "fork_group",
		}); err != nil {
			return err
		}
	}
	if err = quota.New(tx).RemoveResourceReference(f.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         step.Input.Template.Config.Image,
		HolderKind: "step",
		HolderID:   step.OperationID + ":fork_group",
	}); err != nil {
		return err
	}
	accepted, err := operation.New(tx).Get(f.ctx, "tenant", step.Input.RequestTenantID, step.OperationID)
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
	if err = store.RequireOne(dbgen.New(tx).FinishLiveStep(f.ctx, dbgen.FinishLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "fork_group",
		InputHash:   step.Hash,
		Status:      status,
		Body:        operation.Encode(state),
	})); err != nil {
		return err
	}
	if err = f.committer.Complete(tx, operation.Completion{
		ID:     step.OperationID,
		Result: result,
		Failed: failed,
	}); err != nil {
		return err
	}
	if err = f.fault("fork_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	f.registry.DropOperationSecrets(append([]string{
		step.OperationID,
	}, step.State.Children...)...)
	return f.store.Checkpoint()
}

type forkCleanupAdmission struct {
	Group ForkStep
	Index int
}

func (admission *forkCleanupAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, request machine.ResourceAdmission) error {
	child, kind := request.Child, request.Kind
	group, err := LoadFork(ctx, tx, admission.Group.OperationID)
	if err != nil {
		return err
	}
	if group.Hash != admission.Group.Hash || group.State.Phase != "compensating" || operation.Terminal(group.Status) || admission.Index < 0 || admission.Index >= len(group.Input.Targets) || child.Result.SandboxID != group.Input.Targets[admission.Index].ID || (kind != "lifecycle" && kind != "discard") {
		return fmt.Errorf("%w: fork cleanup no longer owns its target", errs.ErrConflict)
	}
	target := group.Input.Targets[admission.Index]
	current, err := LoadSandbox(ctx, tx, target.ID, target.TenantID)
	if err != nil {
		return err
	}
	if current.Identity != target.Identity {
		return fmt.Errorf("%w: fork cleanup birth ownership changed", errs.ErrManualRecovery)
	}
	return machine.InsertResourceLink(ctx, tx, machine.ResourceLinkTarget{
		OperationID: group.OperationID,
		Role:        fmt.Sprintf("retire:%d", admission.Index),
	}, request)
}

func (f *Forks) retireForkBranches(step ForkStep) error {
	for index, target := range step.Input.Targets {
		role := fmt.Sprintf("retire:%d", index)
		if _, _, err := machine.LoadResourceLink(f.ctx, f.store.Database, step.OperationID, role); errors.Is(err, sql.ErrNoRows) {
			source, err := machine.LoadResourceStep(f.ctx, f.store.Database, step.State.Children[index])
			if err != nil {
				return err
			}
			parent := &forkCleanupAdmission{
				Group: step,
				Index: index,
			}
			if source.Status == "failed" {
				_, err = f.deletion.AdmitDiscardForParent(f.ctx, machine.DiscardRequest{
					TenantID:          target.TenantID,
					Key:               step.OperationID + ":discard:" + target.ID,
					SandboxID:         target.ID,
					FailedOperationID: source.OperationID,
					ExpectedInstance:  source.Input.Resource.Instance,
				}, parent)
			} else {
				_, err = f.lifecycle.AdmitForParent(f.ctx, LifecycleRequest{
					Principal: authorization.Principal{
						Root:     true,
						TenantID: target.TenantID,
					},
					Key: step.OperationID + ":release:" + target.ID,
					Command: operation.Command{
						Action:    "release",
						SandboxID: target.ID,
					},
				}, parent)
			}
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if _, err := f.runResourceLink(step.OperationID, role); err != nil {
			return err
		}
	}
	return nil
}

// Failed births have no successful generation to release. Their exact discard
// receipt, current tombstone and original settings jointly authorize retiring the
// business fence; a missing VM by itself grants no handoff authority.
func (admission forkCleanupAdmission) retireDiscard(ctx context.Context, tx *sql.Tx, link machine.ResourceLink) error {
	step, index := admission.Group, admission.Index
	discard, err := machine.LoadDiscard(ctx, tx, link.ChildID)
	if err != nil {
		return err
	}
	target := step.Input.Targets[index]
	source := discard.Input.Source
	if discard.Status != "succeeded" || discard.State.Phase != "complete" || discard.Input.Request.SandboxID != target.ID || discard.Input.Request.TenantID != target.TenantID || source.OperationID != step.State.Children[index] || source.Input.SandboxID != target.ID || source.Input.TenantID != target.TenantID {
		return fmt.Errorf("%w: fork discard receipt does not prove target retirement", errs.ErrManualRecovery)
	}

	sandbox, err := LoadSandbox(ctx, tx, target.ID, target.TenantID)
	if err != nil {
		return err
	}
	if sandbox.Identity != target.Identity || sandbox.SourceOperation != source.OperationID || sandbox.OperationID != discard.OperationID || sandbox.State != "terminated" || sandbox.VMID != source.Candidate || sandbox.Instance != "" {
		return fmt.Errorf("%w: fork discard current tombstone changed", errs.ErrManualRecovery)
	}
	present, err := machine.CurrentVMCount(ctx, tx, source.Candidate)
	if err != nil {
		return err
	}
	if present != 0 {
		return fmt.Errorf("%w: fork discard retains a current VM", errs.ErrManualRecovery)
	}
	settings, err := LoadSettings(ctx, tx, target.ID)
	if err != nil {
		return err
	}
	expected := step.Input.Settings
	expected.Generation = source.OperationID
	if !reflect.DeepEqual(settings, expected) {
		return fmt.Errorf("%w: undelivered fork settings changed", errs.ErrManualRecovery)
	}
	next := sandbox
	next.VMID = ""
	next.OperationID = ""
	desired := settings
	desired.ActiveEpoch = ""
	desired.Instance = ""
	desired.Deadline = time.Time{}
	desired.PausedUntil = time.Time{}
	if err = store.RequireOne(dbgen.New(tx).DeleteFence(ctx, dbgen.DeleteFenceParams{
		SandboxID:   target.ID,
		OperationID: source.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    target.ID,
		PreviousBody: operation.Encode(settings),
		NextBody:     operation.Encode(desired),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).RetireTerminatedBody(ctx, dbgen.RetireTerminatedBodyParams{
		ID:           target.ID,
		Tenant:       target.TenantID,
		PreviousBody: operation.Encode(sandbox),
		NextBody:     operation.Encode(next),
	})); err != nil {
		return err
	}
	return nil
}
