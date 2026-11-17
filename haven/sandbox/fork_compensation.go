package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type forkCleanupAdmission struct {
	Group ForkStep
	Index int
}

func (parent *forkCleanupAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, child operation.Record, kind, candidate, hash string) error {
	group, err := LoadFork(ctx, tx, parent.Group.OperationID)
	if err != nil {
		return err
	}
	if group.Hash != parent.Group.Hash || group.State.Phase != "compensating" || operation.Terminal(group.Status) || parent.Index < 0 || parent.Index >= len(group.Input.Targets) || child.Result.SandboxID != group.Input.Targets[parent.Index].ID || (kind != "lifecycle" && kind != "discard") {
		return fmt.Errorf("%w: fork cleanup no longer owns its target", errs.ErrConflict)
	}
	target := group.Input.Targets[parent.Index]
	current, err := LoadSandbox(ctx, tx, target.ID, target.TenantID)
	if err != nil {
		return err
	}
	if current.Identity != target.Identity {
		return fmt.Errorf("%w: fork cleanup birth ownership changed", errs.ErrManualRecovery)
	}
	return machine.InsertResourceLink(ctx, tx, group.OperationID, fmt.Sprintf("retire:%d", parent.Index), "", child, kind, candidate, hash)
}

func (service *Forks) retireForkBranches(step ForkStep) error {
	for index, target := range step.Input.Targets {
		role := fmt.Sprintf("retire:%d", index)
		if _, _, err := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, role); errors.Is(err, sql.ErrNoRows) {
			source, err := machine.LoadResourceStep(service.ctx, service.store.Database, step.State.Children[index])
			if err != nil {
				return err
			}
			parent := &forkCleanupAdmission{
				Group: step,
				Index: index,
			}
			if source.Status == "failed" {
				_, err = service.deletion.AdmitDiscardForParent(service.ctx, machine.DiscardRequest{
					TenantID:          target.TenantID,
					Key:               step.OperationID + ":discard:" + target.ID,
					SandboxID:         target.ID,
					FailedOperationID: source.OperationID,
					ExpectedInstance:  source.Input.Resource.Instance,
				}, parent)
			} else {
				_, err = service.lifecycle.AdmitForParent(service.ctx, authorization.Principal{
					Root:     true,
					TenantID: target.TenantID,
				}, step.OperationID+":release:"+target.ID, operation.Command{
					Action:    "release",
					SandboxID: target.ID,
				}, parent)
			}
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if _, err := service.runResourceLink(step.OperationID, role); err != nil {
			return err
		}
	}
	return nil
}

// Failed births have no successful generation to release. Their exact discard
// receipt, current tombstone and original settings jointly authorize retiring the
// business fence; a missing VM by itself grants no handoff authority.
func retireForkDiscard(ctx context.Context, tx *sql.Tx, step ForkStep, index int, link machine.ResourceLink) error {
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
