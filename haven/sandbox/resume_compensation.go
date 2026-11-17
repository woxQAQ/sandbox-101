package sandbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func ResumeDestination(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination) (LifecycleStep, error) {
	var empty LifecycleStep
	if destination == nil || destination.Kind != "sleep" {
		return empty, fmt.Errorf("%w: resume slot destination missing", errs.ErrManualRecovery)
	}
	parent, err := LoadLifecycle(ctx, tx, destination.OperationID)
	if err != nil {
		return parent, err
	}
	if parent.Input.Command.Action != "resume" || parent.Status == "succeeded" || parent.Input.Sandbox.State != "paused" || parent.Input.Sandbox.ID != destination.ID || parent.Input.Sandbox.SnapshotID != destination.SnapshotID {
		return parent, fmt.Errorf("%w: resume slot destination differs from original paused sandbox", errs.ErrManualRecovery)
	}
	fence, err := LifecycleFence(ctx, tx, destination.ID)
	if err != nil {
		return parent, err
	}
	if fence != parent.OperationID {
		return parent, fmt.Errorf("%w: resume compensation was superseded", errs.ErrConflict)
	}
	if refs, err := quota.New(tx).CountResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         destination.SnapshotID,
		HolderKind: "sleep",
		HolderID:   destination.ID,
	}); err != nil || refs != 1 {
		return parent, fmt.Errorf("resume original sleep holder missing: %v", err)
	}
	if refs, err := quota.New(tx).CountCostReference(ctx, quota.CostReference{
		TenantID:   parent.Input.OwnerTenantID,
		Kind:       "snapshot",
		Key:        destination.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "sleep:" + destination.ID,
	}); err != nil || refs != 1 {
		return parent, fmt.Errorf("resume original sleep fee missing: %v", err)
	}
	return parent, nil
}

func RestoreResumeSlot(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination, cleanup string) error {
	parent, err := ResumeDestination(ctx, tx, destination)
	if err != nil {
		return err
	}
	return quota.New(tx).TransferCostReference(ctx, quota.CostReference{
		TenantID:   parent.Input.OwnerTenantID,
		Kind:       "sandbox_slot",
		Key:        destination.ID,
		HolderKind: "cleanup",
		HolderID:   cleanup,
	}, "active_use", "sleep:"+destination.ID)
}

func RestorePausedSandbox(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination) error {
	parent, err := ResumeDestination(ctx, tx, destination)
	if err != nil {
		return err
	}
	link, _, err := machine.LoadResourceLink(ctx, tx, parent.OperationID, "clone")
	if err != nil {
		return err
	}
	candidate, err := machine.LoadResourceStep(ctx, tx, link.ChildID)
	if err != nil {
		return err
	}

	current, err := LoadSandbox(ctx, tx, destination.ID, parent.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if current.State != "terminated" || current.SourceOperation != candidate.OperationID || current.VMID != candidate.Candidate {
		return fmt.Errorf("%w: resume candidate no longer owns sandbox compensation", errs.ErrConflict)
	}
	count, err := machine.CurrentVMCount(ctx, tx, candidate.Candidate)
	if err != nil || count != 0 {
		return fmt.Errorf("resume candidate physical responsibility still present: %v", err)
	}
	settings, err := LoadSettings(ctx, tx, destination.ID)
	if err != nil {
		return err
	}
	if settings.Generation != parent.Input.Settings.Generation && settings.Generation != parent.OperationID {
		return fmt.Errorf("%w: resume desired configuration changed", errs.ErrConflict)
	}
	restored := parent.Input.Sandbox
	desired := parent.Input.Settings
	desired.Generation = parent.OperationID
	if err = store.RequireOne(dbgen.New(tx).RestorePausedSandbox(ctx, dbgen.RestorePausedSandboxParams{
		ID:           destination.ID,
		Tenant:       parent.Input.OwnerTenantID,
		PreviousBody: operation.Encode(current),
		NextBody:     operation.Encode(restored),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    destination.ID,
		PreviousBody: operation.Encode(settings),
		NextBody:     operation.Encode(desired),
	})); err != nil {
		return err
	}
	return nil
}
