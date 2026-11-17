package machine

import (
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (service *Provisioning) saveRestore(step *ResourceStep, next ResourceState) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, _, err = service.ownership.VerifyProvisionIntent(service.proofs, service.ctx, tx, *step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      step.Key,
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if next.DiskIdentity != nil && step.State.DiskIdentity == nil {
		previous := RetainedVersion{
			Resource: step.Input.Resource,
		}
		version := RetainedVersion{
			Resource:     step.Input.Resource,
			DiskIdentity: next.DiskIdentity,
		}
		if err = store.RequireOne(dbgen.New(tx).ReplaceVersionBody(service.ctx, dbgen.ReplaceVersionBodyParams{
			ID:           step.OperationID,
			VmID:         step.Candidate,
			OperationID:  step.OperationID,
			PreviousBody: operation.Encode(previous),
			NewBody:      operation.Encode(version),
		})); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State, step.Status = next, "running"
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault(next.Phase)
}

func (service *Provisioning) transferRestore(step *ResourceStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, _, err = service.ownership.VerifyProvisionIntent(service.proofs, service.ctx, tx, *step); err != nil {
		return err
	}
	if step.State.Materials == nil || !step.State.Materials.Complete || step.State.DiskIdentity == nil || step.State.BackingReady {
		return fmt.Errorf("restore handoff lacks complete original proof")
	}
	source := step.Input.Restore.Snapshot
	next := step.State
	next.BackingReady = true
	next.Phase = "restore_resources_ready"

	responsibilities := quota.New(tx)
	if err = store.RequireOne(dbgen.New(tx).InsertBacking(service.ctx, dbgen.InsertBackingParams{
		Path:   filepath.Join(step.Input.Resource.RestoreDir, "memory-ranges"),
		VmID:   step.Candidate,
		Digest: source.Manifest.Files["ch/memory-ranges"],
	})); err != nil {
		return err
	}
	if err = responsibilities.AddResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         source.ID,
		HolderKind: "backing",
		HolderID:   step.Input.BackingHolder,
	}); err != nil {
		return err
	}
	if err = responsibilities.RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         source.ID,
		HolderKind: "step",
		HolderID:   step.OperationID + ":resource",
	}); err != nil {
		return err
	}
	if err = responsibilities.TransferCostReference(service.ctx, quota.CostReference{
		TenantID:   step.Input.TenantID,
		Kind:       "snapshot",
		Key:        source.ID,
		HolderKind: "engine_dependency",
		HolderID:   step.OperationID + ":resource",
	}, "active_use", "backing:"+step.Input.BackingHolder); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteSourceLease(service.ctx, dbgen.DeleteSourceLeaseParams{
		SnapshotID:  source.ID,
		OperationID: step.OperationID,
		StepKey:     step.Key,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceRunningStep(service.ctx, dbgen.AdvanceRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      step.Key,
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = service.fault("restore_transfer_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = next
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault(next.Phase)
}
