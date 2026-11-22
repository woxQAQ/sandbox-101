package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (p *Provisioning) runClone(step ResourceStep) error {
	if step.Input.Restore == nil || step.State.RestoreBlocked {
		return fmt.Errorf("%w: restore source missing or candidate blocked", errs.ErrManualRecovery)
	}
	p.storeWriteMu.Lock()
	tx, err := p.store.Database.BeginTx(p.ctx, nil)
	if err == nil {
		_, _, _, err = p.ownership.VerifyProvisionIntent(p.proofs, p.ctx, tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	p.storeWriteMu.Unlock()
	if err != nil {
		return err
	}
	r := step.Input.Resource
	source := step.Input.Restore
	if err = image.VerifyFiles(step.Input.Image, r.ImageDir); err != nil {
		return err
	}
	if !step.State.BackingReady {
		observation, err := p.backend.Inspect(p.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive {
			return p.failStep(step, fmt.Errorf("%w: unproved restore candidate already in use", errs.ErrManualRecovery))
		}
		if step.State.Materials != nil && !step.State.Materials.Complete {
			return p.failStep(step, fmt.Errorf("%w: restore interrupted before material completion; original candidate retained", errs.ErrIncompatible))
		}
		if err = VerifyRestoreSourceFiles(*source); err != nil {
			return p.failStep(step, err)
		}
		if step.State.Materials == nil {
			if step.State.Phase == "admitted" {
				next := step.State
				next.Phase = "restore_prepare_intent"
				if err = p.saveRestore(&step, next); err != nil {
					return err
				}
			}
			if step.State.Phase != "restore_prepare_intent" {
				return p.failStep(step, fmt.Errorf("unproved restore phase changed"))
			}
			request := loom.RestorePreparation{
				Resource:    r,
				OperationID: step.OperationID,
				Source:      source.Snapshot.Dir,
				Digests:     source.Snapshot.Manifest.Files,
			}
			var boundary *loom.RestoreBoundary
			if step.Action == "clone" {
				boundary, err = loom.PrepareClone(request)
			} else {
				boundary, err = loom.PrepareRestore(request)
			}
			if err != nil {
				return p.failStep(step, err)
			}
			defer func() {
				_ = boundary.Close()
			}()
			next := step.State
			next.Materials = CopyRestoreMaterials(boundary.Materials)
			next.Phase = "restore_candidate_prepared"
			if err = p.saveRestore(&step, next); err != nil {
				return err
			}
			for _, name := range loom.RestoreFileNames() {
				if err = boundary.CopyFile(name); err != nil {
					return p.failStep(step, err)
				}
				if name == "memory-ranges" {
					continue
				}
				next = step.State
				next.Phase = "restore_candidate_copied"
				if err = p.saveRestore(&step, next); err != nil {
					return err
				}
				if err = boundary.PublishFile(name); err != nil {
					return p.failStep(step, err)
				}
				next = step.State
				next.Phase = "restore_candidate_renamed"
				if err = p.saveRestore(&step, next); err != nil {
					return err
				}
			}
			if err = boundary.SyncMaterials(); err != nil {
				return p.failStep(step, err)
			}
			next = step.State
			next.Materials = CopyRestoreMaterials(boundary.Materials)
			next.Phase = "restore_candidate_synced"
			if err = p.saveRestore(&step, next); err != nil {
				return err
			}
			next = step.State
			next.Materials = CopyRestoreMaterials(boundary.Materials)
			next.Materials.Complete = true
			identity := next.Materials.Files["disk.raw"].Identity
			next.DiskIdentity = &identity
			next.Phase = "restore_materials_complete"
			if err = p.saveRestore(&step, next); err != nil {
				return err
			}
		}
		if err = loom.VerifyRestoreMaterials(step.State.Materials, r, source.Snapshot.Manifest.Files, true); err != nil {
			return p.failStep(step, err)
		}
		if err = p.backend.Prepare(p.ctx, r); err != nil {
			return p.failStep(step, err)
		}
		if err = loom.VerifyRestoreMaterials(step.State.Materials, r, source.Snapshot.Manifest.Files, true); err != nil {
			return p.failStep(step, err)
		}
		if err = p.transferRestore(&step); err != nil {
			return err
		}
	}
	observation, err := p.backend.Inspect(p.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive && step.State.Phase == "restore_resources_ready" {
		return p.failStep(step, fmt.Errorf("%w: live restore candidate lacks durable launch intent", errs.ErrManualRecovery))
	}
	checkDisk := !observation.Alive && (step.State.Phase == "restore_resources_ready" || step.State.Phase == "launch_intent")
	if err = loom.VerifyRestoreMaterials(step.State.Materials, r, source.Snapshot.Manifest.Files, checkDisk); err != nil {
		return p.failStep(step, err)
	}
	if err = loom.ValidateCOWTopology(r.RestoreDir, uint64(r.Config.Shape.MemoryMiB)<<20); err != nil {
		return p.failStep(step, err)
	}
	if step.State.Phase == "restore_resources_ready" {
		if err = p.markPhase(&step, "launch_intent"); err != nil {
			return err
		}
	}
	return p.startCandidate(step, true)
}

func VerifyRestoreSourceFiles(source SnapshotSource) error {
	directory, err := loom.OpenCaptureDir(source.Snapshot.Dir)
	if err != nil {
		return err
	}
	info, err := directory.Stat()
	closeErr := directory.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if loom.IdentityOf(info) != source.Publication {
		return fmt.Errorf("restore source directory identity changed")
	}
	if err = loom.VerifyFiles(source.Snapshot.Dir, source.Snapshot.Manifest.Files); err != nil {
		return err
	}
	return loom.ValidateCOWTopology(filepath.Join(source.Snapshot.Dir, "ch"), uint64(source.Snapshot.Manifest.Config.Shape.MemoryMiB)<<20)
}

// RestoreRelations verifies retained dependencies without authorizing execution.
// Explicit cleanup can check a blocked step through this read-only relation proof.
func (proofs SnapshotProofs) RestoreRelations(ctx context.Context, tx *sql.Tx, step ResourceStep) error {
	source := step.Input.Restore
	if source == nil {
		return fmt.Errorf("restore source missing")
	}
	historical, err := LoadCapture(ctx, tx, source.CaptureOperation)
	if err != nil {
		return err
	}
	if err = proofs.ValidateRestoreSource(*source, step.Input.TenantID, historical); err != nil {
		return err
	}
	if step.State.BackingReady {
		if step.State.Materials == nil || !step.State.Materials.Complete || step.State.DiskIdentity == nil {
			return fmt.Errorf("backing transfer lacks complete material proof")
		}
	} else {
		current, err := proofs.LoadSnapshotSource(ctx, tx, step.Input.TenantID, source.Snapshot.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *source) {
			return fmt.Errorf("leased restore source changed")
		}
	}

	count, err := dbgen.New(tx).CountCaptureOwner(ctx, dbgen.CountCaptureOwnerParams{
		SnapshotID:        source.Snapshot.ID,
		TenantID:          step.Input.TenantID,
		SourceOperationID: source.CaptureOperation,
	})

	if err != nil || count != 1 {
		return fmt.Errorf("restore dependency responsibility changed: %v", err)
	}
	shape := step.Input.Resource.Config.Shape
	count, err = quota.CountMatchingCost(ctx, tx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        step.Input.TenantID,
			Kind:            "snapshot",
			Key:             source.Snapshot.ID,
			SourceOperation: source.CaptureOperation,
		},
		StorageMiB: shape.MemoryMiB + shape.DiskMiB,
	})
	if err != nil || count != 1 {
		return fmt.Errorf("restore dependency responsibility changed: %v", err)
	}
	count, err = quota.CountMatchingReservation(ctx, tx, quota.Reservation{
		Owner: "restore:" + step.OperationID,
		Bytes: (shape.MemoryMiB + shape.DiskMiB + 64) << 20,
	})
	if err != nil || count != 1 {
		return fmt.Errorf("restore dependency responsibility changed: %v", err)
	}
	path := filepath.Join(step.Input.Resource.RestoreDir, "memory-ranges")
	var expectedStep int64 = 1
	if step.State.BackingReady {
		expectedStep = 0
		count, err = dbgen.New(tx).CountStepLease(ctx, dbgen.CountStepLeaseParams{
			OperationID: step.OperationID,
			StepKey:     step.Key,
		})

		if err != nil || count != 0 {
			return fmt.Errorf("restore dependency responsibility changed: %v", err)
		}
		count, err = dbgen.New(tx).CountMatchingBacking(ctx, dbgen.CountMatchingBackingParams{
			Path:       path,
			VmID:       step.Candidate,
			FileDigest: source.Snapshot.Manifest.Files["ch/memory-ranges"],
		})

		if err != nil || count != 1 {
			return fmt.Errorf("restore dependency responsibility changed: %v", err)
		}
		count, err = quota.CountResourceReference(ctx, tx, quota.ResourceReference{
			Kind:       "snapshot",
			ID:         source.Snapshot.ID,
			HolderKind: "backing",
			HolderID:   step.Input.BackingHolder,
		})
		if err != nil || count != 1 {
			return fmt.Errorf("restore dependency responsibility changed: %v", err)
		}
	} else {
		count, err = dbgen.New(tx).CountSourceLease(ctx, dbgen.CountSourceLeaseParams{
			SnapshotID:  source.Snapshot.ID,
			OperationID: step.OperationID,
			StepKey:     step.Key,
		})

		if err != nil || count != 1 {
			return fmt.Errorf("restore dependency responsibility changed: %v", err)
		}
		count, err = dbgen.New(tx).CountBacking(ctx, path)

		if err != nil || count != 0 {
			return fmt.Errorf("restore dependency responsibility changed: %v", err)
		}
	}
	count, err = quota.CountResourceReference(ctx, tx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         source.Snapshot.ID,
		HolderKind: "step",
		HolderID:   step.OperationID + ":resource",
	})
	if err != nil || count != expectedStep {
		return fmt.Errorf("restore dependency responsibility changed: %v", err)
	}
	if step.State.BackingReady {
		count, err = quota.CountCostReference(ctx, tx, quota.CostReference{
			TenantID:   step.Input.TenantID,
			Kind:       "snapshot",
			Key:        source.Snapshot.ID,
			HolderKind: "active_use",
			HolderID:   "backing:" + step.Input.BackingHolder,
		})
		if err != nil || count != 1 {
			return fmt.Errorf("restore dependency responsibility changed: %v", err)
		}
	}
	count, err = quota.CountCostReference(ctx, tx, quota.CostReference{
		TenantID:   step.Input.TenantID,
		Kind:       "snapshot",
		Key:        source.Snapshot.ID,
		HolderKind: "engine_dependency",
		HolderID:   step.OperationID + ":resource",
	})
	if err != nil || count != expectedStep {
		return fmt.Errorf("restore dependency responsibility changed: %v", err)
	}
	return nil
}

func (p *Provisioning) saveRestore(step *ResourceStep, next ResourceState) error {
	p.storeWriteMu.Lock()
	defer p.storeWriteMu.Unlock()
	tx, err := p.store.Database.BeginTx(p.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, _, err = p.ownership.VerifyProvisionIntent(p.proofs, p.ctx, tx, *step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(p.ctx, dbgen.AdvanceLiveStepParams{
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
		if err = store.RequireOne(dbgen.New(tx).ReplaceVersionBody(p.ctx, dbgen.ReplaceVersionBodyParams{
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
	if err = p.store.Checkpoint(); err != nil {
		return err
	}
	return p.fault(next.Phase)
}

func (p *Provisioning) transferRestore(step *ResourceStep) error {
	p.storeWriteMu.Lock()
	defer p.storeWriteMu.Unlock()
	tx, err := p.store.Database.BeginTx(p.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, _, err = p.ownership.VerifyProvisionIntent(p.proofs, p.ctx, tx, *step); err != nil {
		return err
	}
	if step.State.Materials == nil || !step.State.Materials.Complete || step.State.DiskIdentity == nil || step.State.BackingReady {
		return fmt.Errorf("restore handoff lacks complete original proof")
	}
	source := step.Input.Restore.Snapshot
	next := step.State
	next.BackingReady = true
	next.Phase = "restore_resources_ready"

	if err = store.RequireOne(dbgen.New(tx).InsertBacking(p.ctx, dbgen.InsertBackingParams{
		Path:       filepath.Join(step.Input.Resource.RestoreDir, "memory-ranges"),
		VmID:       step.Candidate,
		FileDigest: source.Manifest.Files["ch/memory-ranges"],
	})); err != nil {
		return err
	}
	if err = quota.AddResourceReference(p.ctx, tx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         source.ID,
		HolderKind: "backing",
		HolderID:   step.Input.BackingHolder,
	}); err != nil {
		return err
	}
	if err = quota.RemoveResourceReference(p.ctx, tx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         source.ID,
		HolderKind: "step",
		HolderID:   step.OperationID + ":resource",
	}); err != nil {
		return err
	}
	if err = quota.TransferCostReference(p.ctx, tx, quota.CostReference{
		TenantID:   step.Input.TenantID,
		Kind:       "snapshot",
		Key:        source.ID,
		HolderKind: "engine_dependency",
		HolderID:   step.OperationID + ":resource",
	}, "active_use", "backing:"+step.Input.BackingHolder); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteSourceLease(p.ctx, dbgen.DeleteSourceLeaseParams{
		SnapshotID:  source.ID,
		OperationID: step.OperationID,
		StepKey:     step.Key,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceRunningStep(p.ctx, dbgen.AdvanceRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      step.Key,
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = p.fault("restore_transfer_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = next
	if err = p.store.Checkpoint(); err != nil {
		return err
	}
	return p.fault(next.Phase)
}
