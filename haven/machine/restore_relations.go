package machine

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/quota"
)

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
		current, err := proofs.LoadRestoreSource(ctx, tx, step.Input.TenantID, source.Snapshot.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *source) {
			return fmt.Errorf("leased restore source changed")
		}
	}

	responsibilities := quota.New(tx)
	count, err := dbgen.New(tx).CountCaptureOwner(ctx, dbgen.CountCaptureOwnerParams{
		SnapshotID:        source.Snapshot.ID,
		TenantID:          step.Input.TenantID,
		SourceOperationID: source.CaptureOperation,
	})

	if err != nil || count != 1 {
		return fmt.Errorf("restore dependency responsibility changed: %v", err)
	}
	shape := step.Input.Resource.Config.Shape
	count, err = responsibilities.CountMatchingCost(ctx, quota.Cost{
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
	count, err = responsibilities.CountMatchingReservation(ctx, quota.Reservation{
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
			Path:   path,
			VmID:   step.Candidate,
			Digest: source.Snapshot.Manifest.Files["ch/memory-ranges"],
		})

		if err != nil || count != 1 {
			return fmt.Errorf("restore dependency responsibility changed: %v", err)
		}
		count, err = responsibilities.CountResourceReference(ctx, quota.ResourceReference{
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
	count, err = responsibilities.CountResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         source.Snapshot.ID,
		HolderKind: "step",
		HolderID:   step.OperationID + ":resource",
	})
	if err != nil || count != expectedStep {
		return fmt.Errorf("restore dependency responsibility changed: %v", err)
	}
	if step.State.BackingReady {
		count, err = responsibilities.CountCostReference(ctx, quota.CostReference{
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
	count, err = responsibilities.CountCostReference(ctx, quota.CostReference{
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
