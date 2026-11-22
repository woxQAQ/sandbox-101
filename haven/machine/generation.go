package machine

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// AvailableNetworkSlot selects within the caller's admission snapshot. The
// candidate is inserted in that same transaction before responsibility passes.
func AvailableNetworkSlot(ctx context.Context, tx *sql.Tx) (int, error) {
	allocatedSlots, err := dbgen.New(tx).ListSlots(ctx)
	if err != nil {
		return 0, err
	}
	slot := 1
	for _, allocated := range allocatedSlots {
		if int64(slot) == allocated {
			slot++
		}
	}
	if slot >= 32768 {
		return 0, fmt.Errorf("%w: network address pool exhausted", errs.ErrExhausted)
	}
	return slot, nil
}

// AdmitCandidate records the physical candidate and its original generation
// intent. The caller retains the transaction, quota admission and commit.
func AdmitCandidate(ctx context.Context, tx *sql.Tx, owner string, record VMRecord) error {
	if err := store.RequireOne(dbgen.New(tx).CreateCandidate(ctx, dbgen.CreateCandidateParams{
		ID:   record.Resource.VMID,
		Slot: int64(record.Resource.Slot),
		OperationID: sql.NullString{
			String: owner,
			Valid:  true,
		},
		Body: operation.Encode(record),
	})); err != nil {
		return err
	}
	return RetainGenerationIntent(ctx, tx, owner, record)
}

// RetainGenerationIntent records the instance before its version. Restore calls
// this after claiming the stopped VM and updating sandbox ownership; those
// intervening business writes remain in the caller's transaction.
func RetainGenerationIntent(ctx context.Context, tx *sql.Tx, owner string, record VMRecord) error {
	queries := dbgen.New(tx)
	if err := store.RequireOne(queries.InsertInstance(ctx, dbgen.InsertInstanceParams{
		ID:          record.Resource.Instance,
		VmID:        record.Resource.VMID,
		OperationID: owner,
		Body:        operation.Encode(record),
	})); err != nil {
		return err
	}
	return store.RequireOne(queries.InsertVersion(ctx, dbgen.InsertVersionParams{
		VmID:        record.Resource.VMID,
		OperationID: owner,
		StepKey:     "resource",
		Body: operation.Encode(RetainedVersion{
			Resource: record.Resource,
		}),
	}))
}

func CheckGenerationCounts(ctx context.Context, tx *sql.Tx, source ResourceStep, generations []ResourceStep) error {
	var expectedBacking int64
	for _, generation := range generations {
		if generation.State.BackingReady {
			expectedBacking++
		}
	}

	count, err := RetainedInstanceCount(ctx, tx, source.Candidate)
	if err != nil || count != int64(len(generations)) {
		return fmt.Errorf("unrecorded VM generation or backing retained: %v", err)
	}
	count, err = RetainedVersionCount(ctx, tx, source.Candidate)
	if err != nil || count != int64(len(generations)) {
		return fmt.Errorf("unrecorded VM generation or backing retained: %v", err)
	}
	count, err = dbgen.New(tx).CountVMBackings(ctx, source.Candidate)

	if err != nil || count != expectedBacking {
		return fmt.Errorf("unrecorded VM generation or backing retained: %v", err)
	}
	return nil
}

func VerifyGenerationDeletion(intent loom.DeletionIntent, generations []ResourceStep) error {
	for _, generation := range generations {
		for path, identity := range GenerationFixedNodes(generation) {
			node, ok := intent.Nodes[path]
			if !ok || node != identity {
				return fmt.Errorf("deletion receipt differs from retained generation proof")
			}
		}
	}
	return nil
}

func GenerationDeletionMembers(generations []ResourceStep) []loom.VMDeletionMember {
	members := make([]loom.VMDeletionMember, 0, len(generations))
	for _, generation := range generations {
		members = append(members, loom.VMDeletionMember{
			Resource:     generation.Input.Resource,
			DiskIdentity: generation.State.DiskIdentity,
			Materials:    generation.State.Materials,
			Unpublished:  generation.Status == "failed",
			OperationID:  generation.OperationID,
		})
	}
	return members
}

func RetireGeneration(ctx context.Context, tx *sql.Tx, generation ResourceStep, history SnapshotProofs) error {
	if generation.Input.Restore != nil {
		source := generation.Input.Restore.Snapshot

		if generation.State.BackingReady {
			if err := store.RequireOne(dbgen.New(tx).DeleteBacking(ctx, dbgen.DeleteBackingParams{
				Path:       filepath.Join(generation.Input.Resource.RestoreDir, "memory-ranges"),
				VmID:       generation.Candidate,
				FileDigest: source.Manifest.Files["ch/memory-ranges"],
			})); err != nil {
				return err
			}
			if err := quota.RemoveResourceReference(ctx, tx, quota.ResourceReference{
				Kind:       "snapshot",
				ID:         source.ID,
				HolderKind: "backing",
				HolderID:   generation.Input.BackingHolder,
			}); err != nil {
				return err
			}
			if err := quota.RemoveCostReference(ctx, tx, quota.CostReference{
				TenantID:   generation.Input.TenantID,
				Kind:       "snapshot",
				Key:        source.ID,
				HolderKind: "active_use",
				HolderID:   "backing:" + generation.Input.BackingHolder,
			}); err != nil {
				return err
			}
		} else {
			if err := store.RequireOne(dbgen.New(tx).DeleteSourceLease(ctx, dbgen.DeleteSourceLeaseParams{
				SnapshotID:  source.ID,
				OperationID: generation.OperationID,
				StepKey:     generation.Key,
			})); err != nil {
				return err
			}
			if err := quota.RemoveResourceReference(ctx, tx, quota.ResourceReference{
				Kind:       "snapshot",
				ID:         source.ID,
				HolderKind: "step",
				HolderID:   generation.OperationID + ":resource",
			}); err != nil {
				return err
			}
			if err := quota.RemoveCostReference(ctx, tx, quota.CostReference{
				TenantID:   generation.Input.TenantID,
				Kind:       "snapshot",
				Key:        source.ID,
				HolderKind: "engine_dependency",
				HolderID:   generation.OperationID + ":resource",
			}); err != nil {
				return err
			}
		}
		if err := quota.ReleaseReservation(ctx, tx, "restore:"+generation.OperationID); err != nil {
			return err
		}
		if err := RetireDeletedSnapshot(ctx, tx, *generation.Input.Restore, history); err != nil {
			return err
		}
	}

	if err := store.RequireOne(dbgen.New(tx).DeleteVersion(ctx, dbgen.DeleteVersionParams{
		VmID:        generation.Candidate,
		OperationID: generation.OperationID,
		StepKey:     generation.Key,
	})); err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(tx).DeleteInstance(ctx, dbgen.DeleteInstanceParams{
		ID:          generation.Input.Resource.Instance,
		VmID:        generation.Candidate,
		OperationID: generation.OperationID,
	}))
}
