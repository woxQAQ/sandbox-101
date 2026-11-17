package machine

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

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

		responsibilities := quota.New(tx)
		if generation.State.BackingReady {
			if err := store.RequireOne(dbgen.New(tx).DeleteBacking(ctx, dbgen.DeleteBackingParams{
				Path:   filepath.Join(generation.Input.Resource.RestoreDir, "memory-ranges"),
				VmID:   generation.Candidate,
				Digest: source.Manifest.Files["ch/memory-ranges"],
			})); err != nil {
				return err
			}
			if err := responsibilities.RemoveResourceReference(ctx, quota.ResourceReference{
				Kind:       "snapshot",
				ID:         source.ID,
				HolderKind: "backing",
				HolderID:   generation.Input.BackingHolder,
			}); err != nil {
				return err
			}
			if err := responsibilities.RemoveCostReference(ctx, quota.CostReference{
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
			if err := responsibilities.RemoveResourceReference(ctx, quota.ResourceReference{
				Kind:       "snapshot",
				ID:         source.ID,
				HolderKind: "step",
				HolderID:   generation.OperationID + ":resource",
			}); err != nil {
				return err
			}
			if err := responsibilities.RemoveCostReference(ctx, quota.CostReference{
				TenantID:   generation.Input.TenantID,
				Kind:       "snapshot",
				Key:        source.ID,
				HolderKind: "engine_dependency",
				HolderID:   generation.OperationID + ":resource",
			}); err != nil {
				return err
			}
		}
		if err := responsibilities.ReleaseReservation(ctx, "restore:"+generation.OperationID); err != nil {
			return err
		}
		if err := RetireDeletedSnapshot(ctx, tx, *generation.Input.Restore, history); err != nil {
			return err
		}
	}

	if err := store.RequireOne(dbgen.New(tx).DeleteVersion(ctx, dbgen.DeleteVersionParams{
		ID:          generation.OperationID,
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
