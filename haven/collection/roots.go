package collection

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/machine"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

// gcGraphFence checks normalized roots before any collector can be admitted.
// Unknown relationships are retained instead of being treated as absent users.
func (service *GarbageCollection) gcGraphFence(ctx context.Context) ([]types.GCItem, error) {
	tx, err := service.store.Database.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var deferred []types.GCItem
	invalid, err := store.HasForeignKeyViolations(ctx, tx)
	if err != nil {
		return nil, err
	}
	if invalid {
		deferred = append(deferred, types.GCItem{
			Kind:   "metadata",
			Reason: "dangling normalized root reference",
		})
	}
	references, err := quota.New(tx).ResourceReferences(ctx)
	if err != nil {
		return nil, err
	}
	for _, reference := range references {
		kind, id, holder, owner := reference.Kind, reference.ID, reference.HolderKind, reference.HolderID
		validKind := kind == "vm" || kind == "image" || kind == "snapshot"
		validHolder := holder == "template" || holder == "vm" || holder == "snapshot" || holder == "capture" || holder == "step" || holder == "checkpoint" || holder == "checkpoint_binding" || holder == "warm" || holder == "sleep" || holder == "backing" || holder == "sandbox" || holder == "cleanup"
		if !validKind || !validHolder || id == "" || owner == "" {
			deferred = append(deferred, types.GCItem{
				Kind:   "metadata",
				ID:     id,
				Reason: "unknown resource reference root",
			})
		}
	}
	templates, err := supply.UnresolvedTemplates(ctx, tx)
	if err != nil {
		return nil, err
	}
	for _, id := range templates {
		deferred = append(deferred, types.GCItem{
			Kind:   "template",
			ID:     id,
			Reason: "template publication root is unresolved or inconsistent",
		})
	}

	leases, err := machine.SourceLeases(ctx, tx)
	if err != nil {
		return nil, err
	}
	for _, lease := range leases {
		var proofErr error
		switch lease.StepKey {
		case "resource":
			source, lookup := machine.LoadResourceStep(ctx, tx, lease.OperationID)
			proofErr = lookup
			if proofErr == nil {
				if source.Input.Restore == nil || source.Input.Restore.Snapshot.ID != lease.SnapshotID || source.State.BackingReady {
					proofErr = fmt.Errorf("source lease original step mismatch")
				} else {
					proofErr = service.proofs.RestoreRelations(ctx, tx, source)
				}
			}
		case "warm_claim":
			source, lookup := sandbox.LoadWarmClaim(ctx, tx, lease.OperationID)
			proofErr = lookup
			if proofErr == nil && source.Input.Restore.Snapshot.ID != lease.SnapshotID {
				proofErr = fmt.Errorf("warm source lease mismatch")
			}
		case "fork_group":
			group, lookup := sandbox.LoadFork(ctx, tx, lease.OperationID)
			proofErr = lookup
			if proofErr == nil && (operation.Terminal(group.Status) || group.State.Snapshot == nil || group.State.Snapshot.Snapshot.ID != lease.SnapshotID) {
				proofErr = fmt.Errorf("fork source lease mismatch")
			}
		case "lifecycle":
			source, lookup := sandbox.LoadLifecycle(ctx, tx, lease.OperationID)
			proofErr = lookup
			if proofErr == nil && (source.Input.Rollback == nil || source.Input.Command.Action != "rollback" || source.Input.Rollback.Checkpoint.SnapshotID != lease.SnapshotID) {
				proofErr = fmt.Errorf("rollback source lease mismatch")
			}
		default:
			proofErr = fmt.Errorf("unknown source lease purpose")
		}
		if proofErr != nil {
			deferred = append(deferred, types.GCItem{
				Kind:   "source_lease",
				ID:     lease.OperationID,
				Reason: "source lease lacks matching immutable dependency",
			})
		}
	}
	versions, err := machine.VersionRoots(ctx, tx)
	if err != nil {
		return nil, err
	}
	knownBackings := map[string]bool{}
	for _, id := range versions {
		source, lookup := machine.LoadResourceStep(ctx, tx, id)
		if lookup != nil {
			deferred = append(deferred, types.GCItem{
				Kind:   "version",
				ID:     id,
				Reason: "resource version lacks original step",
			})
			continue
		}
		if !source.State.BackingReady {
			continue
		}
		if source.Input.Restore == nil {
			deferred = append(deferred, types.GCItem{
				Kind:   "backing",
				ID:     id,
				Reason: "backing lacks original source",
			})
			continue
		}
		path := filepath.Join(source.Input.Resource.RestoreDir, "memory-ranges")
		if knownBackings[path] {
			deferred = append(deferred, types.GCItem{
				Kind:   "backing",
				ID:     id,
				Reason: "ambiguous physical backing root",
			})
		}
		knownBackings[path] = true
		if err = service.proofs.RestoreRelations(ctx, tx, source); err != nil {
			deferred = append(deferred, types.GCItem{
				Kind:   "backing",
				ID:     id,
				Reason: "backing responsibility or original material proof inconsistent",
			})
		}
	}
	paths, err := machine.BackingRoots(ctx, tx)
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		if !knownBackings[path] {
			deferred = append(deferred, types.GCItem{
				Kind:   "backing",
				ID:     path,
				Reason: "physical backing lacks known resource generation",
			})
		}
	}
	return deferred, ctx.Err()
}
