package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
)

// gcGraphFence checks normalized roots before any collector can be admitted.
// Unknown relationships are retained instead of being treated as absent users.
func (service *Service) gcGraphFence(ctx context.Context) ([]GCItem, error) {
	tx, err := service.store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var deferred []GCItem
	violations, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return nil, err
	}
	invalid := violations.Next()
	err = errors.Join(violations.Err(), violations.Close())
	if err != nil {
		return nil, err
	}
	if invalid {
		deferred = append(deferred, GCItem{Kind: "metadata", Reason: "dangling normalized root reference"})
	}
	rows, err := tx.QueryContext(ctx, "SELECT resource_kind,resource_id,holder_kind,holder_id FROM resource_refs ORDER BY resource_kind,resource_id")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var kind, id, holder, owner string
		if err = rows.Scan(&kind, &id, &holder, &owner); err != nil {
			break
		}
		validKind := kind == "vm" || kind == "image" || kind == "snapshot"
		validHolder := holder == "template" || holder == "vm" || holder == "snapshot" || holder == "capture" || holder == "step" || holder == "checkpoint" || holder == "checkpoint_binding" || holder == "warm" || holder == "sleep" || holder == "backing" || holder == "sandbox" || holder == "cleanup"
		if !validKind || !validHolder || id == "" || owner == "" {
			deferred = append(deferred, GCItem{Kind: "metadata", ID: id, Reason: "unknown resource reference root"})
		}
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, "SELECT id,tenant FROM templates ORDER BY id")
	if err != nil {
		return nil, err
	}
	var templates [][2]string
	for rows.Next() {
		var pair [2]string
		if err = rows.Scan(&pair[0], &pair[1]); err != nil {
			break
		}
		templates = append(templates, pair)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	for _, pair := range templates {
		if _, err = loadOwnedTemplate(ctx, tx, pair[1], pair[0]); err != nil {
			deferred = append(deferred, GCItem{Kind: "template", ID: pair[0], Reason: "template publication root is unresolved or inconsistent"})
		}
	}
	rows, err = tx.QueryContext(ctx, "SELECT snapshot_id,operation_id,step_key FROM source_leases ORDER BY operation_id")
	if err != nil {
		return nil, err
	}
	var leases [][3]string
	for rows.Next() {
		var lease [3]string
		if err = rows.Scan(&lease[0], &lease[1], &lease[2]); err != nil {
			break
		}
		leases = append(leases, lease)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	for _, lease := range leases {
		var proofErr error
		switch lease[2] {
		case "resource":
			source, lookup := loadStep(ctx, tx, lease[1])
			proofErr = lookup
			if proofErr == nil {
				if source.Input.Restore == nil || source.Input.Restore.Snapshot.ID != lease[0] || source.State.BackingReady {
					proofErr = fmt.Errorf("source lease original step mismatch")
				} else {
					proofErr = service.restoreRelations(ctx, tx, source)
				}
			}
		case "warm_claim":
			source, lookup := loadWarmClaim(ctx, tx, lease[1])
			proofErr = lookup
			if proofErr == nil && source.Input.Restore.Snapshot.ID != lease[0] {
				proofErr = fmt.Errorf("warm source lease mismatch")
			}
		case "fork_group":
			group, lookup := loadFork(ctx, tx, lease[1])
			proofErr = lookup
			if proofErr == nil && (terminal(group.Status) || group.State.Snapshot == nil || group.State.Snapshot.Snapshot.ID != lease[0]) {
				proofErr = fmt.Errorf("fork source lease mismatch")
			}
		case "lifecycle":
			source, lookup := loadLifecycle(ctx, tx, lease[1])
			proofErr = lookup
			if proofErr == nil && (source.Input.Rollback == nil || source.Input.Command.Action != "rollback" || source.Input.Rollback.Checkpoint.SnapshotID != lease[0]) {
				proofErr = fmt.Errorf("rollback source lease mismatch")
			}
		default:
			proofErr = fmt.Errorf("unknown source lease purpose")
		}
		if proofErr != nil {
			deferred = append(deferred, GCItem{Kind: "source_lease", ID: lease[1], Reason: "source lease lacks matching immutable dependency"})
		}
	}
	rows, err = tx.QueryContext(ctx, "SELECT id FROM resource_versions ORDER BY id")
	if err != nil {
		return nil, err
	}
	var versions []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		versions = append(versions, id)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	knownBackings := map[string]bool{}
	for _, id := range versions {
		source, lookup := loadStep(ctx, tx, id)
		if lookup != nil {
			deferred = append(deferred, GCItem{Kind: "version", ID: id, Reason: "resource version lacks original step"})
			continue
		}
		if !source.State.BackingReady {
			continue
		}
		if source.Input.Restore == nil {
			deferred = append(deferred, GCItem{Kind: "backing", ID: id, Reason: "backing lacks original source"})
			continue
		}
		path := filepath.Join(source.Input.Resource.RestoreDir, "memory-ranges")
		if knownBackings[path] {
			deferred = append(deferred, GCItem{Kind: "backing", ID: id, Reason: "ambiguous physical backing root"})
		}
		knownBackings[path] = true
		if err = service.restoreRelations(ctx, tx, source); err != nil {
			deferred = append(deferred, GCItem{Kind: "backing", ID: id, Reason: "backing responsibility or original material proof inconsistent"})
		}
	}
	rows, err = tx.QueryContext(ctx, "SELECT path FROM backing ORDER BY path")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var path string
		if err = rows.Scan(&path); err != nil {
			break
		}
		if !knownBackings[path] {
			deferred = append(deferred, GCItem{Kind: "backing", ID: path, Reason: "physical backing lacks known resource generation"})
		}
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return deferred, ctx.Err()
}
