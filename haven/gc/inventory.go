package gc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/woxQAQ/haven/gc/internal/dbgen"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

func (c *Collector) inventory(ctx context.Context) (types.GCReport, error) {
	report := types.GCReport{
		StartedAt:  time.Now().UTC(),
		Roots:      []types.GCRoot{},
		Candidates: []types.GCItem{},
		Collected:  []types.GCItem{},
		Deferred:   []types.GCItem{},
	}
	graphDeferred, graphErr := c.graphFence(ctx)
	if graphErr != nil {
		return report, graphErr
	}
	report.Deferred = append(report.Deferred, graphDeferred...)
	report.Blocked = len(graphDeferred) != 0
	known := map[string]bool{}
	addRoot := func(kind, id, path string) {
		report.Roots = append(report.Roots, types.GCRoot{
			Kind: kind,
			ID:   id,
			Path: path,
		})
		if path != "" {
			known[filepath.Clean(path)] = true
		}
	}
	digests, err := image.PublishedDigests(ctx, c.store.Database)
	if err != nil {
		return report, err
	}
	for _, imageDigest := range digests {
		manifest, err := image.LoadManifest(ctx, c.store.Database, imageDigest)
		if err != nil {
			return report, err
		}
		directory := filepath.Join(c.store.Directory, "images", imageDigest[7:])
		if err = image.VerifyFiles(manifest, directory); err != nil {
			report.Blocked = true
			report.Deferred = append(report.Deferred, types.GCItem{
				Kind:   "image",
				ID:     imageDigest,
				Reason: "published image root could not be verified",
			})
		}
		addRoot("image", imageDigest, directory)
		for _, artifact := range manifest.Artifacts {
			path := filepath.Join(c.store.Directory, artifact.Path)
			addRoot("image_artifact", artifact.SHA256, path)
			if artifact.Role == "layer" {
				known[filepath.Dir(path)] = true
			}
		}
	}
	entries, inventoryErr := machine.VMRoots(ctx, c.store.Database, c.store.Directory)
	for _, item := range entries {
		if !item.Consistent {
			report.Blocked = true
			report.Deferred = append(report.Deferred, types.GCItem{
				Kind:   "vm",
				ID:     item.ID,
				Reason: "current VM root identity inconsistent",
			})
		}
		addRoot("vm", item.ID, item.Directory)
	}
	if inventoryErr != nil {
		return report, inventoryErr
	}
	queries := dbgen.New(c.store.Database)
	controls, err := queries.ListGCStepInputs(ctx)
	if err != nil {
		return report, err
	}
	for _, item := range controls {
		if operation.Digest(item.Input) != item.InputHash {
			report.Blocked = true
			report.Deferred = append(report.Deferred, types.GCItem{
				Kind:   "step",
				ID:     item.OperationID,
				Reason: "immutable step checksum inconsistent",
			})
		}
		if !operation.Terminal(item.Status) && item.Status != "blocked" {
			addRoot("unresolved_step", item.OperationID, "")
		}
	}
	failed, err := queries.ListFailedSupplySteps(ctx)
	if err != nil {
		return report, err
	}
	for _, failure := range failed {
		id := failure.OperationID
		original, err := supply.LoadSupply(ctx, c.store.Database, id)
		if err != nil {
			return report, err
		}
		pending, err := dbgen.New(c.store.Database).CountPendingOwnedCleanup(ctx, dbgen.CountPendingOwnedCleanupParams{
			ID:           "supply:" + id,
			OperationID:  id,
			ResourceKind: "supply",
		})
		if err != nil {
			return report, err
		}
		if pending == 1 {
			report.Candidates = append(report.Candidates, types.GCItem{
				Kind:     "supply",
				ID:       id,
				TenantID: original.Input.Template.TenantID,
				Proof:    original.Hash,
			})
		}
	}
	warmCandidates, err := supply.ObsoleteWarmCandidates(ctx, c.store.Database)
	report.Candidates = append(report.Candidates, warmCandidates...)
	if err != nil {
		return report, err
	}
	publications, inventoryErr := machine.SnapshotRoots(ctx, c.store.Database)
	for _, item := range publications {
		record := item.Record
		addRoot("snapshot", item.ID, record.Dir)
		if !item.Published || item.Deleting {
			continue
		}
		candidate, deferReason, err := c.snapshotCandidate(ctx, record)
		if err != nil {
			return report, err
		}
		if candidate {
			report.Candidates = append(report.Candidates, types.GCItem{
				Kind:     "snapshot",
				ID:       item.ID,
				TenantID: record.TenantID,
				Proof:    item.Hash,
			})
		} else if deferReason != "" {
			report.Deferred = append(report.Deferred, types.GCItem{
				Kind:   "snapshot",
				ID:     item.ID,
				Reason: deferReason,
			})
		}
	}
	if inventoryErr != nil {
		return report, inventoryErr
	}
	imageRoots, imageCandidates, imageDeferred, imageBlocked, err := c.images.Inventory(ctx)
	if err != nil {
		return report, err
	}
	for _, root := range imageRoots {
		addRoot(root.Kind, root.ID, root.Path)
	}
	report.Candidates = append(report.Candidates, imageCandidates...)
	for _, candidate := range imageCandidates {
		if candidate.Kind == "image_cache" {
			known[filepath.Join(c.store.Directory, filepath.Dir(host.LayerRelativePath(candidate.CacheBuilder, candidate.CacheDiffID)))] = true
		}
	}
	report.Deferred = append(report.Deferred, imageDeferred...)
	report.Blocked = report.Blocked || imageBlocked
	for _, base := range []string{
		"vms",
		"snapshots",
		"images",
		"imports",
		"layer-cache",
	} {
		directory := filepath.Join(c.store.Directory, base)
		entries, err := os.ReadDir(directory)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return report, err
		}
		for _, entry := range entries {
			path := filepath.Join(directory, entry.Name())
			if known[path] {
				continue
			}
			if base == "layer-cache" {
				// Listing is diagnostic only. Unknown cache children never acquire
				// collection authority from their names or materialized contents.
				dir, openErr := loom.OpenCaptureDir(path)
				if openErr != nil {
					report.Deferred = append(report.Deferred, types.GCItem{
						Kind:   "unknown_artifact",
						ID:     path,
						Reason: "cache builder directory could not be safely inspected",
					})
					continue
				}
				children, readErr := dir.ReadDir(-1)
				closeErr := dir.Close()
				if err = errors.Join(readErr, closeErr); err != nil {
					return report, err
				}
				for _, child := range children {
					candidate := filepath.Join(path, child.Name())
					if !known[candidate] {
						report.Deferred = append(report.Deferred, types.GCItem{
							Kind:   "unknown_artifact",
							ID:     candidate,
							Reason: "no exact durable cache collection authority",
						})
					}
				}
				continue
			}
			report.Deferred = append(report.Deferred, types.GCItem{
				Kind:   "unknown_artifact",
				ID:     path,
				Reason: "no exact durable collection authority",
			})
		}
	}
	if _, err = os.Lstat(filepath.Join(c.store.Directory, "RECOVERY_REVIEWED.json")); err == nil {
		report.Blocked = true
		report.Deferred = append(report.Deferred, types.GCItem{
			Kind:   "recovery",
			ID:     "reviewed",
			Reason: "review does not prove complete ownership history",
		})
	} else if !os.IsNotExist(err) {
		return report, err
	}
	sort.Slice(report.Candidates, func(i, j int) bool {
		left, right := report.Candidates[i], report.Candidates[j]
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		return left.ID < right.ID
	})
	report.FinishedAt = time.Now().UTC()
	return report, ctx.Err()
}
