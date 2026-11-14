package haven

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

type (
	GCOptions = api.GCOptions
	GCItem    = api.GCItem
	GCRoot    = api.GCRoot
	GCReport  = api.GCReport
)

func (service *Service) gcInventory(ctx context.Context) (GCReport, error) {
	report := GCReport{
		StartedAt:  time.Now().UTC(),
		Roots:      []GCRoot{},
		Candidates: []GCItem{},
		Collected:  []GCItem{},
		Deferred:   []GCItem{},
	}
	graphDeferred, graphErr := service.gcGraphFence(ctx)
	if graphErr != nil {
		return report, graphErr
	}
	report.Deferred = append(report.Deferred, graphDeferred...)
	report.Blocked = len(graphDeferred) != 0
	known := map[string]bool{}
	addRoot := func(kind, id, path string) {
		report.Roots = append(report.Roots, GCRoot{
			Kind: kind,
			ID:   id,
			Path: path,
		})
		if path != "" {
			known[filepath.Clean(path)] = true
		}
	}
	rows, err := service.store.db.QueryContext(ctx, "SELECT digest,body FROM images ORDER BY digest")
	if err != nil {
		return report, err
	}
	type record struct {
		id   string
		body []byte
	}
	var images []record
	for rows.Next() {
		var item record
		if err = rows.Scan(&item.id, &item.body); err != nil {
			break
		}
		images = append(images, item)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return report, err
	}
	for _, item := range images {
		image, err := loadImage(ctx, service.store.db, item.id)
		if err != nil {
			return report, err
		}
		directory := filepath.Join(service.store.directory, "images", item.id[7:])
		if err = verifyImageFiles(image, directory); err != nil {
			report.Blocked = true
			report.Deferred = append(report.Deferred, GCItem{
				Kind:   "image",
				ID:     item.id,
				Reason: "published image root could not be verified",
			})
		}
		addRoot("image", item.id, directory)
		for _, artifact := range image.Artifacts {
			path := filepath.Join(service.store.directory, artifact.Path)
			addRoot("image_artifact", artifact.SHA256, path)
			if artifact.Role == "layer" {
				known[filepath.Dir(path)] = true
			}
		}
	}
	rows, err = service.store.db.QueryContext(ctx, "SELECT id,body FROM vms ORDER BY id")
	if err != nil {
		return report, err
	}
	var vms []record
	for rows.Next() {
		var item record
		if err = rows.Scan(&item.id, &item.body); err != nil {
			break
		}
		vms = append(vms, item)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return report, err
	}
	for _, item := range vms {
		var vm vmRecord
		if err = json.Unmarshal(item.body, &vm); err != nil {
			return report, err
		}
		if vm.Resource.VMID != item.id || vm.Resource.Dir != filepath.Join(service.store.directory, "vms", item.id) {
			report.Blocked = true
			report.Deferred = append(report.Deferred, GCItem{
				Kind:   "vm",
				ID:     item.id,
				Reason: "current VM root identity inconsistent",
			})
		}
		addRoot("vm", item.id, vm.Resource.Dir)
	}
	rows, err = service.store.db.QueryContext(ctx, "SELECT operation_id,input_hash,input,status,action FROM operation_steps ORDER BY rowid")
	if err != nil {
		return report, err
	}
	type control struct {
		id, hash, status, action string
		input                    []byte
	}
	var controls []control
	for rows.Next() {
		var item control
		if err = rows.Scan(&item.id, &item.hash, &item.input, &item.status, &item.action); err != nil {
			break
		}
		controls = append(controls, item)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return report, err
	}
	for _, item := range controls {
		if digest(item.input) != item.hash {
			report.Blocked = true
			report.Deferred = append(report.Deferred, GCItem{
				Kind:   "step",
				ID:     item.id,
				Reason: "immutable step checksum inconsistent",
			})
		}
		if !terminal(item.status) && item.status != "blocked" {
			addRoot("unresolved_step", item.id, "")
		}
	}
	rows, err = service.store.db.QueryContext(ctx, "SELECT operation_id,status FROM operation_steps WHERE step_key='supply' AND status='failed' ORDER BY operation_id")
	if err != nil {
		return report, err
	}
	var failed []string
	for rows.Next() {
		var id, status string
		if err = rows.Scan(&id, &status); err != nil {
			break
		}
		failed = append(failed, id)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return report, err
	}
	for _, id := range failed {
		original, err := loadSupply(ctx, service.store.db, id)
		if err != nil {
			return report, err
		}
		var pending int
		if err = service.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind='supply' AND status='pending'", "supply:"+id, id).Scan(&pending); err != nil {
			return report, err
		}
		if pending == 1 {
			report.Candidates = append(report.Candidates, GCItem{
				Kind:     "supply",
				ID:       id,
				TenantID: original.Input.Template.TenantID,
				Proof:    original.Hash,
			})
		}
	}
	rows, err = service.store.db.QueryContext(ctx, "SELECT operation_id FROM pool_slots WHERE status='ready' ORDER BY operation_id")
	if err != nil {
		return report, err
	}
	var pools []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		pools = append(pools, id)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return report, err
	}
	for _, id := range pools {
		pool, _, err := loadPoolReservation(ctx, service.store.db, id)
		if err != nil {
			return report, err
		}
		current, err := currentTemplate(ctx, service.store.db, pool.Template.ID)
		if err != nil {
			return report, err
		}
		if current == nil || current.Version != pool.Template.Version || current.TargetWarm == 0 {
			original, err := loadSupply(ctx, service.store.db, id)
			if err != nil {
				return report, err
			}
			report.Candidates = append(report.Candidates, GCItem{
				Kind:     "supply",
				ID:       id,
				TenantID: pool.Template.TenantID,
				Proof:    original.Hash,
			})
		}
	}
	rows, err = service.store.db.QueryContext(ctx, "SELECT id,body,published,deleting FROM snapshots ORDER BY id")
	if err != nil {
		return report, err
	}
	type snapshotEntry struct {
		id                  string
		body                []byte
		published, deleting int
	}
	var snapshots []snapshotEntry
	for rows.Next() {
		var item snapshotEntry
		if err = rows.Scan(&item.id, &item.body, &item.published, &item.deleting); err != nil {
			break
		}
		snapshots = append(snapshots, item)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return report, err
	}
	for _, item := range snapshots {
		var snapshot snapshotRecord
		if err = json.Unmarshal(item.body, &snapshot); err != nil {
			return report, err
		}
		addRoot("snapshot", item.id, snapshot.Dir)
		if item.published != 1 || item.deleting != 0 {
			continue
		}
		candidate, deferReason, err := service.snapshotGCCandidate(ctx, snapshot)
		if err != nil {
			return report, err
		}
		if candidate {
			report.Candidates = append(report.Candidates, GCItem{
				Kind:     "snapshot",
				ID:       item.id,
				TenantID: snapshot.TenantID,
				Proof:    digest(item.body),
			})
		} else if deferReason != "" {
			report.Deferred = append(report.Deferred, GCItem{
				Kind:   "snapshot",
				ID:     item.id,
				Reason: deferReason,
			})
		}
	}
	imageRoots, imageCandidates, imageDeferred, imageBlocked, err := service.imageGCInventory(ctx)
	if err != nil {
		return report, err
	}
	for _, root := range imageRoots {
		addRoot(root.Kind, root.ID, root.Path)
	}
	report.Candidates = append(report.Candidates, imageCandidates...)
	for _, candidate := range imageCandidates {
		if candidate.Kind == "image_cache" {
			known[filepath.Join(service.store.directory, filepath.Dir(host.LayerRelativePath(candidate.CacheBuilder, candidate.CacheDiffID)))] = true
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
		directory := filepath.Join(service.store.directory, base)
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
					report.Deferred = append(report.Deferred, GCItem{
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
						report.Deferred = append(report.Deferred, GCItem{
							Kind:   "unknown_artifact",
							ID:     candidate,
							Reason: "no exact durable cache collection authority",
						})
					}
				}
				continue
			}
			report.Deferred = append(report.Deferred, GCItem{
				Kind:   "unknown_artifact",
				ID:     path,
				Reason: "no exact durable collection authority",
			})
		}
	}
	if _, err = os.Lstat(filepath.Join(service.store.directory, "RECOVERY_REVIEWED.json")); err == nil {
		report.Blocked = true
		report.Deferred = append(report.Deferred, GCItem{
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
