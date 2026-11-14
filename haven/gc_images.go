package haven

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
)

func loadImageCache(ctx context.Context, q queryer, builder, diff string) (imageCacheRecord, error) {
	var record imageCacheRecord
	var body []byte
	err := q.QueryRowContext(ctx, "SELECT body FROM image_caches WHERE builder=? AND diff_id=?", builder, diff).Scan(&body)
	if err != nil {
		return record, err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return record, err
	}
	if record.Witness.Cache.Builder != builder || record.Witness.Cache.DiffID != diff || record.SourceOperation == "" {
		return record, fmt.Errorf("cache catalog ownership inconsistent")
	}
	source, err := loadImageImport(ctx, q, record.SourceOperation)
	if err != nil {
		return record, err
	}
	matched := false
	for _, layer := range source.State.Layers {
		if layer.Cache != nil && reflect.DeepEqual(*layer.Cache, record.Witness) {
			matched = true
			break
		}
	}
	if !matched {
		return record, fmt.Errorf("cache catalog lacks original returned witness")
	}
	var count int
	if err = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", "cache:"+builder+":"+diff, record.Witness.PhysicalBytes).Scan(&count); err != nil {
		return record, err
	}
	if count != 1 {
		return record, fmt.Errorf("cache physical charge missing")
	}
	return record, nil
}

func imageCacheLive(ctx context.Context, q rowsQueryer, witness imageimport.ImageCacheWitness) (bool, error) {
	rows, err := q.QueryContext(ctx, "SELECT body FROM images")
	if err != nil {
		return false, err
	}
	var manifests []host.ImageManifest
	for rows.Next() {
		var payload []byte
		var manifest host.ImageManifest
		if err = rows.Scan(&payload); err != nil {
			break
		}
		if err = json.Unmarshal(payload, &manifest); err != nil {
			break
		}
		manifests = append(manifests, manifest)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return false, err
	}
	for _, manifest := range manifests {
		switch manifest.Layout {
		case host.LayeredEROFSLayout:
			if manifest.Builder == witness.Cache.Builder {
				if slices.Contains(manifest.DiffIDs, witness.Cache.DiffID) {
					return true, nil
				}
			}
		case host.MergedOCIEROFSLayout, "merged-erofs-v1":
		default:
			return false, fmt.Errorf("unknown published image root format")
		}
	}
	rows, err = q.QueryContext(ctx, "SELECT operation_id FROM operation_steps WHERE step_key='image_import'")
	if err != nil {
		return false, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return false, err
	}
	for _, id := range ids {
		original, err := loadImageImport(ctx, q, id)
		if err != nil {
			return false, err
		}
		if original.Input.Plan == nil || original.Input.Options.Materialization != host.LayeredEROFSLayout || original.Input.Plan.Builder != witness.Cache.Builder {
			continue
		}
		rooted := !terminal(original.Status)
		if original.Status == "failed" {
			var count int
			if err = q.QueryRowContext(ctx, "SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND resource_kind='image_import' AND status='pending'", "image_import:"+id).Scan(&count); err != nil {
				return false, err
			}
			rooted = count == 1
		}
		if rooted {
			for _, layer := range original.Input.Plan.Layers {
				if layer.DiffID == witness.Cache.DiffID {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func imageWorkCleanupEligible(source imageImportStep) error {
	if !terminal(source.Status) || source.Input.Plan == nil || source.State.Boundary == nil {
		return fmt.Errorf("import lacks terminal prepared-work proof")
	}
	if source.Status == "failed" && source.State.FailedPhase == "layer_intent" && source.Input.Options.Materialization == host.LayeredEROFSLayout {
		index := len(source.State.Layers)
		if index >= len(source.Input.Plan.Layers) || source.Input.Plan.Layers[index].Cached == nil {
			return fmt.Errorf("unwitnessed layer-cache effect retains import responsibility")
		}
	}
	return nil
}

func (service *Service) imageGCInventory(ctx context.Context) ([]GCRoot, []GCItem, []GCItem, bool, error) {
	var roots []GCRoot
	var candidates, deferred []GCItem
	blocked := false
	rows, err := service.store.db.QueryContext(ctx, "SELECT operation_id FROM operation_steps WHERE step_key='image_import' ORDER BY rowid")
	if err != nil {
		return nil, nil, nil, false, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, nil, nil, false, err
	}
	for _, id := range ids {
		source, err := loadImageImport(ctx, service.store.db, id)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if source.Input.Existing != nil {
			continue
		}
		var pending int
		if err = service.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND resource_kind='image_import' AND status='pending'", "image_import:"+id).Scan(&pending); err != nil {
			return nil, nil, nil, false, err
		}
		if !terminal(source.Status) || pending != 0 {
			roots = append(roots, GCRoot{
				Kind: "import_work",
				ID:   id,
				Path: source.Input.Work,
			})
		}
		if pending == 1 {
			if err = imageWorkCleanupEligible(source); err != nil {
				deferred = append(deferred, GCItem{
					Kind:   "image_work",
					ID:     id,
					Reason: "import effect lacks complete historical ownership proof",
				})
			} else {
				candidates = append(candidates, GCItem{
					Kind:  "image_work",
					ID:    id,
					Proof: source.Hash,
				})
			}
		}
	}
	rows, err = service.store.db.QueryContext(ctx, "SELECT builder,diff_id FROM image_caches ORDER BY builder,diff_id")
	if err != nil {
		return nil, nil, nil, false, err
	}
	var keys [][2]string
	for rows.Next() {
		var key [2]string
		if err = rows.Scan(&key[0], &key[1]); err != nil {
			break
		}
		keys = append(keys, key)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, nil, nil, false, err
	}
	for _, key := range keys {
		record, err := loadImageCache(ctx, service.store.db, key[0], key[1])
		if err != nil {
			return nil, nil, nil, false, err
		}
		if err = imageimport.VerifyCacheWitness(ctx, service.store.directory, record.Witness); err != nil {
			blocked = true
			deferred = append(deferred, GCItem{
				Kind:   "image_cache",
				ID:     key[0] + ":" + key[1],
				Reason: "cache historical ownership verification failed",
			})
			continue
		}
		var held int
		if err = service.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", "cache:"+key[0]+":"+key[1], record.Witness.PhysicalBytes).Scan(&held); err != nil {
			return nil, nil, nil, false, err
		}
		if held != 1 {
			blocked = true
			deferred = append(deferred, GCItem{
				Kind:   "image_cache",
				ID:     key[0] + ":" + key[1],
				Reason: "cache physical responsibility missing",
			})
			continue
		}
		live, err := imageCacheLive(ctx, service.store.db, record.Witness)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if live {
			roots = append(roots, GCRoot{
				Kind: "image_cache",
				ID:   key[0] + ":" + key[1],
				Path: record.Witness.Path,
			})
		} else {
			candidates = append(candidates, GCItem{
				Kind:         "image_cache",
				ID:           key[0][7:] + ":" + key[1][7:],
				Proof:        digest(encode(record)),
				CacheBuilder: key[0],
				CacheDiffID:  key[1],
			})
		}
	}
	return roots, candidates, deferred, blocked, nil
}
