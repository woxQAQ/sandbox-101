package image

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

func loadImageCache(ctx context.Context, q store.DBTX, builder, diff string) (Cache, error) {
	record, err := LoadCacheRecord(ctx, q, CacheKey{
		Builder: builder,
		DiffID:  diff,
	})
	if err != nil {
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
	count, err := quota.New(q).CountMatchingReservation(ctx, quota.Reservation{
		Owner: "cache:" + builder + ":" + diff,
		Bytes: record.Witness.PhysicalBytes,
	})
	if err != nil {
		return record, err
	}
	if count != 1 {
		return record, fmt.Errorf("cache physical charge missing")
	}
	return record, nil
}

func imageCacheLive(ctx context.Context, q store.DBTX, witness imageimport.ImageCacheWitness) (bool, error) {
	manifests, err := loadManifests(ctx, q)
	if err != nil {
		return false, err
	}
	for _, manifest := range manifests {
		switch manifest.Layout {
		case host.LayeredEROFSLayout:
			if manifest.Builder == witness.Cache.Builder && slices.Contains(manifest.DiffIDs, witness.Cache.DiffID) {
				return true, nil
			}
		case host.MergedOCIEROFSLayout, "merged-erofs-v1":
		default:
			return false, fmt.Errorf("unknown published image root format")
		}
	}

	ids, err := dbgen.New(q).ListStepOperations(ctx, "image_import")
	if err != nil {
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
		rooted := !operation.Terminal(original.Status)
		if original.Status == "failed" {
			count, err := dbgen.New(q).CountPendingCleanupID(ctx, dbgen.CountPendingCleanupIDParams{
				ID:           "image_import:" + id,
				ResourceKind: "image_import",
			})
			if err != nil {
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
	if !operation.Terminal(source.Status) || source.Input.Plan == nil || source.State.Boundary == nil {
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

func (i *Images) Inventory(ctx context.Context) ([]types.GCRoot, []types.GCItem, []types.GCItem, bool, error) {
	var roots []types.GCRoot
	var candidates, deferred []types.GCItem
	blocked := false

	ids, err := dbgen.New(i.database).ListStepAdmissions(ctx, "image_import")
	if err != nil {
		return nil, nil, nil, false, err
	}
	for _, id := range ids {
		source, err := loadImageImport(ctx, i.database, id)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if source.Input.Existing != nil {
			continue
		}
		pending, err := dbgen.New(i.database).CountPendingCleanupID(ctx, dbgen.CountPendingCleanupIDParams{
			ID:           "image_import:" + id,
			ResourceKind: "image_import",
		})
		if err != nil {
			return nil, nil, nil, false, err
		}
		if !operation.Terminal(source.Status) || pending != 0 {
			roots = append(roots, types.GCRoot{
				Kind: "import_work",
				ID:   id,
				Path: source.Input.Work,
			})
		}
		if pending == 1 {
			if err = imageWorkCleanupEligible(source); err != nil {
				deferred = append(deferred, types.GCItem{
					Kind:   "image_work",
					ID:     id,
					Reason: "import effect lacks complete historical ownership proof",
				})
			} else {
				candidates = append(candidates, types.GCItem{
					Kind:  "image_work",
					ID:    id,
					Proof: source.Hash,
				})
			}
		}
	}
	keys, err := dbgen.New(i.database).ListCacheKeys(ctx)
	if err != nil {
		return nil, nil, nil, false, err
	}
	for _, key := range keys {
		record, err := loadImageCache(ctx, i.database, key.Builder, key.DiffID)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if err = imageimport.VerifyCacheWitness(ctx, i.directory, record.Witness); err != nil {
			blocked = true
			deferred = append(deferred, types.GCItem{
				Kind:   "image_cache",
				ID:     key.Builder + ":" + key.DiffID,
				Reason: "cache historical ownership verification failed",
			})
			continue
		}
		held, err := quota.New(i.database).CountMatchingReservation(ctx, quota.Reservation{
			Owner: "cache:" + key.Builder + ":" + key.DiffID,
			Bytes: record.Witness.PhysicalBytes,
		})
		if err != nil {
			return nil, nil, nil, false, err
		}
		if held != 1 {
			blocked = true
			deferred = append(deferred, types.GCItem{
				Kind:   "image_cache",
				ID:     key.Builder + ":" + key.DiffID,
				Reason: "cache physical responsibility missing",
			})
			continue
		}
		live, err := imageCacheLive(ctx, i.database, record.Witness)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if live {
			roots = append(roots, types.GCRoot{
				Kind: "image_cache",
				ID:   key.Builder + ":" + key.DiffID,
				Path: record.Witness.Path,
			})
		} else {
			candidates = append(candidates, types.GCItem{
				Kind:         "image_cache",
				ID:           key.Builder[7:] + ":" + key.DiffID[7:],
				Proof:        operation.Digest(operation.Encode(record)),
				CacheBuilder: key.Builder,
				CacheDiffID:  key.DiffID,
			})
		}
	}
	return roots, candidates, deferred, blocked, nil
}
