package image

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

type imageImportInput struct {
	RequestTenantID string                         `json:"request_tenant_id"`
	Options         imageimport.ImageImportOptions `json:"options"`
	Plan            *imageimport.ImageImportPlan   `json:"plan,omitempty"`
	Existing        *host.ImageManifest            `json:"existing,omitempty"`
	Work            string                         `json:"work"`
	Target          string                         `json:"target"`
	Reservation     int64                          `json:"reservation"`
}
type imageImportState struct {
	Phase       string                           `json:"phase"`
	FailedPhase string                           `json:"failed_phase,omitempty"`
	Boundary    *imageimport.ImageImportBoundary `json:"boundary,omitempty"`
	Layers      []imageimport.ImageLayerProof    `json:"layers,omitempty"`
	Proof       *imageimport.ImageImportProof    `json:"proof,omitempty"`
	Error       string                           `json:"error,omitempty"`
}
type imageImportStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       imageImportInput
	State       imageImportState
}

func loadImageImport(ctx context.Context, q store.DBTX, id string) (imageImportStep, error) {
	var step imageImportStep
	err := store.ReadSnapshot(ctx, q, func(tx *sql.Tx) error {
		var err error
		step, err = readImageImport(ctx, tx, id)
		return err
	})
	return step, err
}

func readImageImport(ctx context.Context, q *sql.Tx, id string) (imageImportStep, error) {
	step := imageImportStep{
		OperationID: id,
	}
	record, err := dbgen.New(q).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "image_import",
	})
	if err != nil {
		return step, err
	}
	payload := record.Input
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("image import input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	queries := dbgen.New(q)
	progress, err := queries.ReadImportProgress(ctx, id)
	if err != nil {
		return step, fmt.Errorf("cannot read import progress for %s: %v", id, err)
	}
	step.State = imageImportState{
		Phase:       record.Phase.String,
		FailedPhase: progress.FailedPhase,
		Error:       record.StepError,
	}
	step.State.Boundary, err = readPreparation(ctx, queries, id)
	if err != nil {
		return step, err
	}
	if progress.OutputManifestHash.Valid {
		proof := &imageimport.ImageImportProof{
			ManifestSHA256: progress.OutputFileHash.String,
			Synced:         progress.OutputSynced.Bool,
		}
		if err = readManifest(ctx, queries, progress.OutputManifestHash.String, &proof.Manifest); err != nil {
			return step, fmt.Errorf("image output manifest unavailable: %v", err)
		}
		step.State.Proof = proof
	}
	rows, err := queries.ReadImportLayers(ctx, id)
	if err != nil {
		return step, err
	}
	for index, row := range rows {
		if row.Ordinal != int64(index) {
			return step, fmt.Errorf("image import layer sequence changed")
		}
		layer := imageimport.ImageLayerProof{
			Bytes:   row.TarBytes,
			Entries: row.Entries,
		}
		if row.CacheHash.Valid {
			w, err := queries.GetCacheWitness(ctx, row.CacheHash.String)
			if err != nil {
				return step, fmt.Errorf("image layer cache unavailable: %v", err)
			}
			cache, err := cacheWitness(w)
			if err != nil {
				return step, err
			}
			layer.Cache = &cache
		}
		if row.ArtifactHash.Valid {
			w, err := queries.GetArtifactValue(ctx, row.ArtifactHash.String)
			if err != nil {
				return step, fmt.Errorf("image layer artifact unavailable: %v", err)
			}
			a, err := artifactValue(w)
			if err != nil {
				return step, err
			}
			layer.Artifact = &a
		}
		step.State.Layers = append(step.State.Layers, layer)
	}
	if action != "image_import" || candidate != step.Input.Options.Digest || expected != "" {
		return step, fmt.Errorf("image import step identity changed")
	}
	if step.Input.Plan != nil && !reflect.DeepEqual(step.Input.Plan.Options, step.Input.Options) {
		return step, fmt.Errorf("image import normalized source changed")
	}
	return step, nil
}

func transitionImport(ctx context.Context, tx *sql.Tx, step imageImportStep, previousPhase, status string) error {
	current, err := readImageImport(ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	n := len(current.State.Layers)
	next := step.State
	if len(next.Layers) < n || (n > 0 && !reflect.DeepEqual(current.State.Layers, next.Layers[:n])) {
		return fmt.Errorf("image import layer history changed")
	}
	if err = operation.AdvancePhase(ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "image_import",
		InputHash:      step.Hash,
		PreviousStatus: step.Status,
		PreviousPhase:  previousPhase,
		NextStatus:     status,
		NextPhase:      next.Phase,
	}); err != nil {
		return err
	}
	queries := dbgen.New(tx)
	if err = recordPreparation(ctx, queries, step.OperationID, current.State.Boundary, next.Boundary); err != nil {
		return err
	}
	params := dbgen.UpdateImportProgressParams{
		OperationID: step.OperationID,
		FailedPhase: next.FailedPhase,
	}
	if current.State.Proof != nil && !reflect.DeepEqual(current.State.Proof, next.Proof) {
		return fmt.Errorf("image output proof changed")
	}
	if p := next.Proof; p != nil {
		hash := operation.Digest(operation.Encode(p.Manifest))
		if current.State.Proof == nil {
			if err = retainManifest(ctx, queries, hash, p.Manifest); err != nil {
				return err
			}
		}
		params.OutputManifestHash = sql.NullString{
			String: hash,
			Valid:  true,
		}
		params.OutputFileHash = sql.NullString{
			String: p.ManifestSHA256,
			Valid:  true,
		}
		params.OutputSynced = sql.NullBool{
			Bool:  p.Synced,
			Valid: true,
		}
	}
	if err = store.RequireOne(queries.UpdateImportProgress(ctx, params)); err != nil {
		return err
	}
	for index := n; index < len(next.Layers); index++ {
		layer := next.Layers[index]
		params := dbgen.AppendImportLayerParams{
			OperationID: step.OperationID,
			Ordinal:     int64(index),
			TarBytes:    layer.Bytes,
			Entries:     layer.Entries,
		}
		if layer.Cache != nil {
			hash, err := retainCacheWitness(ctx, queries, layer.Cache)
			if err != nil {
				return err
			}
			params.CacheHash = sql.NullString{
				String: hash,
				Valid:  true,
			}
		}
		if layer.Artifact != nil {
			hash, err := retainArtifact(ctx, queries, *layer.Artifact)
			if err != nil {
				return err
			}
			params.ArtifactHash = sql.NullString{
				String: hash,
				Valid:  true,
			}
		}
		if err = store.RequireOne(queries.AppendImportLayer(ctx, params)); err != nil {
			return err
		}
	}
	return store.RequireOne(queries.SetImportError(ctx, dbgen.SetImportErrorParams{
		OperationID: step.OperationID,
		StepError:   next.Error,
	}))
}

func (i *Images) saveImageImport(step *imageImportStep, phase string) error {
	previousPhase := step.State.Phase
	step.State.Phase = phase
	i.storeWriteMu.Lock()
	defer i.storeWriteMu.Unlock()
	tx, err := i.database.BeginTx(i.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	operations := operation.New(tx)
	if err = transitionImport(i.ctx, tx, *step, previousPhase, "running"); err != nil {
		return err
	}
	if err = operations.MarkRunning(i.ctx, step.OperationID); err != nil {
		return err
	}
	catalog := dbgen.New(tx)
	for _, layer := range step.State.Layers {
		if layer.Cache == nil {
			continue
		}
		cache := layer.Cache
		key := CacheKey{
			Builder: cache.Cache.Builder,
			DiffID:  cache.Cache.DiffID,
		}
		record, lookup := LoadCacheRecord(i.ctx, tx, key)
		err = lookup
		if err == nil {
			if !reflect.DeepEqual(record.Witness, *cache) {
				return fmt.Errorf("%w: image cache immutable ownership changed", errs.ErrManualRecovery)
			}
			owned, err := quota.CountMatchingReservation(i.ctx, tx, quota.Reservation{
				Owner: "cache:" + cache.Cache.Builder + ":" + cache.Cache.DiffID,
				Bytes: cache.PhysicalBytes,
			})
			if err != nil {
				return err
			}
			if owned != 1 {
				return fmt.Errorf("%w: image cache physical charge missing", errs.ErrManualRecovery)
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			if cache.PhysicalBytes <= 0 {
				return fmt.Errorf("%w: image cache lacks physical charge", errs.ErrIncompatible)
			}
			owner := "cache:" + cache.Cache.Builder + ":" + cache.Cache.DiffID
			if err = quota.DeductReservationBytes(i.ctx, tx, "import:"+step.OperationID, cache.PhysicalBytes); err != nil {
				return err
			}
			if err = quota.Reserve(i.ctx, tx, quota.Reservation{
				Owner: owner,
				Bytes: cache.PhysicalBytes,
			}); err != nil {
				return err
			}
			if err = store.RequireOne(catalog.InsertCache(i.ctx, dbgen.InsertCacheParams{
				Builder:           key.Builder,
				DiffID:            key.DiffID,
				SourceOperationID: step.OperationID,
				WitnessHash:       operation.Digest(operation.Encode(cache)),
			})); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.Status = "running"
	return i.checkpoint()
}

func importRemainingCharge(ctx context.Context, q store.DBTX, source imageImportStep) (int64, error) {
	funded, err := fundedCaches(ctx, q, source.OperationID)
	if err != nil {
		return 0, err
	}
	remaining := source.Input.Reservation
	for _, record := range funded {
		found := false
		for _, layer := range source.State.Layers {
			if layer.Cache != nil && reflect.DeepEqual(*layer.Cache, record.Witness) {
				found = true
				break
			}
		}
		if !found || record.Witness.PhysicalBytes <= 0 || record.Witness.PhysicalBytes > remaining {
			return 0, fmt.Errorf("import cache funding lacks original returned witness")
		}
		remaining -= record.Witness.PhysicalBytes
	}
	for _, layer := range source.State.Layers {
		if layer.Cache == nil {
			continue
		}
		record, err := loadImageCache(ctx, q, layer.Cache.Cache.Builder, layer.Cache.Cache.DiffID)
		if err != nil {
			return 0, err
		}
		if !reflect.DeepEqual(record.Witness, *layer.Cache) {
			return 0, fmt.Errorf("import cache historical ownership changed")
		}
		count, err := quota.CountMatchingReservation(ctx, q, quota.Reservation{
			Owner: "cache:" + record.Witness.Cache.Builder + ":" + record.Witness.Cache.DiffID,
			Bytes: record.Witness.PhysicalBytes,
		})
		if err != nil {
			return 0, err
		}
		if count != 1 {
			return 0, fmt.Errorf("cache independent physical charge missing")
		}
	}
	return remaining, nil
}
