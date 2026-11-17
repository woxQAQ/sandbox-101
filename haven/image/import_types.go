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
	payload, body := record.Input, record.Body
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("image import input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "image_import" || candidate != step.Input.Options.Digest || expected != "" {
		return step, fmt.Errorf("image import step identity changed")
	}
	if step.Input.Plan != nil && !reflect.DeepEqual(step.Input.Plan.Options, step.Input.Options) {
		return step, fmt.Errorf("image import normalized source changed")
	}
	return step, nil
}

func (service *Images) saveImageImport(step *imageImportStep, phase string) error {
	step.State.Phase = phase
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).RecordLiveStepProgress(service.ctx, dbgen.RecordLiveStepProgressParams{
		OperationID: step.OperationID,
		StepKey:     "image_import",
		InputHash:   step.Hash,
		Body:        operation.Encode(step.State),
	})); err != nil {
		return err
	}
	if err = operations.Run(service.ctx, step.OperationID); err != nil {
		return err
	}
	catalog := dbgen.New(tx)
	responsibilities := quota.New(tx)
	for _, layer := range step.State.Layers {
		if layer.Cache == nil {
			continue
		}
		cache := layer.Cache
		key := CacheKey{
			Builder: cache.Cache.Builder,
			DiffID:  cache.Cache.DiffID,
		}
		record, lookup := LoadCacheRecord(service.ctx, tx,

			key)
		err = lookup
		if err == nil {
			if !reflect.DeepEqual(record.Witness, *cache) {
				return fmt.Errorf("%w: image cache immutable ownership changed", errs.ErrManualRecovery)
			}
			owned, err := responsibilities.CountMatchingReservation(service.ctx, quota.Reservation{
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
			if err = responsibilities.DeductReservationBytes(service.ctx, "import:"+step.OperationID, cache.PhysicalBytes); err != nil {
				return err
			}
			if err = responsibilities.Reserve(service.ctx, quota.Reservation{
				Owner: owner,
				Bytes: cache.PhysicalBytes,
			}); err != nil {
				return err
			}
			if err = store.RequireOne(catalog.InsertCache(service.ctx, dbgen.InsertCacheParams{
				Builder: key.Builder,
				DiffID:  key.DiffID,
				Body: operation.Encode(Cache{
					Witness:         *cache,
					SourceOperation: step.OperationID,
				}),
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
	return service.checkpoint()
}
