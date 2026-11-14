package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
)

type imageCacheRecord struct {
	Witness         imageimport.ImageCacheWitness `json:"witness"`
	SourceOperation string                        `json:"source_operation"`
}

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
	OperationID, Hash, Status string
	Input                     imageImportInput
	State                     imageImportState
}

func loadImageImport(ctx context.Context, q queryer, id string) (imageImportStep, error) {
	step := imageImportStep{
		OperationID: id,
	}
	var payload, body []byte
	var action, candidate, expected string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,action,candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key='image_import'", id).Scan(&step.Hash, &payload, &step.Status, &body, &action, &candidate, &expected)
	if err != nil {
		return step, err
	}
	if digest(payload) != step.Hash {
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

func (service *Service) saveImageImport(step *imageImportStep, phase string) error {
	step.State.Phase = phase
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='image_import' AND input_hash=? AND status IN ('pending','running')", encode(step.State), step.OperationID, step.Hash)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='running' WHERE id=? AND status IN ('pending','running')", step.OperationID)); err != nil {
		return err
	}
	for _, layer := range step.State.Layers {
		if layer.Cache == nil {
			continue
		}
		cache := layer.Cache
		var body []byte
		err = tx.QueryRowContext(service.ctx, "SELECT body FROM image_caches WHERE builder=? AND diff_id=?", cache.Cache.Builder, cache.Cache.DiffID).Scan(&body)
		if err == nil {
			var record imageCacheRecord
			if err = json.Unmarshal(body, &record); err != nil {
				return err
			}
			if !reflect.DeepEqual(record.Witness, *cache) {
				return loom.Fail(loom.ManualRecovery, "image cache immutable ownership changed")
			}
			var owned int
			if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", "cache:"+cache.Cache.Builder+":"+cache.Cache.DiffID, cache.PhysicalBytes).Scan(&owned); err != nil {
				return err
			}
			if owned != 1 {
				return loom.Fail(loom.ManualRecovery, "image cache physical charge missing")
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			if cache.PhysicalBytes <= 0 {
				return loom.Fail(loom.Incompatible, "image cache lacks physical charge")
			}
			owner := "cache:" + cache.Cache.Builder + ":" + cache.Cache.DiffID
			if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE reservations SET bytes=bytes-? WHERE owner=? AND bytes>=?", cache.PhysicalBytes, "import:"+step.OperationID, cache.PhysicalBytes)); err != nil {
				return err
			}
			if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO reservations VALUES(?,?,0,0)", owner, cache.PhysicalBytes)); err != nil {
				return err
			}
			if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO image_caches VALUES(?,?,?)", cache.Cache.Builder, cache.Cache.DiffID, encode(imageCacheRecord{
				Witness:         *cache,
				SourceOperation: step.OperationID,
			}))); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}
