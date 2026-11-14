package haven

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/imageimport"
)

func (service *Service) runImageImport(step imageImportStep) (returned error) {
	if terminal(step.Status) {
		return nil
	}
	defer func() {
		if returned != nil && !errors.Is(returned, ErrInterrupted) && !errors.Is(returned, context.Canceled) {
			if err := service.failImageImport(step); err != nil {
				returned = errors.Join(returned, err)
			}
		}
	}()
	if step.Input.Plan == nil {
		return fmt.Errorf("pending image import lacks frozen plan")
	}
	for {
		switch step.State.Phase {
		case "admitted":
			if err := service.saveImageImport(&step, "prepare_intent"); err != nil {
				return err
			}
			boundary, err := imageimport.PrepareImageImport(service.store.directory, step.OperationID)
			if err != nil {
				return err
			}
			if err = service.fault("image_import_prepared_effect"); err != nil {
				return err
			}
			step.State.Boundary = &boundary
			if err = service.saveImageImport(&step, "prepared"); err != nil {
				return err
			}
		case "prepared":
			if step.State.Boundary == nil {
				return fmt.Errorf("image import lacks preparation receipt")
			}
			if err := service.saveImageImport(&step, "staging_intent"); err != nil {
				return err
			}
			if err := imageimport.StageImageImport(service.ctx, service.store.directory, step.OperationID, *step.Input.Plan, *step.State.Boundary); err != nil {
				return err
			}
			if err := service.saveImageImport(&step, "staged"); err != nil {
				return err
			}
		case "staged":
			if step.State.Boundary == nil {
				return fmt.Errorf("image import lacks prepared output")
			}
			if len(step.State.Layers) < len(step.Input.Plan.Layers) {
				var bytes, files int64
				for _, proof := range step.State.Layers {
					bytes += proof.Bytes
					files += proof.Entries
				}
				if err := service.saveImageImport(&step, "layer_intent"); err != nil {
					return err
				}
				proof, err := imageimport.MaterializeImageLayer(service.ctx, service.store.directory, step.OperationID, *step.Input.Plan, *step.State.Boundary, len(step.State.Layers), step.Input.Options.MaxBytes-bytes, step.Input.Options.MaxFiles-files)
				if err != nil {
					return err
				}
				step.State.Layers = append(step.State.Layers, proof)
				if err = service.saveImageImport(&step, "staged"); err != nil {
					return err
				}
				if err = service.fault("image_import_layer_saved"); err != nil {
					return err
				}
				continue
			}
			if err := service.saveImageImport(&step, "output_intent"); err != nil {
				return err
			}
			proof, err := imageimport.BuildImageImportOutput(service.ctx, service.store.directory, step.OperationID, *step.Input.Plan, *step.State.Boundary, step.State.Layers)
			if err != nil {
				return err
			}
			step.State.Proof = &proof
			if err = service.saveImageImport(&step, "materialized"); err != nil {
				return err
			}
			if err = service.fault("image_import_materialized"); err != nil {
				return err
			}
		case "materialized":
			if step.State.Boundary == nil || step.State.Proof == nil {
				return fmt.Errorf("image publication lacks output proof")
			}
			if err := imageimport.VerifyImportedImage(service.ctx, service.store.directory, filepath.Join(step.Input.Work, "output"), *step.State.Boundary, *step.State.Proof); err != nil {
				return err
			}
			if err := service.saveImageImport(&step, "publish_intent"); err != nil {
				return err
			}
		case "publish_intent":
			if step.State.Boundary == nil || step.State.Proof == nil {
				return fmt.Errorf("image publication lacks frozen rename proof")
			}
			if err := imageimport.PublishImportedImage(service.ctx, service.store.directory, step.OperationID, *step.State.Boundary, *step.State.Proof); err != nil {
				return err
			}
			if err := service.fault("image_import_renamed"); err != nil {
				return err
			}
			return service.completeImageImport(step)
		case "prepare_intent", "staging_intent", "layer_intent", "output_intent":
			return loom.Fail(loom.ManualRecovery, "interrupted image effect has unknown output; explicit cleanup review required")
		default:
			return loom.Fail(loom.ManualRecovery, "unknown image import phase")
		}
	}
}

func (service *Service) failImageImport(step imageImportStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadImageImport(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash {
		return fmt.Errorf("failed image import identity changed")
	}
	current.State.FailedPhase = current.State.Phase
	current.State.Phase = "failed"
	current.State.Error = "image import failed; node storage responsibility retained"
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key='image_import' AND input_hash=? AND status IN ('pending','running')", []any{
			encode(current.State),
			step.OperationID,
			step.Hash,
		}},
		{"UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", []any{
			encode(operationResult{
				Error: current.State.Error,
			}),
			step.OperationID,
		}},
		{"INSERT INTO cleanup_intents VALUES(?,?,'image_import',?,'pending',?)", []any{
			"image_import:" + step.OperationID,
			step.OperationID,
			step.Input.Options.Digest,
			encode(current),
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) completeImageImport(step imageImportStep) error {
	if step.State.Boundary == nil || step.State.Proof == nil {
		return fmt.Errorf("image publication proof missing")
	}
	proof := step.State.Proof
	plan := step.Input.Plan
	if plan == nil || proof.Manifest.Digest != step.Input.Options.Digest || proof.Manifest.Layout != step.Input.Options.Materialization || proof.Manifest.Builder != plan.Builder || !reflect.DeepEqual(proof.Manifest.Tools, plan.Tools) {
		return fmt.Errorf("image publication differs from frozen plan")
	}
	if err := imageimport.VerifyImportedImage(service.ctx, service.store.directory, step.Input.Target, *step.State.Boundary, *proof); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadImageImport(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || current.State.Phase != "publish_intent" || !reflect.DeepEqual(current.State, step.State) {
		return fmt.Errorf("image publication frozen phase changed")
	}
	var owner, layout string
	var payload []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT operation_id,layout,body FROM image_intents WHERE digest=?", step.Input.Options.Digest).Scan(&owner, &layout, &payload); err != nil {
		return err
	}
	if owner != step.OperationID || layout != step.Input.Options.Materialization || string(payload) != string(encode(step.Input)) {
		return fmt.Errorf("image publication ownership claim changed")
	}
	remaining, err := importRemainingCharge(service.ctx, tx, step)
	if err != nil {
		return err
	}
	state := step.State
	state.Phase = "complete"
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO images VALUES(?,?)", []any{
			proof.Manifest.Digest,
			encode(proof.Manifest),
		}},
		{"UPDATE reservations SET owner=? WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", []any{
			"image:" + proof.Manifest.Digest,
			"import:" + step.OperationID,
			remaining,
		}},
		{"INSERT INTO cleanup_intents VALUES(?,?,'image_import',?,'pending',?)", []any{
			"image_import:" + step.OperationID,
			step.OperationID,
			proof.Manifest.Digest,
			encode(struct {
				Input imageImportInput
				State imageImportState
			}{step.Input, state}),
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='image_import' AND input_hash=? AND status IN ('pending','running')", []any{
			encode(state),
			step.OperationID,
			step.Hash,
		}},
		{"UPDATE operations SET status='succeeded',body=? WHERE id=? AND status IN ('pending','running')", []any{
			encode(operationResult{
				ImageDigest: proof.Manifest.Digest,
			}),
			step.OperationID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("image_import_publication_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}
