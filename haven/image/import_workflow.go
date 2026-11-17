package image

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (service *Images) runImageImport(step imageImportStep) (returned error) {
	if operation.Terminal(step.Status) {
		return nil
	}
	defer func() {
		if returned != nil && !errors.Is(returned, errs.ErrInterrupted) && !errors.Is(returned, context.Canceled) {
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
			boundary, err := imageimport.PrepareImageImport(service.directory, step.OperationID)
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
			if err := imageimport.StageImageImport(service.ctx, service.directory, step.OperationID, *step.Input.Plan, *step.State.Boundary); err != nil {
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
				proof, err := imageimport.MaterializeImageLayer(service.ctx, service.directory, step.OperationID, *step.Input.Plan, *step.State.Boundary, len(step.State.Layers), step.Input.Options.MaxBytes-bytes, step.Input.Options.MaxFiles-files)
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
			proof, err := imageimport.BuildImageImportOutput(service.ctx, service.directory, step.OperationID, *step.Input.Plan, *step.State.Boundary, step.State.Layers)
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
			if err := imageimport.VerifyImportedImage(service.ctx, service.directory, filepath.Join(step.Input.Work, "output"), *step.State.Boundary, *step.State.Proof); err != nil {
				return err
			}
			if err := service.saveImageImport(&step, "publish_intent"); err != nil {
				return err
			}
		case "publish_intent":
			if step.State.Boundary == nil || step.State.Proof == nil {
				return fmt.Errorf("image publication lacks frozen rename proof")
			}
			if err := imageimport.PublishImportedImage(service.ctx, service.directory, step.OperationID, *step.State.Boundary, *step.State.Proof); err != nil {
				return err
			}
			if err := service.fault("image_import_renamed"); err != nil {
				return err
			}
			return service.completeImageImport(step)
		case "prepare_intent", "staging_intent", "layer_intent", "output_intent":
			return fmt.Errorf("%w: interrupted image effect has unknown output; explicit cleanup review required", errs.ErrManualRecovery)
		default:
			return fmt.Errorf("%w: unknown image import phase", errs.ErrManualRecovery)
		}
	}
}

func (service *Images) failImageImport(step imageImportStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.database.BeginTx(service.ctx, nil)
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
	if operation.Terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash {
		return fmt.Errorf("failed image import identity changed")
	}
	current.State.FailedPhase = current.State.Phase
	current.State.Phase = "failed"
	current.State.Error = "image import failed; node storage responsibility retained"
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(service.ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "image_import",
		InputHash:   step.Hash,
		Body:        operation.Encode(current.State),
	})); err != nil {
		return err
	}
	if err = operations.FailResult(service.ctx, step.OperationID, operation.Encode(operation.Result{
		Error: current.State.Error,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(service.ctx, dbgen.InsertCleanupIntentParams{
		ID:           "image_import:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "image_import",
		ResourceID:   step.Input.Options.Digest,
		Body:         operation.Encode(current),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.checkpoint()
}

func (service *Images) completeImageImport(step imageImportStep) error {
	if step.State.Boundary == nil || step.State.Proof == nil {
		return fmt.Errorf("image publication proof missing")
	}
	proof := step.State.Proof
	plan := step.Input.Plan
	if plan == nil || proof.Manifest.Digest != step.Input.Options.Digest || proof.Manifest.Layout != step.Input.Options.Materialization || proof.Manifest.Builder != plan.Builder || !reflect.DeepEqual(proof.Manifest.Tools, plan.Tools) {
		return fmt.Errorf("image publication differs from frozen plan")
	}
	if err := imageimport.VerifyImportedImage(service.ctx, service.directory, step.Input.Target, *step.State.Boundary, *proof); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.database.BeginTx(service.ctx, nil)
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
	catalog := dbgen.New(tx)
	intent, err := catalog.GetImportIntent(service.ctx, step.Input.Options.Digest)
	if err != nil {
		return err
	}
	if intent.OperationID != step.OperationID || intent.Layout != step.Input.Options.Materialization || string(intent.Body) != string(operation.Encode(step.Input)) {
		return fmt.Errorf("image publication ownership claim changed")
	}
	remaining, err := importRemainingCharge(service.ctx, tx, step)
	if err != nil {
		return err
	}
	state := step.State
	state.Phase = "complete"
	if err = store.RequireOne(catalog.InsertManifest(service.ctx, dbgen.InsertManifestParams{
		Digest: proof.Manifest.Digest,
		Body:   operation.Encode(proof.Manifest),
	})); err != nil {
		return err
	}
	if err = quota.New(tx).TransferStorageReservation(service.ctx, "import:"+step.OperationID, "image:"+proof.Manifest.Digest, remaining); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(service.ctx, dbgen.InsertCleanupIntentParams{
		ID:           "image_import:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "image_import",
		ResourceID:   proof.Manifest.Digest,
		Body: operation.Encode(struct {
			Input imageImportInput
			State imageImportState
		}{step.Input, state}),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteLiveStep(service.ctx, dbgen.CompleteLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "image_import",
		InputHash:   step.Hash,
		Body:        operation.Encode(state),
	})); err != nil {
		return err
	}
	if err = operations.CompleteResult(service.ctx, step.OperationID, operation.Encode(operation.Result{
		ImageDigest: proof.Manifest.Digest,
	})); err != nil {
		return err
	}
	if err = service.fault("image_import_publication_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.checkpoint()
}
