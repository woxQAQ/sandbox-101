package image

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// preparedWork is formed only after the phase has checked its frozen plan and
// preparation receipt. The effects retain those exact values through the call.
func (step imageImportStep) preparedWork(state string) imageimport.ImportWork {
	return imageimport.ImportWork{
		StateRoot:   state,
		OperationID: step.OperationID,
		Plan:        *step.Input.Plan,
		Boundary:    *step.State.Boundary,
	}
}

func (i *Images) runImageImport(step imageImportStep) (returned error) {
	if operation.Terminal(step.Status) {
		return nil
	}
	defer func() {
		if returned != nil && !errors.Is(returned, errs.ErrInterrupted) && !errors.Is(returned, context.Canceled) {
			if err := i.failImageImport(step); err != nil {
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
			if err := i.saveImageImport(&step, "prepare_intent"); err != nil {
				return err
			}
			boundary, err := imageimport.PrepareImageImport(i.directory, step.OperationID)
			if err != nil {
				return err
			}
			if err = i.fault("image_import_prepared_effect"); err != nil {
				return err
			}
			step.State.Boundary = &boundary
			if err = i.saveImageImport(&step, "prepared"); err != nil {
				return err
			}
		case "prepared":
			if step.State.Boundary == nil {
				return fmt.Errorf("image import lacks preparation receipt")
			}
			if err := i.saveImageImport(&step, "staging_intent"); err != nil {
				return err
			}
			if err := step.preparedWork(i.directory).Stage(i.ctx); err != nil {
				return err
			}
			if err := i.saveImageImport(&step, "staged"); err != nil {
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
				if err := i.saveImageImport(&step, "layer_intent"); err != nil {
					return err
				}
				proof, err := step.preparedWork(i.directory).MaterializeLayer(i.ctx, len(step.State.Layers), host.LayerLimits{
					MaxBytes: step.Input.Options.MaxBytes - bytes,
					MaxFiles: step.Input.Options.MaxFiles - files,
				})
				if err != nil {
					return err
				}
				step.State.Layers = append(step.State.Layers, proof)
				if err = i.saveImageImport(&step, "staged"); err != nil {
					return err
				}
				if err = i.fault("image_import_layer_saved"); err != nil {
					return err
				}
				continue
			}
			if err := i.saveImageImport(&step, "output_intent"); err != nil {
				return err
			}
			proof, err := step.preparedWork(i.directory).BuildOutput(i.ctx, step.State.Layers)
			if err != nil {
				return err
			}
			step.State.Proof = &proof
			if err = i.saveImageImport(&step, "materialized"); err != nil {
				return err
			}
			if err = i.fault("image_import_materialized"); err != nil {
				return err
			}
		case "materialized":
			if step.State.Boundary == nil || step.State.Proof == nil {
				return fmt.Errorf("image publication lacks output proof")
			}
			if err := step.State.Proof.Verify(i.ctx, i.directory, filepath.Join(step.Input.Work, "output"), *step.State.Boundary); err != nil {
				return err
			}
			if err := i.saveImageImport(&step, "publish_intent"); err != nil {
				return err
			}
		case "publish_intent":
			if step.State.Boundary == nil || step.State.Proof == nil {
				return fmt.Errorf("image publication lacks frozen rename proof")
			}
			if err := step.State.Proof.Publish(i.ctx, i.directory, step.OperationID, *step.State.Boundary); err != nil {
				return err
			}
			if err := i.fault("image_import_renamed"); err != nil {
				return err
			}
			return i.completeImageImport(step)
		case "prepare_intent", "staging_intent", "layer_intent", "output_intent":
			return fmt.Errorf("%w: interrupted image effect has unknown output; explicit cleanup review required", errs.ErrManualRecovery)
		default:
			return fmt.Errorf("%w: unknown image import phase", errs.ErrManualRecovery)
		}
	}
}

func (i *Images) failImageImport(step imageImportStep) error {
	i.storeWriteMu.Lock()
	defer i.storeWriteMu.Unlock()
	tx, err := i.database.BeginTx(i.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadImageImport(i.ctx, tx, step.OperationID)
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
	if err = transitionImport(i.ctx, tx, current, current.State.FailedPhase, "failed"); err != nil {
		return err
	}
	if err = operations.FailResult(i.ctx, step.OperationID, operation.Result{
		Error: current.State.Error,
	}); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(i.ctx, dbgen.InsertCleanupIntentParams{
		ID:           "image_import:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "image_import",
		ResourceID:   step.Input.Options.Digest,
		StepKey:      "image_import",
		WitnessHash:  step.Hash,
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return i.checkpoint()
}

func (i *Images) completeImageImport(step imageImportStep) error {
	if step.State.Boundary == nil || step.State.Proof == nil {
		return fmt.Errorf("image publication proof missing")
	}
	proof := step.State.Proof
	plan := step.Input.Plan
	if plan == nil || proof.Manifest.Digest != step.Input.Options.Digest || proof.Manifest.Layout != step.Input.Options.Materialization || proof.Manifest.Builder != plan.Builder || !reflect.DeepEqual(proof.Manifest.Tools, plan.Tools) {
		return fmt.Errorf("image publication differs from frozen plan")
	}
	if err := proof.Verify(i.ctx, i.directory, step.Input.Target, *step.State.Boundary); err != nil {
		return err
	}
	i.storeWriteMu.Lock()
	defer i.storeWriteMu.Unlock()
	tx, err := i.database.BeginTx(i.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadImageImport(i.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || current.State.Phase != "publish_intent" || !reflect.DeepEqual(current.State, step.State) {
		return fmt.Errorf("image publication frozen phase changed")
	}
	catalog := dbgen.New(tx)
	intent, err := catalog.GetImportIntent(i.ctx, step.Input.Options.Digest)
	if err != nil {
		return err
	}
	if intent.OperationID != step.OperationID || intent.Layout != step.Input.Options.Materialization || intent.InputHash != step.Hash {
		return fmt.Errorf("image publication ownership claim changed")
	}
	remaining, err := importRemainingCharge(i.ctx, tx, step)
	if err != nil {
		return err
	}
	step.State.Phase = "complete"
	if err = PublishManifest(i.ctx, tx, proof.Manifest); err != nil {
		return err
	}
	if err = quota.TransferStorageReservation(i.ctx, tx, "import:"+step.OperationID, "image:"+proof.Manifest.Digest, remaining); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(i.ctx, dbgen.InsertCleanupIntentParams{
		ID:           "image_import:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "image_import",
		ResourceID:   proof.Manifest.Digest,
		StepKey:      "image_import",
		WitnessHash:  step.Hash,
	})); err != nil {
		return err
	}
	if err = transitionImport(i.ctx, tx, step, current.State.Phase, "succeeded"); err != nil {
		return err
	}
	if err = operations.CompleteResult(i.ctx, step.OperationID, operation.Result{
		ImageDigest: proof.Manifest.Digest,
	}); err != nil {
		return err
	}
	if err = i.fault("image_import_publication_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return i.checkpoint()
}
