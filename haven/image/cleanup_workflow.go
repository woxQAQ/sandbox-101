package image

import (
	"context"
	"fmt"
	"os"
	"reflect"

	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (service *Images) imageCleanupIntents(step imageCleanupStep) ([]loom.DeletionIntent, error) {
	if step.Input.Cache != nil {
		intent, err := imageimport.InspectCacheDeletion(service.ctx, service.directory, step.Input.Cache.Witness)
		return []loom.DeletionIntent{
			intent,
		}, err
	}
	source := step.Input.Source
	if source == nil || source.State.Boundary == nil || source.Input.Plan == nil {
		return nil, fmt.Errorf("image cleanup source preparation missing")
	}
	if err := imageWorkCleanupEligible(*source); err != nil {
		return nil, err
	}
	published := source.Status == "succeeded"
	if published {
		if source.State.Proof == nil {
			return nil, fmt.Errorf("published image cleanup lacks original publication proof")
		}
		current, err := LoadManifest(service.ctx, service.database, source.Input.Options.Digest)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(current, source.State.Proof.Manifest) {
			return nil, fmt.Errorf("published image catalog changed")
		}
		if err = source.State.Proof.Verify(service.ctx, service.directory, source.Input.Target, *source.State.Boundary); err != nil {
			return nil, err
		}
	} else {
		if _, err := os.Lstat(source.Input.Target); os.IsNotExist(err) {
			info, err := os.Lstat(source.Input.Work + "/output")
			if err != nil || loom.IdentityOf(info) != source.State.Boundary.Output {
				return nil, fmt.Errorf("original import output reclamation witness missing: %v", err)
			}
		} else if err != nil {
			return nil, err
		}
	}
	prepared := source.preparedWork(service.directory)
	var work loom.DeletionIntent
	var err error
	if published {
		work, err = prepared.InspectPublishedWorkDeletion(service.ctx)
	} else {
		work, err = prepared.InspectUnpublishedWorkDeletion(service.ctx)
	}
	if err != nil {
		return nil, err
	}
	intents := []loom.DeletionIntent{
		work,
	}
	if !published {
		count, err := dbgen.New(service.database).CountManifest(context.Background(), source.Input.Options.Digest)
		if err != nil || count != 0 {
			return nil, fmt.Errorf("failed import publication ownership changed: %v", err)
		}
		if _, err = os.Lstat(source.Input.Target); err == nil {
			if source.State.Proof == nil {
				return nil, fmt.Errorf("failed import final directory lacks original output proof")
			}
			intent, err := source.State.Proof.InspectUnpublishedDeletion(service.ctx, service.directory, source.Input.Target, *source.State.Boundary)
			if err != nil {
				return nil, err
			}
			intents = append(intents, intent)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	return intents, nil
}

func (service *Images) saveImageCleanup(step *imageCleanupStep, state imageCleanupState) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if state.Phase == "prepared" {
		for _, intent := range state.Intents {
			if err = store.RequireOne(dbgen.New(tx).InsertDeletionReceipt(service.ctx, dbgen.InsertDeletionReceiptParams{
				Path:        intent.Path,
				OperationID: step.OperationID,
				StepKey:     "image_cleanup",
				Body:        operation.Encode(intent),
			})); err != nil {
				return err
			}
		}
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "image_cleanup",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(state),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	step.Status = "running"
	return service.checkpoint()
}

func (service *Images) verifyImageCleanupReceipts(step imageCleanupStep) error {
	if len(step.State.Intents) == 0 {
		return fmt.Errorf("image cleanup deletion proof missing")
	}
	for index, intent := range step.State.Intents {
		payload, err := dbgen.New(service.database).GetDeletionReceipt(service.ctx, dbgen.GetDeletionReceiptParams{
			Path:        intent.Path,
			OperationID: step.OperationID,
			StepKey:     "image_cleanup",
		})
		if err != nil {
			return err
		}
		if string(payload) != string(operation.Encode(intent)) || intent.Format != 1 || !intent.Dir {
			return fmt.Errorf("image cleanup immutable deletion receipt changed")
		}
		if step.Input.Cache != nil {
			if index != 0 || intent.Path != step.Input.Cache.Witness.Path || intent.Nodes["."] != step.Input.Cache.Witness.Directory {
				return fmt.Errorf("cache cleanup receipt source changed")
			}
		} else {
			source := step.Input.Source
			if source == nil || source.State.Boundary == nil {
				return fmt.Errorf("image work cleanup source absent")
			}
			if index == 0 {
				if intent.Path != source.Input.Work || intent.Nodes["."] != source.State.Boundary.Work {
					return fmt.Errorf("image work receipt changed")
				}
			} else if index != 1 || source.Status != "failed" || intent.Path != source.Input.Target || intent.Nodes["."] != source.State.Boundary.Output {
				return fmt.Errorf("unpublished image receipt changed")
			}
		}
	}
	return nil
}

func (service *Images) runImageCleanup(step imageCleanupStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if step.State.Phase == "admitted" {
		intents, err := service.imageCleanupIntents(step)
		if err != nil {
			return err
		}
		state := imageCleanupState{
			Phase:   "prepared",
			Intents: intents,
		}
		if err = service.saveImageCleanup(&step, state); err != nil {
			return err
		}
		if err = service.fault("image_cleanup_prepared"); err != nil {
			return err
		}
	}
	if err := service.verifyImageCleanupReceipts(step); err != nil {
		return err
	}
	if step.State.Phase == "prepared" {
		var identities []loom.FileIdentity
		for _, intent := range step.State.Intents {
			for _, identity := range intent.Nodes {
				identities = append(identities, identity)
			}
		}
		if err := loom.CheckLocalReaderIdentities(identities); err != nil {
			return err
		}
		for _, intent := range step.State.Intents {
			files, err := loom.ResumeDeletion(service.ctx, service.directory, intent)
			if err != nil {
				return err
			}
			for _, file := range files {
				if err = loom.RemoveDeletionFile(service.ctx, service.directory, intent, file); err != nil {
					return err
				}
				if err = service.fault("image_cleanup_file_removed"); err != nil {
					return err
				}
			}
			if err = loom.RemoveDeletionDirectories(service.ctx, service.directory, intent); err != nil {
				return err
			}
		}
		if err := service.saveImageCleanup(&step, imageCleanupState{
			Phase:   "removed",
			Intents: step.State.Intents,
		}); err != nil {
			return err
		}
	}
	if step.State.Phase != "removed" {
		return fmt.Errorf("image cleanup phase unsupported")
	}
	return service.completeImageCleanup(step)
}

func (service *Images) completeImageCleanup(step imageCleanupStep) error {
	if err := service.verifyImageCleanupReceipts(step); err != nil {
		return err
	}
	for _, intent := range step.State.Intents {
		remaining, err := loom.ResumeDeletion(service.ctx, service.directory, intent)
		if err != nil || len(remaining) != 0 {
			return fmt.Errorf("image artifact reclamation incomplete: %v", err)
		}
		if _, err = os.Lstat(intent.Path); !os.IsNotExist(err) {
			return fmt.Errorf("image artifact directory reclamation unconfirmed")
		}
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
	current, err := loadImageCleanup(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("image cleanup current step changed")
	}
	if step.Input.Cache != nil {
		cache := step.Input.Cache
		record, err := loadImageCache(service.ctx, tx, cache.Witness.Cache.Builder, cache.Witness.Cache.DiffID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(record, *cache) {
			return fmt.Errorf("retired cache ownership changed")
		}
		live, err := imageCacheLive(service.ctx, tx, cache.Witness)
		if err != nil {
			return err
		}
		if live {
			return fmt.Errorf("retired cache gained a consumer")
		}
		if err = store.RequireOne(dbgen.New(tx).DeleteCache(service.ctx, dbgen.DeleteCacheParams{
			Builder: cache.Witness.Cache.Builder,
			DiffID:  cache.Witness.Cache.DiffID,
			Body:    operation.Encode(cache),
		})); err != nil {
			return err
		}
		if err = quota.New(tx).ReleaseMatchingReservation(service.ctx, quota.Reservation{
			Owner: "cache:" + cache.Witness.Cache.Builder + ":" + cache.Witness.Cache.DiffID,
			Bytes: cache.Witness.PhysicalBytes,
		}); err != nil {
			return err
		}
	} else {
		source := step.Input.Source
		original, err := loadImageImport(service.ctx, tx, source.OperationID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(original, *source) {
			return fmt.Errorf("original import terminal changed")
		}
		if source.Status == "failed" {
			remaining, err := importRemainingCharge(service.ctx, tx, *source)
			if err != nil {
				return err
			}
			for _, layer := range source.State.Layers {
				if layer.Cache == nil {
					continue
				}
				record, err := loadImageCache(service.ctx, tx, layer.Cache.Cache.Builder, layer.Cache.Cache.DiffID)
				if err != nil || !reflect.DeepEqual(record.Witness, *layer.Cache) {
					return fmt.Errorf("failed import cache responsibility missing: %v", err)
				}
			}
			if err = store.RequireOne(dbgen.New(tx).DeleteImportIntent(service.ctx, dbgen.DeleteImportIntentParams{
				Digest:      source.Input.Options.Digest,
				OperationID: source.OperationID,
				Body:        operation.Encode(source.Input),
			})); err != nil {
				return err
			}
			if err = quota.New(tx).ReleaseMatchingReservation(service.ctx, quota.Reservation{
				Owner: "import:" + source.OperationID,
				Bytes: remaining,
			}); err != nil {
				return err
			}
		}
		if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanupKind(service.ctx, dbgen.CompletePendingCleanupKindParams{
			ID:           "image_import:" + source.OperationID,
			OperationID:  source.OperationID,
			ResourceKind: "image_import",
		})); err != nil {
			return err
		}
	}
	for _, intent := range step.State.Intents {
		if err = store.RequireOne(dbgen.New(tx).DeleteMatchingDeletionReceipt(service.ctx, dbgen.DeleteMatchingDeletionReceiptParams{
			Path:        intent.Path,
			OperationID: step.OperationID,
			StepKey:     "image_cleanup",
			Body:        operation.Encode(intent),
		})); err != nil {
			return err
		}
	}
	state := step.State
	state.Phase = "complete"
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanupResource(service.ctx, dbgen.CompletePendingCleanupResourceParams{
		ID:           "image_cleanup:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: step.Input.Kind,
		ResourceID:   step.Input.ResourceID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteUnchangedStep(service.ctx, dbgen.CompleteUnchangedStepParams{
		OperationID:  step.OperationID,
		StepKey:      "image_cleanup",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(state),
	})); err != nil {
		return err
	}
	if err = operations.Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("image_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.checkpoint()
}
