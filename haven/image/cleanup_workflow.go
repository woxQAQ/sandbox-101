package image

import (
	"context"
	"database/sql"
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

func (i *Images) imageCleanupIntents(step imageCleanupStep) ([]loom.DeletionIntent, error) {
	if step.Input.Cache != nil {
		intent, err := imageimport.InspectCacheDeletion(i.ctx, i.directory, step.Input.Cache.Witness)
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
		current, err := LoadManifest(i.ctx, i.database, source.Input.Options.Digest)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(current, source.State.Proof.Manifest) {
			return nil, fmt.Errorf("published image catalog changed")
		}
		if err = source.State.Proof.Verify(i.ctx, i.directory, source.Input.Target, *source.State.Boundary); err != nil {
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
	prepared := source.preparedWork(i.directory)
	var work loom.DeletionIntent
	var err error
	if published {
		work, err = prepared.InspectPublishedWorkDeletion(i.ctx)
	} else {
		work, err = prepared.InspectUnpublishedWorkDeletion(i.ctx)
	}
	if err != nil {
		return nil, err
	}
	intents := []loom.DeletionIntent{
		work,
	}
	if !published {
		count, err := dbgen.New(i.database).CountManifest(context.Background(), source.Input.Options.Digest)
		if err != nil || count != 0 {
			return nil, fmt.Errorf("failed import publication ownership changed: %v", err)
		}
		if _, err = os.Lstat(source.Input.Target); err == nil {
			if source.State.Proof == nil {
				return nil, fmt.Errorf("failed import final directory lacks original output proof")
			}
			intent, err := source.State.Proof.InspectUnpublishedDeletion(i.ctx, i.directory, source.Input.Target, *source.State.Boundary)
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

func (i *Images) saveImageCleanup(step *imageCleanupStep, state imageCleanupState) error {
	i.storeWriteMu.Lock()
	defer i.storeWriteMu.Unlock()
	tx, err := i.database.BeginTx(i.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if state.Phase == "prepared" {
		if err = store.RequireOne(dbgen.New(tx).BindCleanupReceipts(i.ctx, dbgen.BindCleanupReceiptsParams{
			OperationID: step.OperationID,
			ReceiptsHash: sql.NullString{
				String: operation.Digest(operation.Encode(state.Intents)),
				Valid:  true,
			},
		})); err != nil {
			return err
		}
		for _, intent := range state.Intents {
			if err = store.RecordDeletionReceipt(i.ctx, tx, step.OperationID, "image_cleanup", intent); err != nil {
				return err
			}
		}
	}
	if err = operation.AdvancePhase(i.ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "image_cleanup",
		InputHash:      step.Hash,
		PreviousStatus: step.Status,
		PreviousPhase:  step.State.Phase,
		NextStatus:     "running",
		NextPhase:      state.Phase,
	}); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	step.Status = "running"
	return i.checkpoint()
}

func (i *Images) verifyImageCleanupReceipts(step imageCleanupStep) error {
	if len(step.State.Intents) == 0 {
		return fmt.Errorf("image cleanup deletion proof missing")
	}
	for index, intent := range step.State.Intents {
		retained, err := store.LoadDeletionReceipt(i.ctx, i.database, step.OperationID, "image_cleanup", intent.Path)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(retained, intent) || intent.Format != 1 || !intent.Dir {
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

func (i *Images) runImageCleanup(step imageCleanupStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if step.State.Phase == "admitted" {
		intents, err := i.imageCleanupIntents(step)
		if err != nil {
			return err
		}
		state := imageCleanupState{
			Phase:   "prepared",
			Intents: intents,
		}
		if err = i.saveImageCleanup(&step, state); err != nil {
			return err
		}
		if err = i.fault("image_cleanup_prepared"); err != nil {
			return err
		}
	}
	if err := i.verifyImageCleanupReceipts(step); err != nil {
		return err
	}
	if step.State.Phase == "prepared" {
		for _, intent := range step.State.Intents {
			if err := loom.CheckLocalReaderIdentities(intent); err != nil {
				return err
			}
		}
		for _, intent := range step.State.Intents {
			files, err := loom.ResumeDeletion(i.ctx, i.directory, intent)
			if err != nil {
				return err
			}
			for _, file := range files {
				if err = loom.RemoveDeletionFile(i.ctx, i.directory, intent, file); err != nil {
					return err
				}
				if err = i.fault("image_cleanup_file_removed"); err != nil {
					return err
				}
			}
			if err = loom.RemoveDeletionDirectories(i.ctx, i.directory, intent); err != nil {
				return err
			}
		}
		if err := i.saveImageCleanup(&step, imageCleanupState{
			Phase:   "removed",
			Intents: step.State.Intents,
		}); err != nil {
			return err
		}
	}
	if step.State.Phase != "removed" {
		return fmt.Errorf("image cleanup phase unsupported")
	}
	return i.completeImageCleanup(step)
}

func (i *Images) completeImageCleanup(step imageCleanupStep) error {
	if err := i.verifyImageCleanupReceipts(step); err != nil {
		return err
	}
	for _, intent := range step.State.Intents {
		remaining, err := loom.ResumeDeletion(i.ctx, i.directory, intent)
		if err != nil || len(remaining) != 0 {
			return fmt.Errorf("image artifact reclamation incomplete: %v", err)
		}
		if _, err = os.Lstat(intent.Path); !os.IsNotExist(err) {
			return fmt.Errorf("image artifact directory reclamation unconfirmed")
		}
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
	current, err := loadImageCleanup(i.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("image cleanup current step changed")
	}
	if step.Input.Cache != nil {
		cache := step.Input.Cache
		record, err := loadImageCache(i.ctx, tx, cache.Witness.Cache.Builder, cache.Witness.Cache.DiffID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(record, *cache) {
			return fmt.Errorf("retired cache ownership changed")
		}
		live, err := imageCacheLive(i.ctx, tx, cache.Witness)
		if err != nil {
			return err
		}
		if live {
			return fmt.Errorf("retired cache gained a consumer")
		}
		if err = store.RequireOne(dbgen.New(tx).DeleteCache(i.ctx, dbgen.DeleteCacheParams{
			Builder:           cache.Witness.Cache.Builder,
			DiffID:            cache.Witness.Cache.DiffID,
			SourceOperationID: cache.SourceOperation,
		})); err != nil {
			return err
		}
		if err = quota.ReleaseMatchingReservation(i.ctx, tx, quota.Reservation{
			Owner: "cache:" + cache.Witness.Cache.Builder + ":" + cache.Witness.Cache.DiffID,
			Bytes: cache.Witness.PhysicalBytes,
		}); err != nil {
			return err
		}
	} else {
		source := step.Input.Source
		original, err := loadImageImport(i.ctx, tx, source.OperationID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(original, *source) {
			return fmt.Errorf("original import terminal changed")
		}
		if source.Status == "failed" {
			remaining, err := importRemainingCharge(i.ctx, tx, *source)
			if err != nil {
				return err
			}
			for _, layer := range source.State.Layers {
				if layer.Cache == nil {
					continue
				}
				record, err := loadImageCache(i.ctx, tx, layer.Cache.Cache.Builder, layer.Cache.Cache.DiffID)
				if err != nil || !reflect.DeepEqual(record.Witness, *layer.Cache) {
					return fmt.Errorf("failed import cache responsibility missing: %v", err)
				}
			}
			if err = store.RequireOne(dbgen.New(tx).DeleteImportIntent(i.ctx, dbgen.DeleteImportIntentParams{
				Digest:      source.Input.Options.Digest,
				OperationID: source.OperationID,
				InputHash:   source.Hash,
				Layout:      source.Input.Options.Materialization,
			})); err != nil {
				return err
			}
			if err = quota.ReleaseMatchingReservation(i.ctx, tx, quota.Reservation{
				Owner: "import:" + source.OperationID,
				Bytes: remaining,
			}); err != nil {
				return err
			}
		}
		if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanupKind(i.ctx, dbgen.CompletePendingCleanupKindParams{
			ID:           "image_import:" + source.OperationID,
			OperationID:  source.OperationID,
			ResourceKind: "image_import",
		})); err != nil {
			return err
		}
	}
	for _, intent := range step.State.Intents {
		if err = store.RetireDeletionReceipt(i.ctx, tx, step.OperationID, "image_cleanup", intent); err != nil {
			return err
		}
	}
	state := step.State
	state.Phase = "complete"
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanupResource(i.ctx, dbgen.CompletePendingCleanupResourceParams{
		ID:           "image_cleanup:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: step.Input.Kind,
		ResourceID:   step.Input.ResourceID,
	})); err != nil {
		return err
	}
	if err = operation.AdvancePhase(i.ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "image_cleanup",
		InputHash:      step.Hash,
		PreviousStatus: step.Status,
		PreviousPhase:  step.State.Phase,
		NextStatus:     "succeeded",
		NextPhase:      state.Phase,
	}); err != nil {
		return err
	}
	if err = operations.Complete(i.ctx, step.OperationID); err != nil {
		return err
	}
	if err = i.fault("image_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return i.checkpoint()
}
