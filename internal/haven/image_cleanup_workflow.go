package haven

import (
	"fmt"
	"os"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/imageimport"
)

func (service *Service) imageCleanupIntents(step imageCleanupStep) ([]loom.DeletionIntent, error) {
	if step.Input.Cache != nil {
		intent, err := imageimport.InspectCacheDeletion(service.ctx, service.store.directory, step.Input.Cache.Witness)
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
		current, err := loadImage(service.ctx, service.store.db, source.Input.Options.Digest)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(current, source.State.Proof.Manifest) {
			return nil, fmt.Errorf("published image catalog changed")
		}
		if err = imageimport.VerifyImportedImage(service.ctx, service.store.directory, source.Input.Target, *source.State.Boundary, *source.State.Proof); err != nil {
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
	work, err := imageimport.InspectWorkDeletion(service.ctx, service.store.directory, source.OperationID, *source.Input.Plan, *source.State.Boundary, published)
	if err != nil {
		return nil, err
	}
	intents := []loom.DeletionIntent{
		work,
	}
	if !published {
		var count int
		if err = service.store.db.QueryRow("SELECT COUNT(*) FROM images WHERE digest=?", source.Input.Options.Digest).Scan(&count); err != nil || count != 0 {
			return nil, fmt.Errorf("failed import publication ownership changed: %v", err)
		}
		if _, err = os.Lstat(source.Input.Target); err == nil {
			if source.State.Proof == nil {
				return nil, fmt.Errorf("failed import final directory lacks original output proof")
			}
			intent, err := imageimport.InspectUnpublishedImageDeletion(service.ctx, service.store.directory, source.Input.Target, *source.State.Boundary, *source.State.Proof)
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

func (service *Service) saveImageCleanup(step *imageCleanupStep, state imageCleanupState) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if state.Phase == "prepared" {
		for _, intent := range state.Intents {
			if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO deletion_intents VALUES(?,?,'image_cleanup',?)", intent.Path, step.OperationID, encode(intent))); err != nil {
				return err
			}
		}
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='image_cleanup' AND input_hash=? AND body=? AND status IN ('pending','running')", encode(state), step.OperationID, step.Hash, encode(step.State))); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	step.Status = "running"
	return service.store.checkpoint()
}

func (service *Service) verifyImageCleanupReceipts(step imageCleanupStep) error {
	if len(step.State.Intents) == 0 {
		return fmt.Errorf("image cleanup deletion proof missing")
	}
	for index, intent := range step.State.Intents {
		var payload []byte
		err := service.store.db.QueryRowContext(service.ctx, "SELECT body FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='image_cleanup'", intent.Path, step.OperationID).Scan(&payload)
		if err != nil {
			return err
		}
		if string(payload) != string(encode(intent)) || intent.Format != 1 || !intent.Dir {
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

func (service *Service) runImageCleanup(step imageCleanupStep) error {
	if terminal(step.Status) {
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
			files, err := loom.ResumeDeletion(service.ctx, service.store.directory, intent)
			if err != nil {
				return err
			}
			for _, file := range files {
				if err = loom.RemoveDeletionFile(service.ctx, service.store.directory, intent, file); err != nil {
					return err
				}
				if err = service.fault("image_cleanup_file_removed"); err != nil {
					return err
				}
			}
			if err = loom.RemoveDeletionDirectories(service.ctx, service.store.directory, intent); err != nil {
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

func (service *Service) completeImageCleanup(step imageCleanupStep) error {
	if err := service.verifyImageCleanupReceipts(step); err != nil {
		return err
	}
	for _, intent := range step.State.Intents {
		remaining, err := loom.ResumeDeletion(service.ctx, service.store.directory, intent)
		if err != nil || len(remaining) != 0 {
			return fmt.Errorf("image artifact reclamation incomplete: %v", err)
		}
		if _, err = os.Lstat(intent.Path); !os.IsNotExist(err) {
			return fmt.Errorf("image artifact directory reclamation unconfirmed")
		}
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
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM image_caches WHERE builder=? AND diff_id=? AND body=?", cache.Witness.Cache.Builder, cache.Witness.Cache.DiffID, encode(cache))); err != nil {
			return err
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", "cache:"+cache.Witness.Cache.Builder+":"+cache.Witness.Cache.DiffID, cache.Witness.PhysicalBytes)); err != nil {
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
			if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM image_intents WHERE digest=? AND operation_id=? AND body=?", source.Input.Options.Digest, source.OperationID, encode(source.Input))); err != nil {
				return err
			}
			if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", "import:"+source.OperationID, remaining)); err != nil {
				return err
			}
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind='image_import' AND status='pending'", "image_import:"+source.OperationID, source.OperationID)); err != nil {
			return err
		}
	}
	for _, intent := range step.State.Intents {
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='image_cleanup' AND body=?", intent.Path, step.OperationID, encode(intent))); err != nil {
			return err
		}
	}
	state := step.State
	state.Phase = "complete"
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status='pending'", []any{
			"image_cleanup:" + step.OperationID,
			step.OperationID,
			step.Input.Kind,
			step.Input.ResourceID,
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='image_cleanup' AND input_hash=? AND body=? AND status IN ('pending','running')", []any{
			encode(state),
			step.OperationID,
			step.Hash,
			encode(step.State),
		}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{
			step.OperationID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("image_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}
