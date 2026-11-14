package haven

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) releaseGenerationSet(step resourceStep) ([]resourceStep, error) {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.releaseProof(tx, step); err != nil {
		return nil, err
	}
	source, err := loadStep(service.ctx, tx, step.Input.SourceOperation)
	if err != nil {
		return nil, err
	}
	return service.cleanupGenerations(service.ctx, tx, source, "cleanup", step.OperationID+":resource")
}

func (service *Service) runRelease(step resourceStep) error {
	generations, err := service.releaseGenerationSet(step)
	if err != nil {
		return err
	}
	r := step.Input.Resource
	if step.State.Phase == "admitted" {
		if err := service.markPhase(&step, "stop_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "stop_intent" {
		for _, generation := range slices.Backward(generations) {
			if err := service.opts.Backend.Stop(service.ctx, generation.Input.Resource); err != nil {
				return err
			}
		}
		for _, generation := range generations {
			observation, err := service.opts.Backend.Inspect(service.ctx, generation.Input.Resource)
			if err != nil {
				return err
			}
			if observation.Alive {
				return fmt.Errorf("%w: release known instance exit unconfirmed", ErrUnknown)
			}
		}
		for _, generation := range generations {
			if err := service.opts.Backend.Network(service.ctx, generation.Input.Resource, false); err != nil {
				return err
			}
		}
		observation, err := service.opts.Backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: release exit or network shutdown unconfirmed", ErrUnknown)
		}
		if err = service.fault("effect_stopped"); err != nil {
			return err
		}
		if err = service.markPhase(&step, "stopped"); err != nil {
			return err
		}
	}
	if step.State.Phase == "stopped" {
		if err := service.markPhase(&step, "remove_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "remove_intent" {
		for _, generation := range generations {
			if err := service.opts.Backend.Remove(service.ctx, generation.Input.Resource); err != nil {
				return err
			}
		}
		if err := service.fault("effect_removed"); err != nil {
			return err
		}
		observation, err := service.opts.Backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: backend removal remains unconfirmed", ErrUnknown)
		}
		if err = service.markPhase(&step, "removed"); err != nil {
			return err
		}
	}
	if step.State.Phase == "removed" {
		if err = service.inspectStoppedGenerations(generations); err != nil {
			return err
		}
		intent, err := service.deletionIntent(step)
		if err != nil {
			return err
		}
		paths, err := loom.ResumeDeletion(service.ctx, service.store.directory, intent)
		if err != nil {
			return err
		}
		if err = loom.CheckLocalReaders(paths); err != nil {
			return err
		}
		for _, path := range paths {
			if err = loom.RemoveDeletionFile(service.ctx, service.store.directory, intent, path); err != nil {
				return err
			}
			if err = service.fault("release_unlinked"); err != nil {
				return err
			}
		}
		if err = loom.RemoveDeletionDirectories(service.ctx, service.store.directory, intent); err != nil {
			return err
		}
		if err = service.fault("effect_files_removed"); err != nil {
			return err
		}
		if err = service.markPhase(&step, "files_removed"); err != nil {
			return err
		}
	}
	if step.State.Phase != "files_removed" {
		return fmt.Errorf("unknown release resource phase")
	}
	if _, err := os.Lstat(r.Dir); !os.IsNotExist(err) {
		return fmt.Errorf("release files are not proven absent: %v", err)
	}
	observation, err := service.opts.Backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive || observation.Network {
		return fmt.Errorf("%w: release effect no longer matches absence proof", ErrUnknown)
	}
	if err = service.inspectStoppedGenerations(generations); err != nil {
		return err
	}
	return service.completeRelease(step)
}

func (service *Service) deletionIntent(step resourceStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	var body []byte
	err := service.store.db.QueryRowContext(service.ctx, "SELECT body FROM deletion_intents WHERE path=? AND operation_id=? AND step_key=?", step.Input.Resource.Dir, step.OperationID, step.Key).Scan(&body)
	if err == nil {
		if err = json.Unmarshal(body, &intent); err != nil {
			return intent, err
		}
		if intent.Format != 1 || intent.Path != step.Input.Resource.Dir || !intent.Dir {
			return intent, fmt.Errorf("deletion receipt differs from frozen candidate")
		}
		for _, capture := range step.Input.Captures {
			boundary := capture.State.Boundary
			if boundary == nil {
				return intent, fmt.Errorf("capture deletion witness missing")
			}
			if node, known := intent.Nodes["capture/"+capture.OperationID]; known && node != boundary.Parent {
				return intent, fmt.Errorf("deletion receipt differs from capture parent proof")
			}
		}
		if proof := step.Input.RestoreMaterials; proof != nil {
			version, _ := filepath.Rel(step.Input.Resource.Dir, filepath.Dir(step.Input.Resource.RestoreDir))
			fixed := map[string]loom.FileIdentity{
				version:              proof.Version,
				version + "/restore": proof.Restore,
			}
			for _, name := range loom.RestoreFileNames() {
				relative := version + "/restore/" + name
				if name == "disk.raw" {
					relative = version + "/disk.raw"
				}
				fixed[relative] = proof.Files[name].Identity
			}
			for relative, expected := range fixed {
				if node, known := intent.Nodes[relative]; known && node != expected {
					return intent, fmt.Errorf("deletion receipt differs from restore identity proof")
				}
			}
		}
		generations, err := service.releaseGenerationSet(step)
		if err != nil {
			return intent, err
		}
		if err = verifyGenerationDeletion(intent, generations); err != nil {
			return intent, err
		}
		return intent, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return intent, err
	}
	if step.Input.DiskIdentity == nil {
		return intent, fmt.Errorf("release lacks frozen disk identity")
	}
	generations, err := service.releaseGenerationSet(step)
	if err != nil {
		return intent, err
	}
	intent, _, err = loom.InspectVMSetDeletion(service.ctx, service.store.directory, generationDeletionMembers(generations), captureBoundaries(step))
	if err != nil {
		return intent, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return intent, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.releaseProof(tx, step); err != nil {
		return intent, err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO deletion_intents VALUES(?,?,?,?)", intent.Path, step.OperationID, step.Key, encode(intent))); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	if err = service.store.checkpoint(); err != nil {
		return intent, err
	}
	return intent, service.fault("deletion_intent")
}

func (service *Service) completeRelease(step resourceStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadStep(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "files_removed" {
		return fmt.Errorf("release completion intent changed")
	}
	if _, err = service.releaseProof(tx, step); err != nil {
		return err
	}
	input := step.Input
	source, err := loadStep(service.ctx, tx, input.SourceOperation)
	if err != nil {
		return err
	}
	generations, err := service.cleanupGenerations(service.ctx, tx, source, "cleanup", step.OperationID+":resource")
	if err != nil {
		return err
	}
	quotas := []struct {
		kind, key, source string
		slots, storage    int64
	}{}
	if input.SlotDestination == nil {
		quotas = append(quotas, struct {
			kind, key, source string
			slots, storage    int64
		}{input.SlotBudget.Kind, input.SandboxID, input.SlotOperation, input.SlotBudget.Units, 0})
	} else {
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE quota_refs SET holder_kind='active_use',holder_id=? WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND holder_kind='cleanup' AND holder_id=?", slotDestinationHolder(input.SlotDestination), input.TenantID, input.SandboxID, step.OperationID+":resource")); err != nil {
			return err
		}
	}
	for _, generation := range generations {
		quotas = append(quotas, struct {
			kind, key, source string
			slots, storage    int64
		}{"vm_disk", generation.Input.DiskKey, generation.OperationID, 0, generation.Input.Resource.Config.Shape.DiskMiB})
	}
	for _, quota := range quotas {
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind='cleanup' AND holder_id=?", input.TenantID, quota.kind, quota.key, step.OperationID+":resource")); err != nil {
			return err
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=? AND sandbox_slots=? AND storage_mib=? AND source_operation_id=?", input.TenantID, quota.kind, quota.key, quota.slots, quota.storage, quota.source)); err != nil {
			return err
		}
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='cleanup' AND holder_id=?", step.Candidate, step.OperationID+":resource")); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='vm' AND holder_id=?", input.Resource.Config.Image, step.Candidate)); err != nil {
		return err
	}
	for _, capture := range step.Input.Captures {
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='capture' AND holder_id=?", capture.Input.Source.Input.Resource.Config.Image, capture.OperationID)); err != nil {
			return err
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='transferred'", "capture:"+capture.OperationID, capture.OperationID)); err != nil {
			return err
		}
	}
	if err = retireVMPolicy(service.ctx, tx, generations, step.OperationID); err != nil {
		return err
	}
	for _, generation := range generations {
		if err = service.retireGeneration(service.ctx, tx, generation); err != nil {
			return err
		}
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM reservations WHERE owner=?", "vm:"+step.Candidate)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM vms WHERE id=? AND operation_id=? AND current_instance=? AND state='stopped'", step.Candidate, step.OperationID, step.ExpectedInstance)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key=?", input.Resource.Dir, step.OperationID, step.Key)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='pending'", "cleanup:"+step.OperationID, step.OperationID)); err != nil {
		return err
	}
	next := step.State
	next.Phase = "complete"
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key=? AND status='running' AND input_hash=?", encode(next), step.OperationID, step.Key, step.Hash)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", step.OperationID)); err != nil {
		return err
	}
	if input.SlotDestination != nil && input.SlotDestination.Kind == "sleep" {
		parent, err := loadLifecycle(service.ctx, tx, input.SlotDestination.OperationID)
		if err != nil {
			return err
		}
		if parent.Input.Command.Action == "resume" {
			if err = service.restorePausedSandbox(tx, input.SlotDestination); err != nil {
				return err
			}
		}
	}
	if input.SlotDestination != nil && input.SlotDestination.Kind == "claim" {
		if err = service.retireSelectedPool(tx, step); err != nil {
			return err
		}
	}
	if err = service.fault("release_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("release_completed")
}
