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

func (service *Service) markDiscard(step *discardStep, phase string) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	vm, err := service.discardProof(service.ctx, tx, *step)
	if err != nil {
		return err
	}
	next := discardState{
		Phase: phase,
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='discard' AND input_hash=? AND status IN ('pending','running') AND body=?", encode(next), step.OperationID, step.Hash, encode(step.State))); err != nil {
		return err
	}
	if phase == "stopped" {
		vm.AgentReady = false
		vm.Verified = true
		vm.Observation = nil
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE vms SET state='stopped',body=? WHERE id=? AND operation_id=? AND current_instance=?", encode(vm), step.Input.Source.Candidate, step.OperationID, step.Input.CurrentInstance)); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State, step.Status = next, "running"
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("discard_" + phase)
}

func (service *Service) discardGenerationSet(step discardStep) ([]resourceStep, error) {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.discardProof(service.ctx, tx, step); err != nil {
		return nil, err
	}
	return service.cleanupGenerations(service.ctx, tx, step.Input.Source, "cleanup", step.OperationID+":discard")
}

func (service *Service) runDiscard(step discardStep) error {
	if terminal(step.Status) {
		return nil
	}
	generations, err := service.discardGenerationSet(step)
	if err != nil {
		return err
	}
	service.admission.Lock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err == nil {
		_, err = service.discardProof(service.ctx, tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	r := step.Input.Source.Input.Resource
	if step.State.Phase == "admitted" {
		if err = service.markDiscard(&step, "stop_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "stop_intent" {
		// All journal-owned instances stop before inspection authorizes mutation.
		for _, generation := range slices.Backward(generations) {
			if err = service.opts.Backend.Stop(service.ctx, generation.Input.Resource); err != nil {
				return err
			}
		}
		for _, generation := range generations {
			observation, err := service.opts.Backend.Inspect(service.ctx, generation.Input.Resource)
			if err != nil {
				return err
			}
			if observation.Alive {
				return fmt.Errorf("%w: discard known instance exit unconfirmed", ErrUnknown)
			}
		}
		observation, err := service.opts.Backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive {
			return fmt.Errorf("%w: discard cannot prove candidate exit", ErrUnknown)
		}
		for _, generation := range generations {
			if err = service.opts.Backend.Network(service.ctx, generation.Input.Resource, false); err != nil {
				return err
			}
		}
		observation, err = service.opts.Backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: discard device shutdown unconfirmed", ErrUnknown)
		}
		if err = service.fault("discard_effect_stopped"); err != nil {
			return err
		}
		if err = service.markDiscard(&step, "stopped"); err != nil {
			return err
		}
	}
	if step.State.Phase == "stopped" {
		if err = service.markDiscard(&step, "remove_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "remove_intent" {
		for _, generation := range generations {
			if err = service.opts.Backend.Remove(service.ctx, generation.Input.Resource); err != nil {
				return err
			}
		}
		observation, err := service.opts.Backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: discard host removal unconfirmed", ErrUnknown)
		}
		if err = service.fault("discard_effect_removed"); err != nil {
			return err
		}
		if err = service.markDiscard(&step, "removed"); err != nil {
			return err
		}
	}
	if step.State.Phase == "removed" {
		if err = service.inspectStoppedGenerations(generations); err != nil {
			return err
		}
		intent, err := service.discardDeletion(step)
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
			if err = service.fault("discard_unlinked"); err != nil {
				return err
			}
		}
		if err = loom.RemoveDeletionDirectories(service.ctx, service.store.directory, intent); err != nil {
			return err
		}
		if err = service.fault("discard_effect_files_removed"); err != nil {
			return err
		}
		if err = service.markDiscard(&step, "files_removed"); err != nil {
			return err
		}
	}
	if step.State.Phase != "files_removed" {
		return fmt.Errorf("unknown discard phase")
	}
	if _, err = os.Lstat(r.Dir); !os.IsNotExist(err) {
		return fmt.Errorf("discard candidate files not proven absent: %v", err)
	}
	observation, err := service.opts.Backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive || observation.Network {
		return fmt.Errorf("%w: discard absence proof no longer matches host", ErrUnknown)
	}
	if err = service.inspectStoppedGenerations(generations); err != nil {
		return err
	}
	return service.completeDiscard(step)
}

func discardFixedNodes(source resourceStep) map[string]loom.FileIdentity {
	fixed := map[string]loom.FileIdentity{}
	r := source.Input.Resource
	if source.State.DiskIdentity != nil {
		relative, _ := filepath.Rel(r.Dir, r.Disk)
		fixed[relative] = *source.State.DiskIdentity
	}
	if proof := source.State.Materials; proof != nil {
		version, _ := filepath.Rel(r.Dir, filepath.Dir(r.RestoreDir))
		fixed[version] = proof.Version
		fixed[version+"/restore"] = proof.Restore
		for _, name := range loom.RestoreFileNames() {
			file := proof.Files[name]
			if file.Candidate == "" && name != "memory-ranges" {
				continue
			}
			relative := version + "/restore/" + name
			if name == "disk.raw" {
				relative = version + "/disk.raw"
			}
			fixed[relative] = file.Identity
		}
	}
	return fixed
}

func (service *Service) discardDeletion(step discardStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	var body []byte
	source := step.Input.Source
	r := source.Input.Resource
	err := service.store.db.QueryRowContext(service.ctx, "SELECT body FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='discard'", r.Dir, step.OperationID).Scan(&body)
	if err == nil {
		if err = json.Unmarshal(body, &intent); err != nil {
			return intent, err
		}
		if intent.Format != 1 || intent.Path != r.Dir || !intent.Dir {
			return intent, fmt.Errorf("discard deletion receipt changed")
		}
		for relative, expected := range discardFixedNodes(source) {
			if actual, known := intent.Nodes[relative]; known && actual != expected {
				return intent, fmt.Errorf("discard receipt differs from original identity")
			}
		}
		generations, err := service.discardGenerationSet(step)
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
	generations, err := service.discardGenerationSet(step)
	if err != nil {
		return intent, err
	}
	intent, _, err = loom.InspectVMSetDeletion(service.ctx, service.store.directory, generationDeletionMembers(generations), nil)
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
	if _, err = service.discardProof(service.ctx, tx, step); err != nil {
		return intent, err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO deletion_intents VALUES(?,?,'discard',?)", r.Dir, step.OperationID, encode(intent))); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	if err = service.store.checkpoint(); err != nil {
		return intent, err
	}
	return intent, service.fault("discard_deletion_intent")
}

func (service *Service) completeDiscard(step discardStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadDiscard(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "files_removed" {
		return fmt.Errorf("discard completion identity changed")
	}
	if _, err = service.discardProof(service.ctx, tx, step); err != nil {
		return err
	}
	source, input := step.Input.Source, step.Input
	generations, err := service.cleanupGenerations(service.ctx, tx, source, "cleanup", step.OperationID+":discard")
	if err != nil {
		return err
	}
	quotas := []struct {
		kind, key, operation string
		slots, storage       int64
	}{
		{"pending_storage", source.OperationID + ":resource", source.OperationID, 0, source.Input.Resource.Config.Shape.DiskMiB},
		{source.Input.SlotBudget.Kind, input.Request.SandboxID, source.Input.SlotOperation, source.Input.SlotBudget.Units, 0},
	}
	for _, generation := range generations[:len(generations)-1] {
		quotas = append(quotas, struct {
			kind, key, operation string
			slots, storage       int64
		}{"vm_disk", generation.Input.DiskKey, generation.OperationID, 0, generation.Input.Resource.Config.Shape.DiskMiB})
	}
	for _, quota := range quotas {
		if quota.kind == "sandbox_slot" && source.Input.FailureSlotDestination != nil {
			if err = service.restoreResumeSlot(tx, source.Input.FailureSlotDestination, step.OperationID+":discard"); err != nil {
				return err
			}
			continue
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind='cleanup' AND holder_id=?", input.Request.TenantID, quota.kind, quota.key, step.OperationID+":discard")); err != nil {
			return err
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=? AND sandbox_slots=? AND storage_mib=? AND source_operation_id=?", input.Request.TenantID, quota.kind, quota.key, quota.slots, quota.storage, quota.operation)); err != nil {
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
	if len(source.Input.Ancestors) > 0 {
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='vm' AND holder_id=?", source.Input.Resource.Config.Image, source.Candidate)); err != nil {
			return err
		}
	}
	next := discardState{
		Phase: "complete",
	}
	statements := []struct {
		q    string
		args []any
	}{
		{"DELETE FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='cleanup' AND holder_id=?", []any{
			source.Candidate,
			step.OperationID + ":discard",
		}},
		{"DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='cleanup' AND holder_id=?", []any{
			source.Input.Resource.Config.Image,
			step.OperationID + ":discard",
		}},
		{"DELETE FROM reservations WHERE owner=?", []any{
			"vm:" + source.Candidate,
		}},
		{"DELETE FROM vms WHERE id=? AND operation_id=? AND current_instance=? AND state='stopped'", []any{
			source.Candidate,
			step.OperationID,
			input.CurrentInstance,
		}},
		{"DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='discard'", []any{
			source.Input.Resource.Dir,
			step.OperationID,
		}},
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='transferred'", []any{
			"blocked:" + source.OperationID,
			source.OperationID,
		}},
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='pending'", []any{
			"discard:" + step.OperationID,
			step.OperationID,
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='discard' AND input_hash=? AND body=? AND status='running'", []any{
			encode(next),
			step.OperationID,
			step.Hash,
			encode(step.State),
		}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{
			step.OperationID,
		}},
	}
	for _, statement := range statements {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if source.Input.FailureSlotDestination != nil {
		if err = service.restorePausedSandbox(tx, source.Input.FailureSlotDestination); err != nil {
			return err
		}
	}
	if err = service.fault("discard_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("discard_completed")
}
