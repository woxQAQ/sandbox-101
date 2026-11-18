package machine

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (d *Deletion) markDiscard(step *DiscardStep, phase string) error {
	d.admission.Lock()
	defer d.admission.Unlock()
	tx, err := d.store.Database.BeginTx(d.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	currentMachine, err := d.discardProof(d.ctx, tx, *step)
	if err != nil {
		return err
	}
	next := discardState{
		Phase: phase,
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(d.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "discard",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if phase == "stopped" {
		currentMachine.AgentReady = false
		currentMachine.Verified = true
		currentMachine.Observation = nil
		if err = store.RequireOne(dbgen.New(tx).RecordStopped(d.ctx, dbgen.RecordStoppedParams{
			ID:              step.Input.Source.Candidate,
			CurrentInstance: step.Input.CurrentInstance,
			OperationID: sql.NullString{
				String: step.OperationID,
				Valid:  true,
			},
			Body: operation.Encode(currentMachine),
		})); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State, step.Status = next, "running"
	if err = d.store.Checkpoint(); err != nil {
		return err
	}
	return d.fault("discard_" + phase)
}

func (d *Deletion) discardGenerationSet(step DiscardStep) ([]ResourceStep, error) {
	d.admission.Lock()
	defer d.admission.Unlock()
	tx, err := d.store.Database.BeginTx(d.ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = d.discardProof(d.ctx, tx, step); err != nil {
		return nil, err
	}
	return d.proofs.CleanupGenerations(d.ctx, tx, step.Input.Source, quota.Holder{
		Kind: "cleanup",
		ID:   step.OperationID + ":discard",
	})
}

func (d *Deletion) runDiscard(step DiscardStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	generations, err := d.discardGenerationSet(step)
	if err != nil {
		return err
	}
	d.admission.Lock()
	tx, err := d.store.Database.BeginTx(d.ctx, nil)
	if err == nil {
		_, err = d.discardProof(d.ctx, tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	d.admission.Unlock()
	if err != nil {
		return err
	}
	r := step.Input.Source.Input.Resource
	if step.State.Phase == "admitted" {
		if err = d.markDiscard(&step, "stop_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "stop_intent" {
		// All journal-owned instances stop before inspection authorizes mutation.
		for _, generation := range slices.Backward(generations) {
			if err = d.backend.Stop(d.ctx, generation.Input.Resource); err != nil {
				return err
			}
		}
		for _, generation := range generations {
			observation, err := d.backend.Inspect(d.ctx, generation.Input.Resource)
			if err != nil {
				return err
			}
			if observation.Alive {
				return fmt.Errorf("%w: discard known instance exit unconfirmed", errs.ErrUnknown)
			}
		}
		observation, err := d.backend.Inspect(d.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive {
			return fmt.Errorf("%w: discard cannot prove candidate exit", errs.ErrUnknown)
		}
		for _, generation := range generations {
			if err = d.backend.Network(d.ctx, generation.Input.Resource, false); err != nil {
				return err
			}
		}
		observation, err = d.backend.Inspect(d.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: discard device shutdown unconfirmed", errs.ErrUnknown)
		}
		if err = d.fault("discard_effect_stopped"); err != nil {
			return err
		}
		if err = d.markDiscard(&step, "stopped"); err != nil {
			return err
		}
	}
	if step.State.Phase == "stopped" {
		if err = d.markDiscard(&step, "remove_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "remove_intent" {
		for _, generation := range generations {
			if err = d.backend.Remove(d.ctx, generation.Input.Resource); err != nil {
				return err
			}
		}
		observation, err := d.backend.Inspect(d.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: discard host removal unconfirmed", errs.ErrUnknown)
		}
		if err = d.fault("discard_effect_removed"); err != nil {
			return err
		}
		if err = d.markDiscard(&step, "removed"); err != nil {
			return err
		}
	}
	if step.State.Phase == "removed" {
		if err = d.inspectStoppedGenerations(generations); err != nil {
			return err
		}
		intent, err := d.discardDeletion(step)
		if err != nil {
			return err
		}
		paths, err := loom.ResumeDeletion(d.ctx, d.store.Directory, intent)
		if err != nil {
			return err
		}
		if err = loom.CheckLocalReaders(paths); err != nil {
			return err
		}
		for _, path := range paths {
			if err = loom.RemoveDeletionFile(d.ctx, d.store.Directory, intent, path); err != nil {
				return err
			}
			if err = d.fault("discard_unlinked"); err != nil {
				return err
			}
		}
		if err = loom.RemoveDeletionDirectories(d.ctx, d.store.Directory, intent); err != nil {
			return err
		}
		if err = d.fault("discard_effect_files_removed"); err != nil {
			return err
		}
		if err = d.markDiscard(&step, "files_removed"); err != nil {
			return err
		}
	}
	if step.State.Phase != "files_removed" {
		return fmt.Errorf("unknown discard phase")
	}
	if _, err = os.Lstat(r.Dir); !os.IsNotExist(err) {
		return fmt.Errorf("discard candidate files not proven absent: %v", err)
	}
	observation, err := d.backend.Inspect(d.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive || observation.Network {
		return fmt.Errorf("%w: discard absence proof no longer matches host", errs.ErrUnknown)
	}
	if err = d.inspectStoppedGenerations(generations); err != nil {
		return err
	}
	return d.completeDiscard(step)
}

func (d *Deletion) discardDeletion(step DiscardStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	source := step.Input.Source
	r := source.Input.Resource
	body, err := dbgen.New(d.store.Database).GetDeletionReceipt(d.ctx, dbgen.GetDeletionReceiptParams{
		Path:        r.Dir,
		OperationID: step.OperationID,
		StepKey:     "discard",
	})

	if err == nil {
		if err = json.Unmarshal(body, &intent); err != nil {
			return intent, err
		}
		if intent.Format != 1 || intent.Path != r.Dir || !intent.Dir {
			return intent, fmt.Errorf("discard deletion receipt changed")
		}
		for relative, expected := range GenerationFixedNodes(source) {
			if actual, known := intent.Nodes[relative]; known && actual != expected {
				return intent, fmt.Errorf("discard receipt differs from original identity")
			}
		}
		generations, err := d.discardGenerationSet(step)
		if err != nil {
			return intent, err
		}
		if err = VerifyGenerationDeletion(intent, generations); err != nil {
			return intent, err
		}
		return intent, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return intent, err
	}
	generations, err := d.discardGenerationSet(step)
	if err != nil {
		return intent, err
	}
	intent, _, err = loom.InspectVMSetDeletion(d.ctx, d.store.Directory, GenerationDeletionMembers(generations), nil)
	if err != nil {
		return intent, err
	}
	d.admission.Lock()
	defer d.admission.Unlock()
	tx, err := d.store.Database.BeginTx(d.ctx, nil)
	if err != nil {
		return intent, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = d.discardProof(d.ctx, tx, step); err != nil {
		return intent, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertDeletionReceipt(d.ctx, dbgen.InsertDeletionReceiptParams{
		Path:        r.Dir,
		OperationID: step.OperationID,
		StepKey:     "discard",
		Body:        operation.Encode(intent),
	})); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	if err = d.store.Checkpoint(); err != nil {
		return intent, err
	}
	return intent, d.fault("discard_deletion_intent")
}

func (d *Deletion) completeDiscard(step DiscardStep) error {
	d.admission.Lock()
	defer d.admission.Unlock()
	tx, err := d.store.Database.BeginTx(d.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadDiscard(d.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "files_removed" {
		return fmt.Errorf("discard completion identity changed")
	}
	if _, err = d.discardProof(d.ctx, tx, step); err != nil {
		return err
	}
	source, input := step.Input.Source, step.Input
	generations, err := d.proofs.CleanupGenerations(d.ctx, tx, source, quota.Holder{
		Kind: "cleanup",
		ID:   step.OperationID + ":discard",
	})
	if err != nil {
		return err
	}
	quotas := []struct {
		kind      string
		key       string
		operation string
		slots     int64
		storage   int64
	}{
		{"pending_storage", source.OperationID + ":resource", source.OperationID, 0, source.Input.Resource.Config.Shape.DiskMiB},
		{source.Input.SlotBudget.Kind, input.Request.SandboxID, source.Input.SlotOperation, source.Input.SlotBudget.Units, 0},
	}
	for _, generation := range generations[:len(generations)-1] {
		quotas = append(quotas, struct {
			kind      string
			key       string
			operation string
			slots     int64
			storage   int64
		}{"vm_disk", generation.Input.DiskKey, generation.OperationID, 0, generation.Input.Resource.Config.Shape.DiskMiB})
	}
	for _, charge := range quotas {
		if charge.kind == "sandbox_slot" && source.Input.FailureSlotDestination != nil {
			if err = d.handoff.RestoreResumeSlot(d.ctx, tx, source.Input.FailureSlotDestination, step.OperationID+":discard"); err != nil {
				return err
			}
			continue
		}
		if err = quota.New(tx).RemoveCostReference(d.ctx, quota.CostReference{
			TenantID:   input.Request.TenantID,
			Kind:       charge.kind,
			Key:        charge.key,
			HolderKind: "cleanup",
			HolderID:   step.OperationID + ":discard",
		}); err != nil {
			return err
		}
		if err = quota.New(tx).RetireMatchingCost(d.ctx, quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        input.Request.TenantID,
				Kind:            charge.kind,
				Key:             charge.key,
				SourceOperation: charge.operation,
			},
			Slots:      charge.slots,
			StorageMiB: charge.storage,
		}); err != nil {
			return err
		}
	}
	if err = d.proofs.RetireVMPolicy(d.ctx, tx, generations, step.OperationID); err != nil {
		return err
	}
	for _, generation := range generations {
		if err = RetireGeneration(d.ctx, tx, generation, d.proofs.Snapshots()); err != nil {
			return err
		}
	}
	if len(source.Input.Ancestors) > 0 {
		if err = quota.New(tx).RemoveResourceReference(d.ctx, quota.ResourceReference{
			Kind:       "image",
			ID:         source.Input.Resource.Config.Image,
			HolderKind: "vm",
			HolderID:   source.Candidate,
		}); err != nil {
			return err
		}
	}
	next := discardState{
		Phase: "complete",
	}
	responsibilities := quota.New(tx)
	if err = responsibilities.RemoveResourceReference(d.ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         source.Candidate,
		HolderKind: "cleanup",
		HolderID:   step.OperationID + ":discard",
	}); err != nil {
		return err
	}
	if err = responsibilities.RemoveResourceReference(d.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         source.Input.Resource.Config.Image,
		HolderKind: "cleanup",
		HolderID:   step.OperationID + ":discard",
	}); err != nil {
		return err
	}
	if err = responsibilities.ReleaseReservation(d.ctx, "vm:"+source.Candidate); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteStopped(d.ctx, dbgen.DeleteStoppedParams{
		ID:              source.Candidate,
		CurrentInstance: input.CurrentInstance,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
	})); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).RetireDeletionReceipt(d.ctx, dbgen.RetireDeletionReceiptParams{
		Path:        source.Input.Resource.Dir,
		OperationID: step.OperationID,
		StepKey:     "discard",
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteTransferredCleanup(d.ctx, dbgen.CompleteTransferredCleanupParams{
		ID:          "blocked:" + source.OperationID,
		OperationID: source.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanup(d.ctx, dbgen.CompletePendingCleanupParams{
		ID:          "discard:" + step.OperationID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(d.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      "discard",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = operations.Complete(d.ctx, step.OperationID); err != nil {
		return err
	}
	if source.Input.FailureSlotDestination != nil {
		if err = d.handoff.RestorePausedSandbox(d.ctx, tx, source.Input.FailureSlotDestination); err != nil {
			return err
		}
	}
	if err = d.fault("discard_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = d.store.Checkpoint(); err != nil {
		return err
	}
	return d.fault("discard_completed")
}
