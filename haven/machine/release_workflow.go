package machine

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (service *VMDeletion) releaseGenerationSet(step ResourceStep) ([]ResourceStep, error) {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.releaseProof(tx, step); err != nil {
		return nil, err
	}
	source, err := LoadResourceStep(service.ctx, tx, step.Input.SourceOperation)
	if err != nil {
		return nil, err
	}
	return service.proofs.CleanupGenerations(service.ctx, tx, source, "cleanup", step.OperationID+":resource")
}

func (service *VMDeletion) runRelease(step ResourceStep) error {
	generations, err := service.releaseGenerationSet(step)
	if err != nil {
		return err
	}
	r := step.Input.Resource
	if step.State.Phase == "admitted" {
		if err := service.markRelease(&step, "stop_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "stop_intent" {
		for _, generation := range slices.Backward(generations) {
			if err := service.backend.Stop(service.ctx, generation.Input.Resource); err != nil {
				return err
			}
		}
		for _, generation := range generations {
			observation, err := service.backend.Inspect(service.ctx, generation.Input.Resource)
			if err != nil {
				return err
			}
			if observation.Alive {
				return fmt.Errorf("%w: release known instance exit unconfirmed", errs.ErrUnknown)
			}
		}
		for _, generation := range generations {
			if err := service.backend.Network(service.ctx, generation.Input.Resource, false); err != nil {
				return err
			}
		}
		observation, err := service.backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: release exit or network shutdown unconfirmed", errs.ErrUnknown)
		}
		if err = service.fault("effect_stopped"); err != nil {
			return err
		}
		if err = service.markRelease(&step, "stopped"); err != nil {
			return err
		}
	}
	if step.State.Phase == "stopped" {
		if err := service.markRelease(&step, "remove_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "remove_intent" {
		for _, generation := range generations {
			if err := service.backend.Remove(service.ctx, generation.Input.Resource); err != nil {
				return err
			}
		}
		if err := service.fault("effect_removed"); err != nil {
			return err
		}
		observation, err := service.backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: backend removal remains unconfirmed", errs.ErrUnknown)
		}
		if err = service.markRelease(&step, "removed"); err != nil {
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
		paths, err := loom.ResumeDeletion(service.ctx, service.store.Directory, intent)
		if err != nil {
			return err
		}
		if err = loom.CheckLocalReaders(paths); err != nil {
			return err
		}
		for _, path := range paths {
			if err = loom.RemoveDeletionFile(service.ctx, service.store.Directory, intent, path); err != nil {
				return err
			}
			if err = service.fault("release_unlinked"); err != nil {
				return err
			}
		}
		if err = loom.RemoveDeletionDirectories(service.ctx, service.store.Directory, intent); err != nil {
			return err
		}
		if err = service.fault("effect_files_removed"); err != nil {
			return err
		}
		if err = service.markRelease(&step, "files_removed"); err != nil {
			return err
		}
	}
	if step.State.Phase != "files_removed" {
		return fmt.Errorf("unknown release resource phase")
	}
	if _, err := os.Lstat(r.Dir); !os.IsNotExist(err) {
		return fmt.Errorf("release files are not proven absent: %v", err)
	}
	observation, err := service.backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive || observation.Network {
		return fmt.Errorf("%w: release effect no longer matches absence proof", errs.ErrUnknown)
	}
	if err = service.inspectStoppedGenerations(generations); err != nil {
		return err
	}
	return service.completeRelease(step)
}

func (service *VMDeletion) deletionIntent(step ResourceStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	body, err := dbgen.New(service.store.Database).GetDeletionReceipt(service.ctx, dbgen.GetDeletionReceiptParams{
		Path:        step.Input.Resource.Dir,
		OperationID: step.OperationID,
		StepKey:     step.Key,
	})

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
		if err = VerifyGenerationDeletion(intent, generations); err != nil {
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
	intent, _, err = loom.InspectVMSetDeletion(service.ctx, service.store.Directory, GenerationDeletionMembers(generations), CaptureBoundaries(step))
	if err != nil {
		return intent, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return intent, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.releaseProof(tx, step); err != nil {
		return intent, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertDeletionReceipt(service.ctx, dbgen.InsertDeletionReceiptParams{
		Path:        intent.Path,
		OperationID: step.OperationID,
		StepKey:     step.Key,
		Body:        operation.Encode(intent),
	})); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return intent, err
	}
	return intent, service.fault("deletion_intent")
}

func (service *VMDeletion) completeRelease(step ResourceStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadResourceStep(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "files_removed" {
		return fmt.Errorf("release completion intent changed")
	}
	if _, err = service.releaseProof(tx, step); err != nil {
		return err
	}
	input := step.Input
	source, err := LoadResourceStep(service.ctx, tx, input.SourceOperation)
	if err != nil {
		return err
	}
	generations, err := service.proofs.CleanupGenerations(service.ctx, tx, source, "cleanup", step.OperationID+":resource")
	if err != nil {
		return err
	}
	quotas := []struct {
		kind    string
		key     string
		source  string
		slots   int64
		storage int64
	}{}
	if input.SlotDestination == nil {
		quotas = append(quotas, struct {
			kind    string
			key     string
			source  string
			slots   int64
			storage int64
		}{input.SlotBudget.Kind, input.SandboxID, input.SlotOperation, input.SlotBudget.Units, 0})
	} else {
		if err = quota.New(tx).TransferCostReference(service.ctx, quota.CostReference{
			TenantID:   input.TenantID,
			Kind:       "sandbox_slot",
			Key:        input.SandboxID,
			HolderKind: "cleanup",
			HolderID:   step.OperationID + ":resource",
		}, "active_use", SlotDestinationHolder(input.SlotDestination)); err != nil {
			return err
		}
	}
	for _, generation := range generations {
		quotas = append(quotas, struct {
			kind    string
			key     string
			source  string
			slots   int64
			storage int64
		}{"vm_disk", generation.Input.DiskKey, generation.OperationID, 0, generation.Input.Resource.Config.Shape.DiskMiB})
	}
	for _, charge := range quotas {
		if err = quota.New(tx).RemoveCostReference(service.ctx, quota.CostReference{
			TenantID:   input.TenantID,
			Kind:       charge.kind,
			Key:        charge.key,
			HolderKind: "cleanup",
			HolderID:   step.OperationID + ":resource",
		}); err != nil {
			return err
		}
		if err = quota.New(tx).RetireMatchingCost(service.ctx, quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        input.TenantID,
				Kind:            charge.kind,
				Key:             charge.key,
				SourceOperation: charge.source,
			},
			Slots:      charge.slots,
			StorageMiB: charge.storage,
		}); err != nil {
			return err
		}
	}
	if err = quota.New(tx).RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         step.Candidate,
		HolderKind: "cleanup",
		HolderID:   step.OperationID + ":resource",
	}); err != nil {
		return err
	}
	if err = quota.New(tx).RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         input.Resource.Config.Image,
		HolderKind: "vm",
		HolderID:   step.Candidate,
	}); err != nil {
		return err
	}
	for _, capture := range step.Input.Captures {
		if err = quota.New(tx).RemoveResourceReference(service.ctx, quota.ResourceReference{
			Kind:       "image",
			ID:         capture.Input.Source.Input.Resource.Config.Image,
			HolderKind: "capture",
			HolderID:   capture.OperationID,
		}); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).CompleteTransferredCleanup(service.ctx, dbgen.CompleteTransferredCleanupParams{
			ID:          "capture:" + capture.OperationID,
			OperationID: capture.OperationID,
		})); err != nil {
			return err
		}
	}
	if err = RetireVMPolicy(service.ctx, tx, generations, step.OperationID, service.proofs); err != nil {
		return err
	}
	for _, generation := range generations {
		if err = RetireGeneration(service.ctx, tx, generation, service.proofs.Snapshots()); err != nil {
			return err
		}
	}
	if err = quota.New(tx).ReleaseReservation(service.ctx, "vm:"+step.Candidate); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteStopped(service.ctx, dbgen.DeleteStoppedParams{
		ID:              step.Candidate,
		CurrentInstance: step.ExpectedInstance,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).RetireDeletionReceipt(service.ctx, dbgen.RetireDeletionReceiptParams{
		Path:        input.Resource.Dir,
		OperationID: step.OperationID,
		StepKey:     step.Key,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanup(service.ctx, dbgen.CompletePendingCleanupParams{
		ID:          "cleanup:" + step.OperationID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	next := step.State
	next.Phase = "complete"
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStepByInput(service.ctx, dbgen.CompleteRunningStepByInputParams{
		OperationID: step.OperationID,
		StepKey:     step.Key,
		InputHash:   step.Hash,
		Body:        operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = operation.New(tx).Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.handoff.CompleteRelease(service.ctx, tx, step); err != nil {
		return err
	}
	if err = service.fault("release_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("release_completed")
}
