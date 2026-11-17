package machine

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func captureDiscardPaths(step CaptureDiscardStep) []string {
	source := step.Input.Source
	return []string{
		filepath.Join(source.Input.Source.Input.Resource.Dir, "capture", source.OperationID),
		source.Input.Snapshot.Dir,
	}
}

func (service *CaptureDiscard) checkCaptureDiscard(step CaptureDiscardStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadCaptureDiscard(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("capture discard current intent changed")
	}
	return service.captureDiscardProof(service.ctx, tx, step, false)
}

func (service *CaptureDiscard) inspectCaptureDiscardSource(step CaptureDiscardStep) error {
	service.admission.Lock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	var history []ResourceStep
	if err == nil {
		err = service.captureDiscardProof(service.ctx, tx, step, false)
		if err == nil {
			history, err = service.proofs.GenerationHistory(service.ctx, tx, step.Input.Source.Input.Source)
		}
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	return service.inspectStoppedGenerations(append(history, step.Input.Source.Input.Source))
}

func (service *CaptureDiscard) captureDiscardReceipts(step *CaptureDiscardStep) ([]loom.DeletionIntent, error) {
	paths := captureDiscardPaths(*step)
	var intents []loom.DeletionIntent
	missing := 0
	for _, path := range paths {
		body, err := dbgen.New(service.store.Database).GetDeletionReceipt(service.ctx, dbgen.GetDeletionReceiptParams{
			Path:        path,
			OperationID: step.OperationID,
			StepKey:     "capture_discard",
		})

		if errors.Is(err, sql.ErrNoRows) {
			missing++
			continue
		}
		if err != nil {
			return nil, err
		}
		var intent loom.DeletionIntent
		if err = json.Unmarshal(body, &intent); err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	if missing == 0 {
		if step.Phase != "deleting" && step.Phase != "files_removed" {
			return nil, fmt.Errorf("capture discard receipts outside deletion phase")
		}
		return intents, loom.VerifyCaptureDeletion(intents, paths[0], paths[1], step.Input.Source.State.Boundary)
	}
	if missing != 2 || step.Phase != "admitted" {
		return nil, fmt.Errorf("capture discard incomplete durable receipt set")
	}
	source := step.Input.Source
	intents, err := loom.InspectCaptureDeletion(service.ctx, service.store.Directory, source.Input.Source.Input.Resource, source.OperationID, source.Input.Snapshot.ID, source.State.Boundary)
	if err != nil {
		return nil, err
	}
	if err = loom.VerifyCaptureDeletion(intents, paths[0], paths[1], source.State.Boundary); err != nil {
		return nil, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadCaptureDiscard(service.ctx, tx, step.OperationID)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(current, *step) {
		return nil, fmt.Errorf("capture discard receipt intent changed")
	}
	if err = service.captureDiscardProof(service.ctx, tx, *step, false); err != nil {
		return nil, err
	}

	for _, intent := range intents {
		if err = store.RequireOne(dbgen.New(tx).InsertDeletionReceipt(service.ctx, dbgen.InsertDeletionReceiptParams{
			Path:        intent.Path,
			OperationID: step.OperationID,
			StepKey:     "capture_discard",
			Body:        operation.Encode(intent),
		})); err != nil {
			return nil, err
		}
	}
	if err = store.RequireOne(dbgen.New(tx).BeginPendingStep(service.ctx, dbgen.BeginPendingStepParams{
		OperationID:  step.OperationID,
		StepKey:      "capture_discard",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NextBody:     operation.Encode("deleting"),
	})); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	step.Phase = "deleting"
	step.Status = "running"
	if err = service.store.Checkpoint(); err != nil {
		return nil, err
	}
	return intents, service.fault("capture_discard_receipts")
}

func (service *CaptureDiscard) runCaptureDiscard(step CaptureDiscardStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if err := service.checkCaptureDiscard(step); err != nil {
		return err
	}
	if err := service.inspectCaptureDiscardSource(step); err != nil {
		return err
	}
	intents, err := service.captureDiscardReceipts(&step)
	if err != nil {
		return err
	}
	if step.Phase == "deleting" {
		// Inventory both destinations before unlinking either. The VMM has exited;
		// surviving readers still retain all accounting and immutable receipts.
		var allPaths []string
		for _, intent := range intents {
			paths, err := loom.ResumeDeletion(service.ctx, service.store.Directory, intent)
			if err != nil {
				return err
			}
			allPaths = append(allPaths, paths...)
		}
		if err = loom.CheckLocalReaders(allPaths); err != nil {
			return err
		}
		if err = service.inspectCaptureDiscardSource(step); err != nil {
			return err
		}
		for _, intent := range intents {
			paths, err := loom.ResumeDeletion(service.ctx, service.store.Directory, intent)
			if err != nil {
				return err
			}
			for _, path := range paths {
				if err = loom.RemoveDeletionFile(service.ctx, service.store.Directory, intent, path); err != nil {
					return err
				}
				if err = service.fault("capture_discard_unlinked"); err != nil {
					return err
				}
			}
			if err = loom.RemoveDeletionDirectories(service.ctx, service.store.Directory, intent); err != nil {
				return err
			}
			if err = service.fault("capture_discard_directory_removed"); err != nil {
				return err
			}
		}
		if err = service.markCaptureDiscardRemoved(&step); err != nil {
			return err
		}
	}
	if step.Phase != "files_removed" {
		return fmt.Errorf("unsupported capture discard phase")
	}
	for _, path := range captureDiscardPaths(step) {
		if _, err = os.Lstat(path); !os.IsNotExist(err) {
			return fmt.Errorf("capture discard physical absence unconfirmed: %v", err)
		}
	}
	if err = service.inspectCaptureDiscardSource(step); err != nil {
		return err
	}
	return service.completeCaptureDiscard(step)
}

func (service *CaptureDiscard) markCaptureDiscardRemoved(step *CaptureDiscardStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.captureDiscardProof(service.ctx, tx, *step, false); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceRunningStepBody(service.ctx, dbgen.ReplaceRunningStepBodyParams{
		OperationID:  step.OperationID,
		StepKey:      "capture_discard",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NextBody:     operation.Encode("files_removed"),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.Phase = "files_removed"
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("capture_discard_files_removed")
}

func (service *CaptureDiscard) completeCaptureDiscard(step CaptureDiscardStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadCaptureDiscard(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.Phase != "files_removed" {
		return fmt.Errorf("capture discard completion changed")
	}
	if err = service.captureDiscardProof(service.ctx, tx, step, false); err != nil {
		return err
	}
	source := step.Input.Source
	publication := source.Input.Snapshot
	resource := source.Input.Source
	body, previousState, completed := operation.Encode(publication), operation.Encode(step.Phase), operation.Encode("complete")
	responsibilities := quota.New(tx)
	operations := operation.New(tx)
	if err = responsibilities.RemoveCostReference(service.ctx, quota.CostReference{
		TenantID:   publication.TenantID,
		Kind:       "pending_storage",
		Key:        source.OperationID + ":capture",
		HolderKind: "cleanup",
		HolderID:   step.OperationID + ":capture_discard",
	}); err != nil {
		return err
	}
	if err = responsibilities.RetireCost(service.ctx, quota.CostIdentity{
		TenantID:        publication.TenantID,
		Kind:            "pending_storage",
		Key:             source.OperationID + ":capture",
		SourceOperation: source.OperationID,
	}); err != nil {
		return err
	}
	if err = responsibilities.ReleaseReservation(service.ctx, "snapshot:"+publication.ID); err != nil {
		return err
	}
	if err = responsibilities.RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         resource.Candidate,
		HolderKind: "capture",
		HolderID:   source.OperationID,
	}); err != nil {
		return err
	}
	if err = responsibilities.RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         resource.Input.Resource.Config.Image,
		HolderKind: "capture",
		HolderID:   source.OperationID,
	}); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteSnapshot(service.ctx, dbgen.DeleteSnapshotParams{
		ID:        publication.ID,
		Published: 0,
		Body:      body,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteTransferredCleanup(service.ctx, dbgen.CompleteTransferredCleanupParams{
		ID:          "capture:" + source.OperationID,
		OperationID: source.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanup(service.ctx, dbgen.CompletePendingCleanupParams{
		ID:          "capture_discard:" + step.OperationID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReleaseStoppedOwnership(service.ctx, dbgen.ReleaseStoppedOwnershipParams{
		ID:              resource.Candidate,
		CurrentInstance: step.Input.Request.ExpectedInstance,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
	})); err != nil {
		return err
	}
	for _, path := range captureDiscardPaths(step) {
		if err = store.RequireOne(dbgen.New(tx).RetireDeletionReceipt(service.ctx, dbgen.RetireDeletionReceiptParams{
			Path:        path,
			OperationID: step.OperationID,
			StepKey:     "capture_discard",
		})); err != nil {
			return err
		}
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(service.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      "capture_discard",
		InputHash:    step.Hash,
		PreviousBody: previousState,
		NewBody:      completed,
	})); err != nil {
		return err
	}
	if err = operations.Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("capture_discard_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("capture_discard_completed")
}
