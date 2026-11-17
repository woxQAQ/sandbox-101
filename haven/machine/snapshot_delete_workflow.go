package machine

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (service *SnapshotDeletion) checkSnapshotDelete(step SnapshotDeleteStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSnapshotDelete(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("snapshot delete current intent changed")
	}
	_, err = service.snapshotDeleteProof(service.ctx, tx, step, false)
	return err
}

func (service *SnapshotDeletion) snapshotDeleteReceipt(step *SnapshotDeleteStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	source := step.Input.Source
	body, err := dbgen.New(service.store.Database).GetDeletionReceipt(service.ctx, dbgen.GetDeletionReceiptParams{
		Path:        source.Snapshot.Dir,
		OperationID: step.OperationID,
		StepKey:     "snapshot_delete",
	})

	if err == nil {
		if step.Phase != "deleting" && step.Phase != "files_removed" {
			return intent, fmt.Errorf("snapshot deletion receipt outside phase")
		}
		if err = json.Unmarshal(body, &intent); err != nil {
			return intent, err
		}
		return intent, loom.VerifySnapshotDeletion(intent, source.Snapshot.Dir, source.Publication)
	}
	if !errors.Is(err, sql.ErrNoRows) || step.Phase != "admitted" {
		return intent, fmt.Errorf("snapshot deletion receipt unavailable: %w", err)
	}
	intent, err = loom.InspectSnapshotDeletion(service.ctx, service.store.Directory, loom.SnapshotDeletionTarget{
		ID:          source.Snapshot.ID,
		Publication: source.Publication,
		Files:       source.Snapshot.Manifest.Files,
	})
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
	current, err := LoadSnapshotDelete(service.ctx, tx, step.OperationID)
	if err != nil {
		return intent, err
	}
	if !reflect.DeepEqual(current, *step) {
		return intent, fmt.Errorf("snapshot deletion receipt intent changed")
	}
	backings, err := service.snapshotDeleteProof(service.ctx, tx, *step, false)
	if err != nil {
		return intent, err
	}
	for _, backing := range backings {
		if backing.State.Materials == nil || backing.State.Materials.Files["memory-ranges"].Identity != intent.Nodes["ch/memory-ranges"] {
			return intent, fmt.Errorf("snapshot retained memory differs from published inode")
		}
	}
	if err = store.RequireOne(dbgen.New(tx).InsertDeletionReceipt(service.ctx, dbgen.InsertDeletionReceiptParams{
		Path:        intent.Path,
		OperationID: step.OperationID,
		StepKey:     "snapshot_delete",
		Body:        operation.Encode(intent),
	})); err != nil {
		return intent, err
	}
	if err = store.RequireOne(dbgen.New(tx).BeginPendingStep(service.ctx, dbgen.BeginPendingStepParams{
		OperationID:  step.OperationID,
		StepKey:      "snapshot_delete",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NextBody:     operation.Encode("deleting"),
	})); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	step.Phase = "deleting"
	step.Status = "running"
	if err = service.store.Checkpoint(); err != nil {
		return intent, err
	}
	return intent, service.fault("snapshot_delete_receipt")
}

func (service *SnapshotDeletion) runSnapshotDelete(step SnapshotDeleteStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if err := service.checkSnapshotDelete(step); err != nil {
		return err
	}
	if _, err := os.Lstat(snapshotRawPath(step.Input.Source)); !os.IsNotExist(err) {
		return fmt.Errorf("snapshot raw cleanup absence unconfirmed: %v", err)
	}
	intent, err := service.snapshotDeleteReceipt(&step)
	if err != nil {
		return err
	}
	if step.Phase == "deleting" {
		paths, err := loom.ResumeDeletion(service.ctx, service.store.Directory, intent)
		if err != nil {
			return err
		}
		if err = loom.CheckLocalReaders(paths); err != nil {
			return err
		}
		if err = service.checkSnapshotDelete(step); err != nil {
			return err
		}
		for _, path := range paths {
			if err = loom.RemoveDeletionFile(service.ctx, service.store.Directory, intent, path); err != nil {
				return err
			}
			if err = service.fault("snapshot_delete_unlinked"); err != nil {
				return err
			}
		}
		if err = loom.RemoveDeletionDirectories(service.ctx, service.store.Directory, intent); err != nil {
			return err
		}
		if err = service.fault("snapshot_delete_directory_removed"); err != nil {
			return err
		}
		if err = service.markSnapshotRemoved(&step); err != nil {
			return err
		}
	}
	if step.Phase != "files_removed" {
		return fmt.Errorf("snapshot deletion phase unsupported")
	}
	if _, err = os.Lstat(step.Input.Source.Snapshot.Dir); !os.IsNotExist(err) {
		return fmt.Errorf("snapshot deletion physical absence unconfirmed: %v", err)
	}
	return service.completeSnapshotDelete(step)
}

func (service *SnapshotDeletion) markSnapshotRemoved(step *SnapshotDeleteStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.snapshotDeleteProof(service.ctx, tx, *step, false); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceRunningStepBody(service.ctx, dbgen.ReplaceRunningStepBodyParams{
		OperationID:  step.OperationID,
		StepKey:      "snapshot_delete",
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
	return service.fault("snapshot_delete_files_removed")
}

func (service *SnapshotDeletion) completeSnapshotDelete(step SnapshotDeleteStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSnapshotDelete(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.Phase != "files_removed" {
		return fmt.Errorf("snapshot deletion completion changed")
	}
	if _, err = service.snapshotDeleteProof(service.ctx, tx, step, false); err != nil {
		return err
	}
	publication := step.Input.Source.Snapshot
	body, previousState, completed := operation.Encode(publication), operation.Encode(step.Phase), operation.Encode("complete")

	responsibilities := quota.New(tx)
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).DeleteCheckpoint(service.ctx, dbgen.DeleteCheckpointParams{
		ID:     publication.ID,
		Tenant: publication.TenantID,
		Body:   body,
	})); err != nil {
		return err
	}
	if err = responsibilities.RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         publication.ID,
		HolderKind: "cleanup",
		HolderID:   step.OperationID,
	}); err != nil {
		return err
	}
	if err = responsibilities.RemoveCostReference(service.ctx, quota.CostReference{
		TenantID:   publication.TenantID,
		Kind:       "snapshot",
		Key:        publication.ID,
		HolderKind: "cleanup",
		HolderID:   step.OperationID + ":snapshot_delete",
	}); err != nil {
		return err
	}
	if err = responsibilities.ReleaseReservation(service.ctx, "snapshot:"+publication.ID); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteSnapshot(service.ctx, dbgen.DeleteSnapshotParams{
		ID:        publication.ID,
		Published: 1,
		Body:      body,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).RetainCleanup(service.ctx, dbgen.RetainCleanupParams{
		ID:          "snapshot_delete:" + step.OperationID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(service.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      "snapshot_delete",
		InputHash:    step.Hash,
		PreviousBody: previousState,
		NewBody:      completed,
	})); err != nil {
		return err
	}
	if err = operations.Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = RetireDeletedSnapshot(service.ctx, tx, step.Input.Source, service.proofs); err != nil {
		return err
	}
	if err = service.fault("snapshot_delete_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("snapshot_delete_completed")
}
