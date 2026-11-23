package machine

import (
	"context"
	"database/sql"
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

func (sd *SnapshotDeletion) snapshotDeleteProof(ctx context.Context, tx *sql.Tx, step SnapshotDeleteStep, admitting bool) ([]ResourceStep, error) {
	var record SnapshotRecord
	source := step.Input.Source
	snapshot := source.Snapshot
	request := step.Input.Request
	if request.TenantID != snapshot.TenantID || request.SnapshotID != snapshot.ID {
		return nil, fmt.Errorf("snapshot delete owner changed")
	}
	capture, err := LoadCapture(ctx, tx, source.CaptureOperation)
	if err != nil {
		return nil, err
	}
	if err = sd.proofs.ValidateSnapshotPublication(source, request.TenantID, capture); err != nil {
		return nil, err
	}
	deleting, holderKind, holderID, cleanupStatus := 1, "cleanup", step.OperationID+":snapshot_delete", "pending"
	if admitting {
		deleting = 0
		holderKind = "active_use"
		holderID = "checkpoint:" + snapshot.ID
	}
	var witnessHash string
	row, err := dbgen.New(tx).GetPublishedBody(ctx, dbgen.GetPublishedBodyParams{
		ID:         snapshot.ID,
		SourceVmID: snapshot.SourceVM,
		Deleting:   int64(deleting),
	})
	if err != nil {
		return nil, err
	}
	if record, err = snapshotRecord(ctx, tx, row); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(record, snapshot) {
		return nil, fmt.Errorf("snapshot delete catalog changed")
	}
	publicationHash, err := dbgen.New(tx).GetSnapshotPublication(ctx, dbgen.GetSnapshotPublicationParams{
		SnapshotID:         snapshot.ID,
		TenantID:           snapshot.TenantID,
		CaptureOperationID: capture.OperationID,
	})
	if err != nil {
		return nil, err
	}
	if publicationHash != operation.Digest(operation.Encode(snapshot)) {
		return nil, fmt.Errorf("snapshot publication changed")
	}
	operations := operation.New(tx)
	if witnessHash, err = dbgen.New(tx).CleanupBody(ctx, dbgen.CleanupBodyParams{
		ID:           "capture:" + capture.OperationID,
		OperationID:  capture.OperationID,
		ResourceKind: "capture",
		ResourceID:   snapshot.SourceVM,
		Status:       "done",
	}); err != nil {
		return nil, fmt.Errorf("snapshot raw capture cleanup not retired: %w", err)
	}
	if witnessHash != capture.Hash {
		return nil, fmt.Errorf("snapshot raw cleanup history changed")
	}
	backings, err := sd.proofs.Backings(ctx, tx, source)
	if err != nil {
		return nil, err
	}
	physicalHolderKind, physicalHolderID := "cleanup", step.OperationID
	if admitting {
		physicalHolderKind = "checkpoint"
		physicalHolderID = snapshot.ID
	}
	if count, err := quota.CountMatchingReservation(ctx, tx, quota.Reservation{
		Owner: "snapshot:" + snapshot.ID,
		Bytes: (2*snapshot.Manifest.Config.Shape.MemoryMiB + snapshot.Manifest.Config.Shape.DiskMiB + 224) << 20,
	}); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if !admitting {
		parent, err := operations.Projection(ctx, step.OperationID)
		if err != nil {
			return nil, err
		}
		result := parent.Result
		if !parent.Request.Equal(request.operationRequest()) ||
			operation.Terminal(parent.Status) ||
			parent.ScopeKind != "tenant" ||
			parent.ScopeID != request.TenantID ||
			result.SnapshotID != snapshot.ID {
			return nil, fmt.Errorf("snapshot delete parent identity changed")
		}
	}
	if count, err := quota.CountCost(ctx, tx, "snapshot", snapshot.ID); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := quota.CountMatchingCost(ctx, tx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        snapshot.TenantID,
			Kind:            "snapshot",
			Key:             snapshot.ID,
			SourceOperation: capture.OperationID,
		},
		StorageMiB: snapshot.Manifest.Config.Shape.DiskMiB + snapshot.Manifest.Config.Shape.MemoryMiB,
	}); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := quota.CountCostHolders(ctx, tx, "snapshot", snapshot.ID); err != nil || count != int64(len(backings)+1) {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := quota.CountCostReference(ctx, tx, quota.CostReference{
		TenantID:   snapshot.TenantID,
		Kind:       "snapshot",
		Key:        snapshot.ID,
		HolderKind: holderKind,
		HolderID:   holderID,
	}); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := quota.CountResourceHolders(ctx, tx, "snapshot", snapshot.ID); err != nil || count != int64(len(backings)+1) {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	for _, reference := range []quota.ResourceReference{
		{
			Kind:       "snapshot",
			ID:         snapshot.ID,
			HolderKind: physicalHolderKind,
			HolderID:   physicalHolderID,
		},
		{
			Kind:       "image",
			ID:         snapshot.Manifest.Config.Image,
			HolderKind: "snapshot",
			HolderID:   snapshot.ID,
		},
	} {
		if count, err := quota.CountResourceReference(ctx, tx, reference); err != nil || count != 1 {
			return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
		}
	}
	if !admitting {
		if count, err := dbgen.New(tx).MatchingCleanupCount(ctx, dbgen.MatchingCleanupCountParams{
			ID:           "snapshot_delete:" + step.OperationID,
			OperationID:  step.OperationID,
			ResourceKind: "snapshot",
			ResourceID:   snapshot.ID,
			Status:       cleanupStatus,
			WitnessHash:  step.Hash,
		}); err != nil || count != 1 {
			return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
		}
	}
	count, err := sd.usage.TemplateSnapshotUsers(ctx, tx, snapshot.ID)
	if err != nil || count != 0 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	count, err = sd.usage.PoolSnapshotUsers(ctx, tx, snapshot.ID)
	if err != nil || count != 0 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	count, err = dbgen.New(tx).CountCaptureOwner(ctx, dbgen.CountCaptureOwnerParams{
		SnapshotID:        snapshot.ID,
		TenantID:          snapshot.TenantID,
		SourceOperationID: capture.OperationID,
	})

	if err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	count, err = SnapshotReaders(ctx, tx, snapshot.ID)
	if err != nil || count != 0 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	return backings, nil
}

func snapshotRawPath(source SnapshotSource) string {
	return filepath.Join(filepath.Dir(filepath.Dir(source.Snapshot.Dir)), "vms", source.Snapshot.SourceVM, "capture", source.CaptureOperation)
}

func (sd *SnapshotDeletion) checkSnapshotDelete(step SnapshotDeleteStep) error {
	sd.storeWriteMu.Lock()
	defer sd.storeWriteMu.Unlock()
	tx, err := sd.store.Database.BeginTx(sd.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSnapshotDelete(sd.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("snapshot delete current intent changed")
	}
	_, err = sd.snapshotDeleteProof(sd.ctx, tx, step, false)
	return err
}

func (sd *SnapshotDeletion) snapshotDeleteReceipt(step *SnapshotDeleteStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	source := step.Input.Source
	intent, err := store.LoadDeletionReceipt(sd.ctx, sd.store.Database, step.OperationID, "snapshot_delete", source.Snapshot.Dir)

	if err == nil {
		if step.Phase != "deleting" && step.Phase != "files_removed" {
			return intent, fmt.Errorf("snapshot deletion receipt outside phase")
		}
		return intent, loom.VerifySnapshotDeletion(intent, source.Snapshot.Dir, source.Publication)
	}
	if !errors.Is(err, sql.ErrNoRows) || step.Phase != "admitted" {
		return intent, fmt.Errorf("snapshot deletion receipt unavailable: %w", err)
	}
	intent, err = loom.InspectSnapshotDeletion(sd.ctx, sd.store.Directory, loom.SnapshotDeletionTarget{
		ID:          source.Snapshot.ID,
		Publication: source.Publication,
		Files:       source.Snapshot.Manifest.Files,
	})
	if err != nil {
		return intent, err
	}
	sd.storeWriteMu.Lock()
	defer sd.storeWriteMu.Unlock()
	tx, err := sd.store.Database.BeginTx(sd.ctx, nil)
	if err != nil {
		return intent, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSnapshotDelete(sd.ctx, tx, step.OperationID)
	if err != nil {
		return intent, err
	}
	if !reflect.DeepEqual(current, *step) {
		return intent, fmt.Errorf("snapshot deletion receipt intent changed")
	}
	backings, err := sd.snapshotDeleteProof(sd.ctx, tx, *step, false)
	if err != nil {
		return intent, err
	}
	for _, backing := range backings {
		if backing.State.Materials == nil || backing.State.Materials.Files["memory-ranges"].Identity != intent.Nodes["ch/memory-ranges"] {
			return intent, fmt.Errorf("snapshot retained memory differs from published inode")
		}
	}
	if err = store.RecordDeletionReceipt(sd.ctx, tx, step.OperationID, "snapshot_delete", intent); err != nil {
		return intent, err
	}
	if err = operation.AdvancePhase(sd.ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "snapshot_delete",
		InputHash:      step.Hash,
		PreviousStatus: "pending",
		PreviousPhase:  step.Phase,
		NextStatus:     "running",
		NextPhase:      "deleting",
	}); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	step.Phase = "deleting"
	step.Status = "running"
	if err = sd.store.Checkpoint(); err != nil {
		return intent, err
	}
	return intent, sd.fault("snapshot_delete_receipt")
}

func (sd *SnapshotDeletion) runSnapshotDelete(step SnapshotDeleteStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if err := sd.checkSnapshotDelete(step); err != nil {
		return err
	}
	if _, err := os.Lstat(snapshotRawPath(step.Input.Source)); !os.IsNotExist(err) {
		return fmt.Errorf("snapshot raw cleanup absence unconfirmed: %v", err)
	}
	intent, err := sd.snapshotDeleteReceipt(&step)
	if err != nil {
		return err
	}
	if step.Phase == "deleting" {
		paths, err := loom.ResumeDeletion(sd.ctx, sd.store.Directory, intent)
		if err != nil {
			return err
		}
		if err = loom.CheckLocalReaders(paths); err != nil {
			return err
		}
		if err = sd.checkSnapshotDelete(step); err != nil {
			return err
		}
		for _, path := range paths {
			if err = loom.RemoveDeletionFile(sd.ctx, sd.store.Directory, intent, path); err != nil {
				return err
			}
			if err = sd.fault("snapshot_delete_unlinked"); err != nil {
				return err
			}
		}
		if err = loom.RemoveDeletionDirectories(sd.ctx, sd.store.Directory, intent); err != nil {
			return err
		}
		if err = sd.fault("snapshot_delete_directory_removed"); err != nil {
			return err
		}
		if err = sd.markSnapshotRemoved(&step); err != nil {
			return err
		}
	}
	if step.Phase != "files_removed" {
		return fmt.Errorf("snapshot deletion phase unsupported")
	}
	if _, err = os.Lstat(step.Input.Source.Snapshot.Dir); !os.IsNotExist(err) {
		return fmt.Errorf("snapshot deletion physical absence unconfirmed: %v", err)
	}
	return sd.completeSnapshotDelete(step)
}

func (sd *SnapshotDeletion) markSnapshotRemoved(step *SnapshotDeleteStep) error {
	sd.storeWriteMu.Lock()
	defer sd.storeWriteMu.Unlock()
	tx, err := sd.store.Database.BeginTx(sd.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = sd.snapshotDeleteProof(sd.ctx, tx, *step, false); err != nil {
		return err
	}
	if err = operation.AdvancePhase(sd.ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "snapshot_delete",
		InputHash:      step.Hash,
		PreviousStatus: "running",
		PreviousPhase:  step.Phase,
		NextStatus:     "running",
		NextPhase:      "files_removed",
	}); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.Phase = "files_removed"
	if err = sd.store.Checkpoint(); err != nil {
		return err
	}
	return sd.fault("snapshot_delete_files_removed")
}

func (sd *SnapshotDeletion) completeSnapshotDelete(step SnapshotDeleteStep) error {
	sd.storeWriteMu.Lock()
	defer sd.storeWriteMu.Unlock()
	tx, err := sd.store.Database.BeginTx(sd.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSnapshotDelete(sd.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.Phase != "files_removed" {
		return fmt.Errorf("snapshot deletion completion changed")
	}
	if _, err = sd.snapshotDeleteProof(sd.ctx, tx, step, false); err != nil {
		return err
	}
	publication := step.Input.Source.Snapshot
	body := operation.Encode(publication)

	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).DeleteSnapshotPublication(sd.ctx, dbgen.DeleteSnapshotPublicationParams{
		SnapshotID:         publication.ID,
		TenantID:           publication.TenantID,
		CaptureOperationID: publication.OperationID,
		RecordHash:         operation.Digest(body),
	})); err != nil {
		return err
	}
	if err = quota.RemoveResourceReference(sd.ctx, tx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         publication.ID,
		HolderKind: "cleanup",
		HolderID:   step.OperationID,
	}); err != nil {
		return err
	}
	if err = quota.RemoveCostReference(sd.ctx, tx, quota.CostReference{
		TenantID:   publication.TenantID,
		Kind:       "snapshot",
		Key:        publication.ID,
		HolderKind: "cleanup",
		HolderID:   step.OperationID + ":snapshot_delete",
	}); err != nil {
		return err
	}
	if err = quota.ReleaseReservation(sd.ctx, tx, "snapshot:"+publication.ID); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteSnapshot(sd.ctx, dbgen.DeleteSnapshotParams{
		ID:                 publication.ID,
		Published:          1,
		CaptureOperationID: publication.OperationID,
		TenantID:           publication.TenantID,
		SourceVmID:         publication.SourceVM,
		SourceInstance:     publication.SourceInstance,
		Directory:          publication.Dir,
		ManifestHash:       manifestHash(publication.Manifest),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).RetainCleanup(sd.ctx, dbgen.RetainCleanupParams{
		ID:          "snapshot_delete:" + step.OperationID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = operation.AdvancePhase(sd.ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "snapshot_delete",
		InputHash:      step.Hash,
		PreviousStatus: "running",
		PreviousPhase:  step.Phase,
		NextStatus:     "succeeded",
		NextPhase:      "complete",
	}); err != nil {
		return err
	}
	if err = operations.Complete(sd.ctx, step.OperationID); err != nil {
		return err
	}
	if err = RetireDeletedSnapshot(sd.ctx, tx, step.Input.Source, sd.proofs); err != nil {
		return err
	}
	if err = sd.fault("snapshot_delete_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = sd.store.Checkpoint(); err != nil {
		return err
	}
	return sd.fault("snapshot_delete_completed")
}
