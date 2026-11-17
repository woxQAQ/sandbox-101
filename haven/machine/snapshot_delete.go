package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

type SnapshotDeleteRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	SnapshotID string `json:"snapshot_id"`
}
type (
	snapshotDeleteInput struct {
		Request SnapshotDeleteRequest
		Source  SnapshotSource
	}
	SnapshotDeleteStep struct {
		OperationID string
		Hash        string
		Status      string
		Phase       string
		Input       snapshotDeleteInput
	}
)

func LoadSnapshotDelete(ctx context.Context, q store.DBTX, id string) (SnapshotDeleteStep, error) {
	step := SnapshotDeleteStep{
		OperationID: id,
	}
	record, err := dbgen.New(q).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "snapshot_delete",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	payload, body := record.Input, record.Body
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("snapshot delete input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "snapshot_delete" || expected != "" || candidate != step.Input.Request.SnapshotID || candidate != step.Input.Source.Snapshot.ID {
		return step, fmt.Errorf("snapshot delete frozen identity changed")
	}
	return step, nil
}

func (service *SnapshotDeletion) snapshotDeleteProof(ctx context.Context, tx *sql.Tx, step SnapshotDeleteStep, admitting bool) ([]ResourceStep, error) {
	var record SnapshotRecord
	var checkpoint SnapshotRecord
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
	if err = service.proofs.ValidateSnapshotPublication(source, request.TenantID, capture); err != nil {
		return nil, err
	}
	deleting, holderKind, holderID, cleanupStatus := 1, "cleanup", step.OperationID+":snapshot_delete", "pending"
	if admitting {
		deleting = 0
		holderKind = "active_use"
		holderID = "checkpoint:" + snapshot.ID
	}
	var body []byte
	if body, err = dbgen.New(tx).GetPublishedBody(ctx, dbgen.GetPublishedBodyParams{
		ID:         snapshot.ID,
		SourceVmID: snapshot.SourceVM,
		Deleting:   int64(deleting),
	}); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(record, snapshot) {
		return nil, fmt.Errorf("snapshot delete catalog changed")
	}
	if body, err = dbgen.New(tx).GetCheckpointBody(ctx, dbgen.GetCheckpointBodyParams{
		ID:     snapshot.ID,
		Tenant: snapshot.TenantID,
	}); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(body, &checkpoint); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(checkpoint, snapshot) {
		return nil, fmt.Errorf("snapshot delete checkpoint changed")
	}
	operations := operation.New(tx)
	if body, err = dbgen.New(tx).CleanupBody(ctx, dbgen.CleanupBodyParams{
		ID:           "capture:" + capture.OperationID,
		OperationID:  capture.OperationID,
		ResourceKind: "capture",
		ResourceID:   snapshot.SourceVM,
		Status:       "done",
	}); err != nil {
		return nil, fmt.Errorf("snapshot raw capture cleanup not retired: %w", err)
	}
	if string(body) != string(operation.Encode(capture.Input)) {
		return nil, fmt.Errorf("snapshot raw cleanup history changed")
	}
	backings, err := service.proofs.Backings(ctx, tx, source)
	if err != nil {
		return nil, err
	}
	physicalHolderKind, physicalHolderID := "cleanup", step.OperationID
	if admitting {
		physicalHolderKind = "checkpoint"
		physicalHolderID = snapshot.ID
	}
	if count, err := quota.New(tx).CountMatchingReservation(ctx, quota.Reservation{
		Owner: "snapshot:" + snapshot.ID,
		Bytes: (2*snapshot.Manifest.Config.Shape.MemoryMiB + snapshot.Manifest.Config.Shape.DiskMiB + 224) << 20,
	}); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if !admitting {
		var original struct {
			Action  string
			Request SnapshotDeleteRequest
		}
		var result operation.Result
		parent, err := operations.Projection(ctx, step.OperationID)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(parent.Request, &original); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(parent.Body, &result); err != nil {
			return nil, err
		}
		if operation.Digest(parent.Request) != parent.RequestHash || operation.Terminal(parent.Status) || parent.ScopeKind != "tenant" || parent.ScopeID != request.TenantID || original.Action != "snapshot_delete" || original.Request != request || result.SnapshotID != snapshot.ID {
			return nil, fmt.Errorf("snapshot delete parent identity changed")
		}
	}
	responsibilities := quota.New(tx)
	if count, err := responsibilities.CountCost(ctx, "snapshot", snapshot.ID); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := responsibilities.CountMatchingCost(ctx, quota.Cost{
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
	if count, err := responsibilities.CountCostHolders(ctx, "snapshot", snapshot.ID); err != nil || count != int64(len(backings)+1) {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := responsibilities.CountCostReference(ctx, quota.CostReference{
		TenantID:   snapshot.TenantID,
		Kind:       "snapshot",
		Key:        snapshot.ID,
		HolderKind: holderKind,
		HolderID:   holderID,
	}); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := responsibilities.CountResourceHolders(ctx, "snapshot", snapshot.ID); err != nil || count != int64(len(backings)+1) {
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
		if count, err := responsibilities.CountResourceReference(ctx, reference); err != nil || count != 1 {
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
			Body:         operation.Encode(step.Input),
		}); err != nil || count != 1 {
			return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
		}
	}
	count, err := service.usage.TemplateSnapshotUsers(ctx, tx, snapshot.ID)
	if err != nil || count != 0 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	count, err = service.usage.PoolSnapshotUsers(ctx, tx, snapshot.ID)
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

func (service *SnapshotDeletion) AdmitSnapshotDelete(ctx context.Context, request SnapshotDeleteRequest) (operation.Record, error) {
	if service.
		usage ==
		nil {
		return operation.Record{}, fmt.
			Errorf("%w: required SnapshotDeletion capability port unavailable",
				errs.ErrManualRecovery,
			)
	}

	var empty operation.Record
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.SnapshotID, "snap-") {
		return empty, fmt.Errorf("%w: snapshot delete requires scoped snapshot and key", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request SnapshotDeleteRequest
	}{"snapshot_delete", request})
	hash := operation.Digest(frozen)
	service.admission.Lock()
	defer service.admission.Unlock()
	closing := service.lifetime.Closing() || service.store.Reviewed
	if closing {
		return empty, fmt.Errorf("haven is closing")
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := operation.New(tx).Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	source, err := service.proofs.SnapshotPublication(ctx, tx, request.TenantID, request.SnapshotID)
	if err != nil {
		return empty, err
	}
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SnapshotID: request.SnapshotID,
		},
	}
	input := snapshotDeleteInput{
		Request: request,
		Source:  source,
	}
	payload := operation.Encode(input)
	step := SnapshotDeleteStep{
		OperationID: accepted.ID,
		Hash:        operation.Digest(payload),
		Status:      "pending",
		Phase:       "admitted",
		Input:       input,
	}
	if _, err = service.snapshotDeleteProof(ctx, tx, step, true); err != nil {
		return empty, err
	}
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   request.TenantID,
		Key:       request.Key,
		Hash:      hash,
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "snapshot_delete",
		Action:              "snapshot_delete",
		InputHash:           step.Hash,
		Input:               payload,
		CandidateResourceID: request.SnapshotID,
		ExpectedInstance:    "",
		Status:              "pending",
		Body:                operation.Encode(step.Phase),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).MarkPublishedDeleting(ctx, dbgen.MarkPublishedDeletingParams{
		ID:   request.SnapshotID,
		Body: operation.Encode(source.Snapshot),
	})); err != nil {
		return empty, err
	}
	responsibilities := quota.New(tx)
	if err = responsibilities.TransferResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         request.SnapshotID,
		HolderKind: "checkpoint",
		HolderID:   request.SnapshotID,
	}, "cleanup", accepted.ID); err != nil {
		return empty, err
	}
	if err = responsibilities.TransferCostReference(ctx, quota.CostReference{
		TenantID:   request.TenantID,
		Kind:       "snapshot",
		Key:        request.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "checkpoint:" + request.SnapshotID,
	}, "cleanup", accepted.ID+":snapshot_delete"); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "snapshot_delete:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: "snapshot",
		ResourceID:   request.SnapshotID,
		Body:         payload,
	})); err != nil {
		return empty, err
	}
	if _, err = service.snapshotDeleteProof(ctx, tx, step, false); err != nil {
		return empty, err
	}
	if err = service.fault("snapshot_delete_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = service.fault("snapshot_delete_admitted"); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}

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
