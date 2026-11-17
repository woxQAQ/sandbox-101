package machine

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (service *CaptureCleanup) captureRawProof(tx *sql.Tx, step CaptureCleanupStep) error {
	current, err := LoadCaptureCleanup(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("raw capture cleanup intent changed")
	}
	capture := step.Input.Capture
	source := capture.Input.Source
	if err = CaptureCleanupProof(service.ctx, tx, capture, CaptureCleanupTarget{
		VMID:     source.Candidate,
		Instance: source.State.Witness,
		Status:   "transferred",
	}); err != nil {
		return err
	}
	stopped, err := LoadStop(service.ctx, tx, step.Input.Stop.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(stopped, step.Input.Stop) {
		return fmt.Errorf("raw capture original stop proof changed")
	}
	vm, instance, owner, err := LoadVM(service.ctx, tx, source)
	if err != nil {
		return err
	}
	if owner != step.OperationID || instance != source.State.Witness || vm.Deleting || vm.AgentReady {
		return fmt.Errorf("raw capture current stopped ownership changed")
	}
	count, err := quota.New(tx).CountResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         source.Candidate,
		HolderKind: "capture",
		HolderID:   capture.OperationID,
	})
	if err != nil || count != 1 {
		return fmt.Errorf("raw capture responsibility missing: %v", err)
	}
	projection, err := operation.New(tx).Projection(service.ctx, step.OperationID)
	if err != nil {
		return err
	}
	body, request := projection.Body, projection.Request
	status, scope, tenant, hash := projection.Status, projection.ScopeKind, projection.ScopeID, projection.RequestHash
	var result operation.Result
	var frozen struct {
		Action  string
		Request CaptureCleanupRequest
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	if err = json.Unmarshal(request, &frozen); err != nil {
		return err
	}
	if operation.Terminal(status) || scope != "tenant" || tenant != source.Input.TenantID || operation.Digest(request) != hash || frozen.Action != "capture_cleanup" || frozen.Request != step.Input.Request || result.SandboxID != source.Input.SandboxID || result.VMID != source.Candidate || result.SnapshotID != capture.Input.Snapshot.ID {
		return fmt.Errorf("raw capture operation authority changed")
	}
	return nil
}

func (service *CaptureCleanup) checkCaptureRaw(step CaptureCleanupStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	return service.captureRawProof(tx, step)
}

func (service *CaptureCleanup) captureRawReceipt(step *CaptureCleanupStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	capture := step.Input.Capture
	raw := capture.State.Boundary.Path
	body, err := dbgen.New(service.store.Database).GetDeletionReceipt(service.ctx, dbgen.GetDeletionReceiptParams{
		Path:        raw,
		OperationID: step.OperationID,
		StepKey:     "capture_cleanup",
	})

	if err == nil {
		if step.Phase != "deleting" {
			return intent, fmt.Errorf("raw capture receipt outside deletion phase")
		}
		if err = json.Unmarshal(body, &intent); err != nil {
			return intent, err
		}
		return intent, loom.VerifyPublishedCaptureRawDeletion(intent, raw, capture.State.Boundary)
	}
	if !errors.Is(err, sql.ErrNoRows) || step.Phase != "admitted" {
		return intent, fmt.Errorf("raw capture durable receipt missing: %w", err)
	}
	intent, err = loom.InspectPublishedCaptureRawDeletion(service.ctx, service.store.Directory, loom.CaptureRawDeletionTarget{
		Resource:    capture.Input.Source.Input.Resource,
		OperationID: capture.OperationID,
		Boundary:    capture.State.Boundary,
		Digests:     capture.State.Files,
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
	if err = service.captureRawProof(tx, *step); err != nil {
		return intent, err
	}

	if err = store.RequireOne(dbgen.New(tx).InsertDeletionReceipt(service.ctx, dbgen.InsertDeletionReceiptParams{
		Path:        raw,
		OperationID: step.OperationID,
		StepKey:     "capture_cleanup",
		Body:        operation.Encode(intent),
	})); err != nil {
		return intent, err
	}
	if err = store.RequireOne(dbgen.New(tx).BeginPendingStep(service.ctx, dbgen.BeginPendingStepParams{
		OperationID:  step.OperationID,
		StepKey:      "capture_cleanup",
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
	return intent, service.fault("capture_cleanup_receipt")
}

func (service *CaptureCleanup) runCaptureCleanup(step CaptureCleanupStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if err := service.checkCaptureRaw(step); err != nil {
		return err
	}
	source := step.Input.Capture.Input.Source
	if err := service.inspectStoppedGenerations([]ResourceStep{
		source,
	}); err != nil {
		return err
	}
	intent, err := service.captureRawReceipt(&step)
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
	identities := make([]loom.FileIdentity, 0, len(intent.Nodes))
	for _, identity := range intent.Nodes {
		identities = append(identities, identity)
	}
	if err = loom.CheckLocalReaderIdentities(identities); err != nil {
		return err
	}
	if err = service.checkCaptureRaw(step); err != nil {
		return err
	}
	for _, path := range paths {
		if err = loom.RemoveDeletionFile(service.ctx, service.store.Directory, intent, path); err != nil {
			return err
		}
		if err = service.fault("capture_cleanup_unlinked"); err != nil {
			return err
		}
	}
	if err = loom.RemoveDeletionDirectories(service.ctx, service.store.Directory, intent); err != nil {
		return err
	}
	if err = loom.CheckLocalReaderIdentities(identities); err != nil {
		return err
	}
	if err = service.fault("capture_cleanup_files_removed"); err != nil {
		return err
	}
	if err = service.inspectStoppedGenerations([]ResourceStep{
		source,
	}); err != nil {
		return err
	}
	return service.completeCaptureRaw(step, intent)
}

func (service *CaptureCleanup) completeCaptureRaw(step CaptureCleanupStep, intent loom.DeletionIntent) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.captureRawProof(tx, step); err != nil {
		return err
	}
	capture := step.Input.Capture
	source := capture.Input.Source
	responsibilities := quota.New(tx)
	if err = responsibilities.RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         source.Candidate,
		HolderKind: "capture",
		HolderID:   capture.OperationID,
	}); err != nil {
		return err
	}
	if err = responsibilities.RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         source.Input.Resource.Config.Image,
		HolderKind: "capture",
		HolderID:   capture.OperationID,
	}); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).CompleteTransferredCleanup(service.ctx, dbgen.CompleteTransferredCleanupParams{
		ID:          "capture:" + capture.OperationID,
		OperationID: capture.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanup(service.ctx, dbgen.CompletePendingCleanupParams{
		ID:          "raw:" + step.OperationID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReleaseStoppedOwnership(service.ctx, dbgen.ReleaseStoppedOwnershipParams{
		ID:              source.Candidate,
		CurrentInstance: source.State.Witness,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteMatchingDeletionReceipt(service.ctx, dbgen.DeleteMatchingDeletionReceiptParams{
		Path:        intent.Path,
		OperationID: step.OperationID,
		StepKey:     "capture_cleanup",
		Body:        operation.Encode(intent),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(service.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      "capture_cleanup",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NewBody:      operation.Encode("complete"),
	})); err != nil {
		return err
	}
	if err = operations.Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("capture_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.Checkpoint()
}
