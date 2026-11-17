package machine

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (service *Captures) completeCapture(step CaptureStep, manifest Manifest) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadCapture(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.Input, step.Input) || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "publish_intent" || current.State.ResumeRequired || !current.State.Captured || !current.State.Materialized || !current.State.ArtifactsSynced || len(current.State.Files) != 4 || !reflect.DeepEqual(manifest.Files, current.State.Files) {
		return fmt.Errorf("snapshot completion lacks frozen durable proof")
	}
	if _, err = service.captureProof(service.ctx, tx, current); err != nil {
		return err
	}
	publication := step.Input.Snapshot
	publication.Manifest = &manifest
	cost := manifest.Config.Shape.MemoryMiB + manifest.Config.Shape.DiskMiB
	next := step.State
	next.Phase = "complete"
	body, previousBody := operation.Encode(publication), operation.Encode(step.Input.Snapshot)
	completed, previousState, cleanupBody := operation.Encode(next), operation.Encode(step.State), operation.Encode(step.Input)

	responsibilities := quota.New(tx)
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).PublishSnapshot(service.ctx, dbgen.PublishSnapshotParams{
		ID:           publication.ID,
		SourceVm:     publication.SourceVM,
		PreviousBody: previousBody,
		NewBody:      body,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCheckpoint(service.ctx, dbgen.InsertCheckpointParams{
		ID:     publication.ID,
		Tenant: publication.TenantID,
		Body:   body,
	})); err != nil {
		return err
	}
	if err = responsibilities.AddCost(service.ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        publication.TenantID,
			Kind:            "snapshot",
			Key:             publication.ID,
			SourceOperation: step.OperationID,
		},
		StorageMiB: cost,
	}); err != nil {
		return err
	}
	if err = responsibilities.AddCostReference(service.ctx, quota.CostReference{
		TenantID:   publication.TenantID,
		Kind:       "snapshot",
		Key:        publication.ID,
		HolderKind: "active_use",
		HolderID:   "checkpoint:" + publication.ID,
	}); err != nil {
		return err
	}
	if err = responsibilities.RemoveCostReference(service.ctx, quota.CostReference{
		TenantID:   publication.TenantID,
		Kind:       "pending_storage",
		Key:        step.OperationID + ":capture",
		HolderKind: "engine_dependency",
		HolderID:   step.OperationID + ":capture",
	}); err != nil {
		return err
	}
	if err = responsibilities.RetireCost(service.ctx, quota.CostIdentity{
		TenantID:        publication.TenantID,
		Kind:            "pending_storage",
		Key:             step.OperationID + ":capture",
		SourceOperation: step.OperationID,
	}); err != nil {
		return err
	}
	if err = responsibilities.AddResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         publication.ID,
		HolderKind: "checkpoint",
		HolderID:   publication.ID,
	}); err != nil {
		return err
	}
	if err = responsibilities.AddResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         manifest.Config.Image,
		HolderKind: "snapshot",
		HolderID:   publication.ID,
	}); err != nil {
		return err
	}
	// Raw VMM output and its capture boundary retain their own cleanup obligation.
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(service.ctx, dbgen.InsertCleanupIntentParams{
		ID:           "capture:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "capture",
		ResourceID:   publication.SourceVM,
		Body:         cleanupBody,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReleaseOwnership(service.ctx, dbgen.ReleaseOwnershipParams{
		ID:              publication.SourceVM,
		CurrentInstance: publication.SourceInstance,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(service.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      "capture",
		InputHash:    step.Hash,
		PreviousBody: previousState,
		NewBody:      completed,
	})); err != nil {
		return err
	}
	if err = operations.Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("capture_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("capture_completed")
}
