package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"time"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (service *Captures) checkCapture(step CaptureStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	_, err = service.captureProof(service.ctx, tx, step)
	return err
}

func (service *Captures) saveCapture(ctx context.Context, step *CaptureStep, next CaptureState) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.captureProof(ctx, tx, *step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "capture",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State, step.Status = next, "running"
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	if ctx == service.ctx {
		return service.fault(next.Phase)
	}
	return nil
}

// resumeCapture uses a separate bounded safety context. Cancellation of ordinary
// work cannot discard the durable obligation to resume a possibly paused source.
func (service *Captures) resumeCapture(step *CaptureStep) error {
	if !step.State.ResumeRequired {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	service.admission.Lock()
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err == nil {
		_, err = service.captureProof(ctx, tx, *step)
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	r := step.Input.Source.Input.Resource
	observation, err := service.backend.Inspect(ctx, r)
	if err != nil {
		return err
	}
	if !observation.Alive {
		return fmt.Errorf("capture source exit requires explicit recovery")
	}
	if observation.Paused {
		if err = service.backend.Resume(ctx, r); err != nil {
			return err
		}
		observation, err = service.backend.Inspect(ctx, r)
		if err != nil {
			return err
		}
	}
	if !observation.Alive || observation.Paused {
		return fmt.Errorf("capture source resume unconfirmed")
	}
	next := step.State
	next.ResumeRequired = false
	next.Phase = "source_resumed"
	if err = service.saveCapture(ctx, step, next); err != nil {
		return err
	}
	if service.ctx.Err() == nil {
		return service.fault("source_resumed")
	}
	return service.ctx.Err()
}

func (service *Captures) failCapture(step *CaptureStep, cause error) error {
	if err := service.resumeCapture(step); err != nil {
		return errors.Join(cause, err)
	}
	if service.ctx.Err() != nil {
		return service.ctx.Err()
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.captureProof(service.ctx, tx, *step); err != nil {
		return err
	}
	next := step.State
	next.Error = "capture could not establish durable publication"
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).InsertBlockedCleanup(service.ctx, dbgen.InsertBlockedCleanupParams{
		ID:           "capture:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "snapshot",
		ResourceID:   step.Input.Snapshot.ID,
		Body:         operation.Encode(step.Input),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).FailUnchangedLiveStep(service.ctx, dbgen.FailUnchangedLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "capture",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NextBody:     operation.Encode(next),
	})); err != nil {
		return err
	}
	result := operation.Result{
		SandboxID:  step.Input.Request.SandboxID,
		VMID:       step.Input.Source.Candidate,
		SnapshotID: step.Input.Snapshot.ID,
		Error:      next.Error,
	}
	if err = operations.FailResult(service.ctx, step.OperationID, operation.Encode(result)); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReleaseOwnership(service.ctx, dbgen.ReleaseOwnershipParams{
		ID:              step.Input.Source.Candidate,
		CurrentInstance: step.Input.Request.ExpectedInstance,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return errors.Join(cause, service.store.Checkpoint())
}

func (service *Captures) runCapture(step CaptureStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, errs.ErrInterrupted) && step.State.ResumeRequired {
			runErr = errors.Join(runErr, service.resumeCapture(&step))
		}
	}()
	if operation.Terminal(step.Status) {
		return nil
	}
	if err := service.checkCapture(step); err != nil {
		return err
	}
	if step.Input.Backend != service.backend.Compatibility() || step.Input.Architecture != runtime.GOARCH || step.Input.CPU != host.CPUIdentity() {
		return fmt.Errorf("capture host compatibility changed")
	}
	r := step.Input.Source.Input.Resource
	if err := image.VerifyFiles(step.Input.Source.Input.Image, r.ImageDir); err != nil {
		return err
	}
	if err := loom.VerifyWritableDisk(r, *step.Input.Source.State.DiskIdentity); err != nil {
		return err
	}
	// Only this uninterrupted execution may turn fresh capture bytes into the
	// first durable materialization proof. Recovery can verify an existing proof.
	capturedThisAttempt := !step.State.Captured
	if !step.State.Captured {
		if step.State.ResumeRequired || (step.State.Phase != "admitted" && step.State.Phase != "capture_prepared") {
			return service.failCapture(&step, fmt.Errorf("capture interrupted before independent completion"))
		}
		observation, err := service.backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if !observation.Alive || observation.Paused {
			return service.failCapture(&step, fmt.Errorf("capture requires unpaused source"))
		}
		releaseCapture, err := service.guards.Capture(service.ctx)
		if err != nil {
			return err
		}
		defer releaseCapture()
		if step.State.Boundary == nil {
			boundary, err := loom.PrepareCapture(r, step.OperationID)
			if err != nil {
				return err
			}
			if backend, ok := service.backend.(interface {
				PrepareCaptureWriter(loom.Resource, string) error
			}); ok {
				if err = backend.PrepareCaptureWriter(r, filepath.Join(boundary.Path, "ch")); err != nil {
					return err
				}
			}
			next := step.State
			next.Boundary = boundary
			next.Phase = "capture_prepared"
			if err = service.saveCapture(service.ctx, &step, next); err != nil {
				return err
			}
		}
		next := step.State
		next.ResumeRequired = true
		next.Phase = "pause_intent"
		if err := service.saveCapture(service.ctx, &step, next); err != nil {
			return err
		}
		if err := service.backend.Pause(service.ctx, r); err != nil {
			return service.failCapture(&step, err)
		}
		next = step.State
		next.Phase = "paused"
		if err := service.saveCapture(service.ctx, &step, next); err != nil {
			return err
		}
		boundary := step.State.Boundary
		if boundary == nil {
			return service.failCapture(&step, fmt.Errorf("capture directory proof missing"))
		}
		parent, err := boundary.Open()
		if err != nil {
			return service.failCapture(&step, err)
		}
		_ = parent.Close()
		if err = service.backend.Capture(service.ctx, r, filepath.Join(boundary.Path, "ch")); err != nil {
			return service.failCapture(&step, err)
		}
		if err = loom.CopyFile(r.Disk, filepath.Join(boundary.Path, "publication", "disk.raw"), 0o600); err != nil {
			return service.failCapture(&step, err)
		}
		next = step.State
		next.Captured = true
		next.Phase = "captured"
		if err = service.saveCapture(service.ctx, &step, next); err != nil {
			return err
		}
	}
	if err := service.resumeCapture(&step); err != nil {
		return err
	}
	if !capturedThisAttempt && (!step.State.Materialized || len(step.State.Files) != 4) {
		return service.failCapture(&step, fmt.Errorf("capture lacks pre-crash durable bytes proof"))
	}
	boundary := step.State.Boundary
	if boundary == nil || boundary.Path != filepath.Join(r.Dir, "capture", step.OperationID) {
		return service.failCapture(&step, fmt.Errorf("capture boundary missing"))
	}
	parent, err := boundary.Open()
	if err != nil {
		return service.failCapture(&step, err)
	}
	_ = parent.Close()
	stage := filepath.Join(boundary.Path, "publication")
	final := step.Input.Snapshot.Dir
	if info, err := os.Lstat(final); err == nil {
		if !step.State.ArtifactsSynced || loom.IdentityOf(info) != boundary.Publication {
			return service.failCapture(&step, fmt.Errorf("unproved final snapshot directory"))
		}
		stage = final
	} else if !os.IsNotExist(err) {
		return err
	}
	info, err := os.Lstat(stage)
	if err != nil {
		return service.failCapture(&step, err)
	}
	if loom.IdentityOf(info) != boundary.Publication {
		return service.failCapture(&step, fmt.Errorf("publication inode changed"))
	}
	if !step.State.Materialized {
		if err = loom.MaterializeCapture(boundary, r.Config.Shape.MemoryMiB); err != nil {
			return service.failCapture(&step, err)
		}
		next := step.State
		next.Phase = "capture_copied"
		if err = service.saveCapture(service.ctx, &step, next); err != nil {
			return err
		}
	}
	files, err := loom.HashTree(stage)
	if err != nil {
		return service.failCapture(&step, err)
	}
	if len(files) != 4 {
		return service.failCapture(&step, fmt.Errorf("capture artifact set incomplete"))
	}
	for _, name := range []string{
		"ch/config.json",
		"ch/state.json",
		"ch/memory-ranges",
		"disk.raw",
	} {
		if files[name] == "" {
			return service.failCapture(&step, fmt.Errorf("missing capture artifact"))
		}
	}
	if step.State.Materialized {
		if !reflect.DeepEqual(files, step.State.Files) {
			return service.failCapture(&step, fmt.Errorf("durable capture checksum mismatch"))
		}
	} else {
		if err = loom.SyncTree(stage); err != nil {
			return err
		}
		next := step.State
		next.Materialized = true
		next.Files = files
		next.Phase = "capture_materialized"
		if err = service.saveCapture(service.ctx, &step, next); err != nil {
			return err
		}
	}
	manifest := Manifest{
		Format:          1,
		Backend:         step.Input.Backend,
		Architecture:    step.Input.Architecture,
		CPU:             step.Input.CPU,
		Config:          r.Config,
		MemoryMode:      "CopyOnWrite",
		PublicationMode: "IndependentInodes",
		Files:           step.State.Files,
		Image:           operation.Encode(step.Input.Source.Input.Image),
	}
	if err = loom.WriteJSON(filepath.Join(stage, "manifest.json"), manifest); err != nil {
		return err
	}
	if err = loom.SealTree(stage); err != nil {
		return err
	}
	if err = loom.SyncTree(stage); err != nil {
		return err
	}
	next := step.State
	next.ArtifactsSynced = true
	next.Phase = "artifacts_synced"
	if err = service.saveCapture(service.ctx, &step, next); err != nil {
		return err
	}
	if stage != final {
		root, err := loom.OpenCaptureDir(service.store.Directory)
		if err != nil {
			return err
		}
		destination, err := loom.CaptureChild(root, "snapshots", 0o700)
		if err == nil {
			err = errors.Join(destination.Sync(), destination.Close(), root.Sync())
		}
		err = errors.Join(err, root.Close())
		if err != nil {
			return err
		}
		if err = os.Rename(stage, final); err != nil {
			return err
		}
		if err = loom.SyncDir(filepath.Dir(stage)); err != nil {
			return err
		}
		if err = loom.SyncDir(filepath.Dir(final)); err != nil {
			return err
		}
	}
	next = step.State
	next.Phase = "publish_intent"
	if err = service.saveCapture(service.ctx, &step, next); err != nil {
		return err
	}
	return service.completeCapture(step, manifest)
}

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

func FailedCaptureReferences(ctx context.Context, database store.DBTX, source ResourceStep) ([]ownership.GenerationRef, error) {
	ids, err := quota.New(database).ResourceHolderIDs(ctx, "vm", source.Candidate, "capture")
	if err != nil {
		return nil, err
	}
	var refs []ownership.GenerationRef
	for _, id := range ids {
		capture, err := LoadCapture(ctx, database, id)
		if err != nil {
			return nil, err
		}
		if !operation.Terminal(capture.Status) || capture.Input.Source.OperationID != source.OperationID || capture.Input.Source.Hash != source.Hash || capture.Input.Request.ExpectedInstance != source.State.Witness {
			return nil, fmt.Errorf("release capture user lacks original terminal generation proof")
		}
		if capture.Status == "failed" {
			if capture.State.ResumeRequired || capture.State.Error == "" {
				return nil, fmt.Errorf("release capture failure has unresolved source resume")
			}
			refs = append(refs, ownership.GenerationRef{
				OperationID: id,
				InputHash:   capture.Hash,
			})
		}
	}
	return refs, nil
}

func PendingPublishedCaptures(ctx context.Context, database store.DBTX, source ResourceStep) ([]ownership.GenerationRef, error) {
	ids, err := quota.New(database).ResourceHolderIDs(ctx, "vm", source.Candidate, "capture")
	if err != nil {
		return nil, err
	}
	var refs []ownership.GenerationRef
	for _, id := range ids {
		capture, err := LoadCapture(ctx, database, id)
		if err != nil {
			return nil, err
		}
		if capture.Status != "succeeded" || capture.State.Phase != "complete" || capture.State.ResumeRequired || capture.Input.Source.OperationID != source.OperationID || capture.Input.Source.Hash != source.Hash || capture.Input.Request.ExpectedInstance != source.State.Witness {
			return nil, fmt.Errorf("rollback capture user lacks original published generation proof")
		}
		refs = append(refs, ownership.GenerationRef{
			OperationID: id,
			InputHash:   capture.Hash,
		})
	}
	return refs, nil
}
