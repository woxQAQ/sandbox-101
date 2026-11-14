package haven

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"time"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

func (service *Service) checkCapture(step captureStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	_, err = service.captureProof(service.ctx, tx, step)
	return err
}

func (service *Service) saveCapture(ctx context.Context, step *captureStep, next captureState) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.captureProof(ctx, tx, *step); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='capture' AND input_hash=? AND status IN ('pending','running') AND body=?", encode(next), step.OperationID, step.Hash, encode(step.State))); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State, step.Status = next, "running"
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	if ctx == service.ctx {
		return service.fault(next.Phase)
	}
	return nil
}

// resumeCapture uses a separate bounded safety context. Cancellation of ordinary
// work cannot discard the durable obligation to resume a possibly paused source.
func (service *Service) resumeCapture(step *captureStep) error {
	if !step.State.ResumeRequired {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	service.admission.Lock()
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err == nil {
		_, err = service.captureProof(ctx, tx, *step)
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	r := step.Input.Source.Input.Resource
	observation, err := service.opts.Backend.Inspect(ctx, r)
	if err != nil {
		return err
	}
	if !observation.Alive {
		return fmt.Errorf("capture source exit requires explicit recovery")
	}
	if observation.Paused {
		if err = service.opts.Backend.Resume(ctx, r); err != nil {
			return err
		}
		observation, err = service.opts.Backend.Inspect(ctx, r)
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

func (service *Service) failCapture(step *captureStep, cause error) error {
	if err := service.resumeCapture(step); err != nil {
		return errors.Join(cause, err)
	}
	if service.ctx.Err() != nil {
		return service.ctx.Err()
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
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
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO cleanup_intents VALUES(?,?,'snapshot',?,'blocked',?)", "capture:"+step.OperationID, step.OperationID, step.Input.Snapshot.ID, encode(step.Input))); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key='capture' AND input_hash=? AND body=? AND status IN ('pending','running')", encode(next), step.OperationID, step.Hash, encode(step.State))); err != nil {
		return err
	}
	result := operationResult{
		SandboxID:  step.Input.Request.SandboxID,
		VMID:       step.Input.Source.Candidate,
		SnapshotID: step.Input.Snapshot.ID,
		Error:      next.Error,
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", encode(result), step.OperationID)); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE vms SET operation_id=NULL WHERE id=? AND operation_id=? AND current_instance=?", step.Input.Source.Candidate, step.OperationID, step.Input.Request.ExpectedInstance)); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return errors.Join(cause, service.store.checkpoint())
}

func (service *Service) runCapture(step captureStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, ErrInterrupted) && step.State.ResumeRequired {
			runErr = errors.Join(runErr, service.resumeCapture(&step))
		}
	}()
	if terminal(step.Status) {
		return nil
	}
	if err := service.checkCapture(step); err != nil {
		return err
	}
	if step.Input.Backend != service.opts.Backend.Compatibility() || step.Input.Architecture != runtime.GOARCH || step.Input.CPU != host.CPUIdentity() {
		return fmt.Errorf("capture host compatibility changed")
	}
	r := step.Input.Source.Input.Resource
	if err := verifyImageFiles(step.Input.Source.Input.Image, r.ImageDir); err != nil {
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
		observation, err := service.opts.Backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if !observation.Alive || observation.Paused {
			return service.failCapture(&step, fmt.Errorf("capture requires unpaused source"))
		}
		select {
		case service.capture <- struct{}{}:
		case <-service.ctx.Done():
			return service.ctx.Err()
		}
		defer func() {
			<-service.capture
		}()
		if step.State.Boundary == nil {
			boundary, err := loom.PrepareCapture(r, step.OperationID)
			if err != nil {
				return err
			}
			if backend, ok := service.opts.Backend.(interface {
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
		if err := service.opts.Backend.Pause(service.ctx, r); err != nil {
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
		if err = service.opts.Backend.Capture(service.ctx, r, filepath.Join(boundary.Path, "ch")); err != nil {
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
	manifest := snapshotManifest{
		Format:          1,
		Backend:         step.Input.Backend,
		Architecture:    step.Input.Architecture,
		CPU:             step.Input.CPU,
		Config:          r.Config,
		MemoryMode:      "CopyOnWrite",
		PublicationMode: "IndependentInodes",
		Files:           step.State.Files,
		Image:           encode(step.Input.Source.Input.Image),
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
		root, err := loom.OpenCaptureDir(service.store.directory)
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
