package haven

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

func captureDiscardPaths(step captureDiscardStep) []string {
	source := step.Input.Source
	return []string{filepath.Join(source.Input.Source.Input.Resource.Dir, "capture", source.OperationID), source.Input.Snapshot.Dir}
}

func (service *Service) checkCaptureDiscard(step captureDiscardStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadCaptureDiscard(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("capture discard current intent changed")
	}
	return service.captureDiscardProof(service.ctx, tx, step, false)
}

func (service *Service) inspectCaptureDiscardSource(step captureDiscardStep) error {
	service.admission.Lock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	var history []resourceStep
	if err == nil {
		err = service.captureDiscardProof(service.ctx, tx, step, false)
		if err == nil {
			history, err = service.generationHistory(service.ctx, tx, step.Input.Source.Input.Source)
		}
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	return service.inspectStoppedGenerations(append(history, step.Input.Source.Input.Source))
}

func (service *Service) captureDiscardReceipts(step *captureDiscardStep) ([]loom.DeletionIntent, error) {
	paths := captureDiscardPaths(*step)
	var intents []loom.DeletionIntent
	missing := 0
	for _, path := range paths {
		var body []byte
		err := service.store.db.QueryRowContext(service.ctx, "SELECT body FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='capture_discard'", path, step.OperationID).Scan(&body)
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
	intents, err := loom.InspectCaptureDeletion(service.ctx, service.store.directory, source.Input.Source.Input.Resource, source.OperationID, source.Input.Snapshot.ID, source.State.Boundary)
	if err != nil {
		return nil, err
	}
	if err = loom.VerifyCaptureDeletion(intents, paths[0], paths[1], source.State.Boundary); err != nil {
		return nil, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadCaptureDiscard(service.ctx, tx, step.OperationID)
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
		if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO deletion_intents VALUES(?,?,'capture_discard',?)", intent.Path, step.OperationID, encode(intent))); err != nil {
			return nil, err
		}
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='capture_discard' AND input_hash=? AND status='pending' AND body=?", encode("deleting"), step.OperationID, step.Hash, encode(step.Phase))); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	step.Phase = "deleting"
	step.Status = "running"
	if err = service.store.checkpoint(); err != nil {
		return nil, err
	}
	return intents, service.fault("capture_discard_receipts")
}

func (service *Service) runCaptureDiscard(step captureDiscardStep) error {
	if terminal(step.Status) {
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
			paths, err := loom.ResumeDeletion(service.ctx, service.store.directory, intent)
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
			paths, err := loom.ResumeDeletion(service.ctx, service.store.directory, intent)
			if err != nil {
				return err
			}
			for _, path := range paths {
				if err = loom.RemoveDeletionFile(service.ctx, service.store.directory, intent, path); err != nil {
					return err
				}
				if err = service.fault("capture_discard_unlinked"); err != nil {
					return err
				}
			}
			if err = loom.RemoveDeletionDirectories(service.ctx, service.store.directory, intent); err != nil {
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

func (service *Service) markCaptureDiscardRemoved(step *captureDiscardStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err = service.captureDiscardProof(service.ctx, tx, *step, false); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='capture_discard' AND input_hash=? AND status='running' AND body=?", encode("files_removed"), step.OperationID, step.Hash, encode(step.Phase))); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.Phase = "files_removed"
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("capture_discard_files_removed")
}

func (service *Service) completeCaptureDiscard(step captureDiscardStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadCaptureDiscard(service.ctx, tx, step.OperationID)
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
	snapshot := source.Input.Snapshot
	resource := source.Input.Source
	statements := []struct {
		q    string
		args []any
	}{
		{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND holder_kind='cleanup' AND holder_id=?", []any{snapshot.TenantID, source.OperationID + ":capture", step.OperationID + ":capture_discard"}},
		{"DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND source_operation_id=?", []any{snapshot.TenantID, source.OperationID + ":capture", source.OperationID}},
		{"DELETE FROM reservations WHERE owner=?", []any{"snapshot:" + snapshot.ID}},
		{"DELETE FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='capture' AND holder_id=?", []any{resource.Candidate, source.OperationID}},
		{"DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='capture' AND holder_id=?", []any{resource.Input.Resource.Config.Image, source.OperationID}},
		{"DELETE FROM snapshots WHERE id=? AND published=0 AND deleting=1 AND body=?", []any{snapshot.ID, encode(snapshot)}},
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='transferred'", []any{"capture:" + source.OperationID, source.OperationID}},
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='pending'", []any{"capture_discard:" + step.OperationID, step.OperationID}},
		{"UPDATE vms SET operation_id=NULL WHERE id=? AND operation_id=? AND current_instance=? AND state='stopped'", []any{resource.Candidate, step.OperationID, step.Input.Request.ExpectedInstance}},
	}
	for _, path := range captureDiscardPaths(step) {
		statements = append(statements, struct {
			q    string
			args []any
		}{"DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='capture_discard'", []any{path, step.OperationID}})
	}
	statements = append(statements, []struct {
		q    string
		args []any
	}{
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='capture_discard' AND input_hash=? AND body=? AND status='running'", []any{encode("complete"), step.OperationID, step.Hash, encode(step.Phase)}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{step.OperationID}},
	}...)
	for _, statement := range statements {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("capture_discard_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("capture_discard_completed")
}
