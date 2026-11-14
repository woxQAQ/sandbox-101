package haven

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) checkSnapshotDelete(step snapshotDeleteStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadSnapshotDelete(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("snapshot delete current intent changed")
	}
	_, err = service.snapshotDeleteProof(service.ctx, tx, step, false)
	return err
}

func (service *Service) snapshotDeleteReceipt(step *snapshotDeleteStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	var body []byte
	source := step.Input.Source
	err := service.store.db.QueryRowContext(service.ctx, "SELECT body FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='snapshot_delete'", source.Snapshot.Dir, step.OperationID).Scan(&body)
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
	intent, err = loom.InspectSnapshotDeletion(service.ctx, service.store.directory, source.Snapshot.ID, source.Publication, source.Snapshot.Manifest.Files)
	if err != nil {
		return intent, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return intent, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadSnapshotDelete(service.ctx, tx, step.OperationID)
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
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO deletion_intents VALUES(?,?,'snapshot_delete',?)", intent.Path, step.OperationID, encode(intent))); err != nil {
		return intent, err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='snapshot_delete' AND input_hash=? AND status='pending' AND body=?", encode("deleting"), step.OperationID, step.Hash, encode(step.Phase))); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	step.Phase = "deleting"
	step.Status = "running"
	if err = service.store.checkpoint(); err != nil {
		return intent, err
	}
	return intent, service.fault("snapshot_delete_receipt")
}

func (service *Service) runSnapshotDelete(step snapshotDeleteStep) error {
	if terminal(step.Status) {
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
		paths, err := loom.ResumeDeletion(service.ctx, service.store.directory, intent)
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
			if err = loom.RemoveDeletionFile(service.ctx, service.store.directory, intent, path); err != nil {
				return err
			}
			if err = service.fault("snapshot_delete_unlinked"); err != nil {
				return err
			}
		}
		if err = loom.RemoveDeletionDirectories(service.ctx, service.store.directory, intent); err != nil {
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

func (service *Service) markSnapshotRemoved(step *snapshotDeleteStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.snapshotDeleteProof(service.ctx, tx, *step, false); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='snapshot_delete' AND input_hash=? AND status='running' AND body=?", encode("files_removed"), step.OperationID, step.Hash, encode(step.Phase))); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.Phase = "files_removed"
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("snapshot_delete_files_removed")
}

func (service *Service) completeSnapshotDelete(step snapshotDeleteStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadSnapshotDelete(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.Phase != "files_removed" {
		return fmt.Errorf("snapshot deletion completion changed")
	}
	if _, err = service.snapshotDeleteProof(service.ctx, tx, step, false); err != nil {
		return err
	}
	snapshot := step.Input.Source.Snapshot
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM checkpoints WHERE id=? AND tenant=? AND body=?", []any{
			snapshot.ID,
			snapshot.TenantID,
			encode(snapshot),
		}},
		{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='cleanup' AND holder_id=?", []any{
			snapshot.ID,
			step.OperationID,
		}},
		{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='cleanup' AND holder_id=?", []any{
			snapshot.TenantID,
			snapshot.ID,
			step.OperationID + ":snapshot_delete",
		}},
		{"DELETE FROM reservations WHERE owner=?", []any{
			"snapshot:" + snapshot.ID,
		}},
		{"DELETE FROM snapshots WHERE id=? AND published=1 AND deleting=1 AND body=?", []any{
			snapshot.ID,
			encode(snapshot),
		}},
		{"UPDATE cleanup_intents SET status='retained' WHERE id=? AND operation_id=? AND status='pending'", []any{
			"snapshot_delete:" + step.OperationID,
			step.OperationID,
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='snapshot_delete' AND input_hash=? AND body=? AND status='running'", []any{
			encode("complete"),
			step.OperationID,
			step.Hash,
			encode(step.Phase),
		}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{
			step.OperationID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.retireDeletedSnapshot(service.ctx, tx, step.Input.Source); err != nil {
		return err
	}
	if err = service.fault("snapshot_delete_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("snapshot_delete_completed")
}
