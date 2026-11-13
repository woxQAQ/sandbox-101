package haven

import (
	"fmt"
	"reflect"
)

func (service *Service) completeCapture(step captureStep, manifest snapshotManifest) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadCapture(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.Input, step.Input) || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "publish_intent" || current.State.ResumeRequired || !current.State.Captured || !current.State.Materialized || !current.State.ArtifactsSynced || len(current.State.Files) != 4 || !reflect.DeepEqual(manifest.Files, current.State.Files) {
		return fmt.Errorf("snapshot completion lacks frozen durable proof")
	}
	if _, err = service.captureProof(service.ctx, tx, current); err != nil {
		return err
	}
	snapshot := step.Input.Snapshot
	snapshot.Manifest = &manifest
	cost := manifest.Config.Shape.MemoryMiB + manifest.Config.Shape.DiskMiB
	next := step.State
	next.Phase = "complete"
	statements := []struct {
		q    string
		args []any
	}{
		{"UPDATE snapshots SET published=1,body=? WHERE id=? AND source_vm_id=? AND published=0 AND deleting=0 AND body=?", []any{encode(snapshot), snapshot.ID, snapshot.SourceVM, encode(step.Input.Snapshot)}},
		{"INSERT INTO checkpoints VALUES(?,?,?)", []any{snapshot.ID, snapshot.TenantID, encode(snapshot)}},
		{"INSERT INTO quota_resources VALUES(?,'snapshot',?,0,?,?)", []any{snapshot.TenantID, snapshot.ID, cost, step.OperationID}},
		{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'active_use',?)", []any{snapshot.TenantID, snapshot.ID, "checkpoint:" + snapshot.ID}},
		{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{snapshot.TenantID, step.OperationID + ":capture", step.OperationID + ":capture"}},
		{"DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND source_operation_id=?", []any{snapshot.TenantID, step.OperationID + ":capture", step.OperationID}},
		{"INSERT INTO resource_refs VALUES('snapshot',?,'checkpoint',?)", []any{snapshot.ID, snapshot.ID}},
		{"INSERT INTO resource_refs VALUES('image',?,'snapshot',?)", []any{manifest.Config.Image, snapshot.ID}},
		// Raw VMM output and its capture boundary retain their own cleanup obligation.
		{"INSERT INTO cleanup_intents VALUES(?,?,'capture',?,'pending',?)", []any{"capture:" + step.OperationID, step.OperationID, snapshot.SourceVM, encode(step.Input)}},
		{"UPDATE vms SET operation_id=NULL WHERE id=? AND current_instance=? AND operation_id=?", []any{snapshot.SourceVM, snapshot.SourceInstance, step.OperationID}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='capture' AND status='running' AND input_hash=? AND body=?", []any{encode(next), step.OperationID, step.Hash, encode(step.State)}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{step.OperationID}},
	}
	for _, statement := range statements {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("capture_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("capture_completed")
}
