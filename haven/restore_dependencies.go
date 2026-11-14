package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) restoreDependencies(tx *sql.Tx, step resourceStep) error {
	if step.State.RestoreBlocked {
		return fmt.Errorf("%w: restore blocked; explicit cleanup required", ErrManualRecovery)
	}
	return service.restoreRelations(service.ctx, tx, step)
}

// restoreRelations verifies retained dependencies without authorizing execution.
// Explicit cleanup can check a blocked step through this read-only relation proof.
func (service *Service) restoreRelations(ctx context.Context, tx *sql.Tx, step resourceStep) error {
	source := step.Input.Restore
	if source == nil {
		return fmt.Errorf("restore source missing")
	}
	historical, err := loadCapture(ctx, tx, source.CaptureOperation)
	if err != nil {
		return err
	}
	if err = service.validateRestoreSource(*source, step.Input.TenantID, historical); err != nil {
		return err
	}
	shape := step.Input.Resource.Config.Shape
	checks := []struct {
		q    string
		args []any
		want int
	}{
		{"SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=? AND source_operation_id=? AND source_kind='capture'", []any{
			source.Snapshot.ID,
			step.Input.TenantID,
			source.CaptureOperation,
		}, 1},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND sandbox_slots=0 AND storage_mib=? AND source_operation_id=?", []any{
			step.Input.TenantID,
			source.Snapshot.ID,
			shape.MemoryMiB + shape.DiskMiB,
			source.CaptureOperation,
		}, 1},
		{"SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", []any{
			"restore:" + step.OperationID,
			(shape.MemoryMiB + shape.DiskMiB + 64) << 20,
		}, 1},
	}
	if step.State.BackingReady {
		if step.State.Materials == nil || !step.State.Materials.Complete || step.State.DiskIdentity == nil {
			return fmt.Errorf("backing transfer lacks complete material proof")
		}
		checks = append(checks, []struct {
			q    string
			args []any
			want int
		}{
			{"SELECT COUNT(*) FROM source_leases WHERE operation_id=? AND step_key=?", []any{
				step.OperationID,
				step.Key,
			}, 0},
			{"SELECT COUNT(*) FROM backing WHERE path=? AND vm_id=? AND digest=?", []any{
				filepath.Join(step.Input.Resource.RestoreDir, "memory-ranges"),
				step.Candidate,
				source.Snapshot.Manifest.Files["ch/memory-ranges"],
			}, 1},
			{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='backing' AND holder_id=?", []any{
				source.Snapshot.ID,
				step.Input.BackingHolder,
			}, 1},
			{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{
				source.Snapshot.ID,
				step.OperationID + ":resource",
			}, 0},
			{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{
				step.Input.TenantID,
				source.Snapshot.ID,
				"backing:" + step.Input.BackingHolder,
			}, 1},
			{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{
				step.Input.TenantID,
				source.Snapshot.ID,
				step.OperationID + ":resource",
			}, 0},
		}...)
	} else {
		current, err := service.loadRestoreSource(ctx, tx, step.Input.TenantID, source.Snapshot.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *source) {
			return fmt.Errorf("leased restore source changed")
		}
		checks = append(checks, []struct {
			q    string
			args []any
			want int
		}{
			{"SELECT COUNT(*) FROM source_leases WHERE snapshot_id=? AND operation_id=? AND step_key=?", []any{
				source.Snapshot.ID,
				step.OperationID,
				step.Key,
			}, 1},
			{"SELECT COUNT(*) FROM backing WHERE path=?", []any{
				filepath.Join(step.Input.Resource.RestoreDir, "memory-ranges"),
			}, 0},
			{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{
				source.Snapshot.ID,
				step.OperationID + ":resource",
			}, 1},
			{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{
				step.Input.TenantID,
				source.Snapshot.ID,
				step.OperationID + ":resource",
			}, 1},
		}...)
	}
	for _, check := range checks {
		var n int
		if err = tx.QueryRowContext(ctx, check.q, check.args...).Scan(&n); err != nil || n != check.want {
			return fmt.Errorf("restore dependency responsibility changed: %v", err)
		}
	}
	return nil
}

func copyRestoreMaterials(proof *loom.RestoreMaterials) *loom.RestoreMaterials {
	if proof == nil {
		return nil
	}
	var copy loom.RestoreMaterials
	if err := json.Unmarshal(encode(proof), &copy); err != nil {
		panic(err)
	}
	return &copy
}

func (service *Service) saveRestore(step *resourceStep, next stepState) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, _, err = service.checkProvisionIntent(tx, *step); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running') AND body=?", encode(next), step.OperationID, step.Key, step.Hash, encode(step.State))); err != nil {
		return err
	}
	if next.DiskIdentity != nil && step.State.DiskIdentity == nil {
		previous := resourceVersion{
			Resource: step.Input.Resource,
		}
		version := resourceVersion{
			Resource:     step.Input.Resource,
			DiskIdentity: next.DiskIdentity,
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE resource_versions SET body=? WHERE id=? AND vm_id=? AND operation_id=? AND body=?", encode(version), step.OperationID, step.Candidate, step.OperationID, encode(previous))); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State, step.Status = next, "running"
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault(next.Phase)
}

func (service *Service) transferRestore(step *resourceStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, _, err = service.checkProvisionIntent(tx, *step); err != nil {
		return err
	}
	if step.State.Materials == nil || !step.State.Materials.Complete || step.State.DiskIdentity == nil || step.State.BackingReady {
		return fmt.Errorf("restore handoff lacks complete original proof")
	}
	source := step.Input.Restore.Snapshot
	next := step.State
	next.BackingReady = true
	next.Phase = "restore_resources_ready"
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO backing VALUES(?,?,?)", []any{
			filepath.Join(step.Input.Resource.RestoreDir, "memory-ranges"),
			step.Candidate,
			source.Manifest.Files["ch/memory-ranges"],
		}},
		{"INSERT INTO resource_refs VALUES('snapshot',?,'backing',?)", []any{
			source.ID,
			step.Input.BackingHolder,
		}},
		{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{
			source.ID,
			step.OperationID + ":resource",
		}},
		{"UPDATE quota_refs SET holder_kind='active_use',holder_id=? WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{
			"backing:" + step.Input.BackingHolder,
			step.Input.TenantID,
			source.ID,
			step.OperationID + ":resource",
		}},
		{"DELETE FROM source_leases WHERE snapshot_id=? AND operation_id=? AND step_key=?", []any{
			source.ID,
			step.OperationID,
			step.Key,
		}},
		{"UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND body=? AND status='running'", []any{
			encode(next),
			step.OperationID,
			step.Key,
			step.Hash,
			encode(step.State),
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("restore_transfer_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = next
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault(next.Phase)
}
