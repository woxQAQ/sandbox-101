package haven

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/loom"
)

// The runtime fixture supplies a complete static RAM topology and sparse file.
// Actual private mapping and CH behavior remain separate native/KVM boundaries.
type restorableRuntime struct {
	*coldRuntime
}

func (backend *restorableRuntime) Capture(ctx context.Context, r loom.Resource, directory string) error {
	if err := backend.coldRuntime.Capture(ctx, r, directory); err != nil {
		return err
	}
	size := r.Config.Shape.MemoryMiB << 20
	manager := map[string]any{
		"boot_ram":    size,
		"current_ram": size,
		"guest_ram_mappings": []any{
			map[string]any{
				"gpa":         0,
				"size":        size,
				"zone_id":     "mem0",
				"virtio_mem":  false,
				"file_offset": 0,
			},
		},
		"memory_ranges": map[string]any{
			"data": []any{
				map[string]any{
					"gpa":    0,
					"length": size,
				},
			},
		},
	}
	state := map[string]any{
		"snapshots": map[string]any{
			"memory-manager": map[string]any{
				"snapshot_data": map[string]any{
					"state": string(encode(manager)),
				},
			},
		},
	}
	if err := os.WriteFile(filepath.Join(directory, "state.json"), encode(state), 0o600); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(directory, "memory-ranges"), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	truncateErr := file.Truncate(size)
	return errors.Join(truncateErr, file.Close())
}

func cloneFixture(t *testing.T) (*Service, *restorableRuntime, Operation) {
	t.Helper()
	service, base := coldService(t, nil)
	backend := &restorableRuntime{base}
	service.opts.Backend = backend
	capture, _ := captureReady(t, service, "clone-source")
	step := awaitCapture(t, service, capture.ID)
	capturePublished(t, service, step, true)
	return service, backend, capture
}

func reopenCloneService(t *testing.T, old *Service, backend loom.Backend) *Service {
	t.Helper()
	if err := old.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts := old.opts
	opts.Initialize = false
	opts.Fault = nil
	opts.Backend = backend
	service, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = service.Close(context.Background())
	})
	return service
}

func admitCloneFixture(t *testing.T, service *Service, snapshot, key string) Operation {
	t.Helper()
	operation, err := service.AdmitClone(context.Background(), CloneRequest{
		TenantID:   "tenant",
		Key:        key,
		SnapshotID: snapshot,
	})
	if err != nil && !errors.Is(err, ErrInterrupted) {
		t.Fatal(err)
	}
	return operation
}

func cloneLiability(t *testing.T, service *Service, step resourceStep, ready bool) {
	t.Helper()
	var lease, backing, storage int
	if err := service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM source_leases WHERE operation_id=?),(SELECT COUNT(*) FROM backing WHERE vm_id=?),(SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?)", step.OperationID, step.Candidate, step.OperationID).Scan(&lease, &backing, &storage); err != nil {
		t.Fatal(err)
	}
	if storage != 64 || step.State.BackingReady && (lease != 0 || backing != 1) || !step.State.BackingReady && (lease != 1 || backing != 0) {
		t.Fatal("restore source/disk responsibility is discontinuous", lease, backing, storage)
	}
	if ready != (step.Status == "succeeded") || ready != (step.State.Witness != "") {
		t.Fatal("clone instance publication disagrees", step.State)
	}
}

func TestHavenRestoreResponsibility(t *testing.T) {
	parallelLifecycle(t)
	for _, point := range []string{
		"admission_committed",
		"restore_prepare_intent",
		"restore_candidate_prepared",
		"restore_candidate_copied",
		"restore_candidate_renamed",
		"restore_candidate_synced",
		"restore_materials_complete",
		"restore_resources_ready",
		"step_launch_intent",
		"effect_launched",
		"step_launched",
		"step_booted",
		"step_agent_ready",
		"completion_before_commit",
	} {
		t.Run(point, func(t *testing.T) {
			service, backend, source := cloneFixture(t)
			service.opts.Fault = func(candidate string) error {
				if candidate == point {
					return ErrInterrupted
				}
				return nil
			}
			operation := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
			if point != "admission_committed" {
				awaitColdWorker(t, service, operation.ID)
			}
			service = reopenCloneService(t, service, backend)
			step := awaitColdWorker(t, service, operation.ID)
			incomplete := point == "restore_candidate_prepared" || point == "restore_candidate_copied" || point == "restore_candidate_renamed" || point == "restore_candidate_synced"
			cloneLiability(t, service, step, !incomplete)
			if incomplete && (step.Status != "failed" || !step.State.RestoreBlocked || step.State.Witness != "") {
				t.Fatal("incomplete restore was promoted", step.State)
			}
			launches := 2
			if incomplete {
				launches = 1
			}
			if backend.count("launch") != launches {
				t.Fatal("restore allocated another instance", backend.count("launch"), launches)
			}
		})
	}
}

func TestHavenRestoreSourceDeletionAfterHandoff(t *testing.T) {
	parallelLifecycle(t)
	service, backend, source := cloneFixture(t)
	service.opts.Fault = func(point string) error {
		if point == "restore_resources_ready" {
			return ErrInterrupted
		}
		return nil
	}
	operation := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
	step := awaitColdWorker(t, service, operation.ID)
	if !step.State.BackingReady {
		t.Fatal("fixture did not transfer source lease")
	}
	snapshot := step.Input.Restore.Snapshot
	// This private fixture models a completed source catalog deletion. Its backing
	// holder and conservative D+M charge deliberately survive removal of the names.
	tx, err := service.store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"DELETE FROM checkpoints WHERE id=?",
		"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='checkpoint'",
		"DELETE FROM quota_refs WHERE kind='snapshot' AND resource_key=? AND holder_id='checkpoint:'||resource_key",
		"DELETE FROM snapshots WHERE id=?",
	} {
		if _, err = tx.Exec(statement, snapshot.ID); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(snapshot.Dir); err != nil {
		t.Fatal(err)
	}
	service = reopenCloneService(t, service, backend)
	step = awaitColdWorker(t, service, operation.ID)
	cloneLiability(t, service, step, true)
	if err = loom.VerifyRestoreMaterials(step.State.Materials, step.Input.Resource, snapshot.Manifest.Files, false); err != nil {
		t.Fatal("retained backing proof depended on deleted source", err)
	}
	if backend.count("launch") != 2 {
		t.Fatal("handoff adoption relaunched candidate")
	}
}

func TestHavenRestoreKeepsRunningWrites(t *testing.T) {
	parallelLifecycle(t)
	service, backend, source := cloneFixture(t)
	service.opts.Fault = func(point string) error {
		if point == "effect_launched" {
			return ErrInterrupted
		}
		return nil
	}
	operation := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
	before := awaitColdWorker(t, service, operation.ID)
	file, err := os.OpenFile(before.Input.Resource.Disk, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt([]byte("retained guest writes"), 0)
	if err = errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	service = reopenCloneService(t, service, backend)
	after := awaitColdWorker(t, service, operation.ID)
	cloneLiability(t, service, after, true)
	file, err = os.Open(after.Input.Resource.Disk)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, len("retained guest writes"))
	_, readErr := file.ReadAt(data, 0)
	if err = errors.Join(readErr, file.Close()); err != nil || string(data) != "retained guest writes" {
		t.Fatal("live disk was recopied or compared to delivery digest", err)
	}
	if backend.count("launch") != 2 {
		t.Fatal("live candidate relaunched")
	}
}

func TestHavenRestoreHandoffRejectsSQL(t *testing.T) {
	parallelLifecycle(t)
	for _, write := range []struct {
		event, table, condition string
	}{
		{"INSERT", "backing", "1"},
		{"DELETE", "source_leases", "1"},
		{"UPDATE", "quota_refs", "NEW.holder_id LIKE 'backing:%'"},
		{"UPDATE", "operation_steps", "NEW.action='clone' AND json_extract(NEW.body,'$.backing_ready')=1"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture handoff rejection'",
			"IGNORE",
		} {
			t.Run(write.table+"/"+mode, func(t *testing.T) {
				service, backend, source := cloneFixture(t)
				sql := fmt.Sprintf("CREATE TRIGGER deny_restore_handoff BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.store.db.Exec(sql); err != nil {
					t.Fatal(err)
				}
				operation := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
				step := awaitColdWorker(t, service, operation.ID)
				if step.State.BackingReady || terminal(step.Status) || step.State.Phase != "restore_materials_complete" {
					t.Fatal("rejected handoff became visible", step.State)
				}
				cloneLiability(t, service, step, false)
				if backend.count("launch") != 1 {
					t.Fatal("candidate launched before source handoff")
				}
				if _, err := service.store.db.Exec("DROP TRIGGER deny_restore_handoff"); err != nil {
					t.Fatal(err)
				}
				service = reopenCloneService(t, service, backend)
				step = awaitColdWorker(t, service, operation.ID)
				cloneLiability(t, service, step, true)
			})
		}
	}
}
