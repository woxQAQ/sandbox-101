package component

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

// The runtime fixture supplies a complete static RAM topology and sparse file.
// Actual private mapping and CH behavior remain separate native/KVM boundaries.
type restorableRuntime struct {
	*testfixture.ColdRuntime
}

func restorableBackend(runtime *testfixture.ColdRuntime) loom.Backend {
	return &restorableRuntime{runtime}
}

func (backend *restorableRuntime) Capture(ctx context.Context, r loom.Resource, directory string) error {
	if err := backend.ColdRuntime.Capture(ctx, r, directory); err != nil {
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
					"state": string(testfixture.Encode(manager)),
				},
			},
		},
	}
	if err := os.WriteFile(filepath.Join(directory, "state.json"), testfixture.Encode(state), 0o600); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(directory, "memory-ranges"), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	truncateErr := file.Truncate(size)
	return errors.Join(truncateErr, file.Close())
}

func cloneFixture(t *testing.T) (*application.App, *restorableRuntime, operation.Record) {
	t.Helper()
	return cloneFixtureWithFault(t, nil)
}

func cloneFixtureWithFault(t *testing.T, fault func(string) error) (*application.App, *restorableRuntime, operation.Record) {
	t.Helper()
	service, _ := coldServiceWithBackend(t, fault, restorableBackend)
	backend := service.Options.Backend.(*restorableRuntime)
	capture, _ := captureReady(t, service, "clone-source")
	step := awaitCapture(t, service, capture.ID)
	capturePublished(t, service, step, true)
	return service, backend, capture
}

func admitCloneFixture(t *testing.T, service *application.App, snapshot, key string) operation.Record {
	t.Helper()
	acceptedOperation, err := service.VMProvisioning.AdmitClone(context.Background(), machine.CloneRequest{
		TenantID:   "tenant",
		Key:        key,
		SnapshotID: snapshot,
	})
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation
}

func cloneLiability(t *testing.T, service *application.App, step machine.ResourceStep, ready bool) {
	t.Helper()
	var lease, backing, storage int
	if err := service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM source_leases WHERE operation_id=?),(SELECT COUNT(*) FROM backing WHERE vm_id=?),(SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?)", step.OperationID, step.Candidate, step.OperationID).Scan(&lease, &backing, &storage); err != nil {
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
			faults := &testfixture.SnapshotFaults{}
			service, backend, source := cloneFixtureWithFault(t, faults.Check)
			faults.Set(func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
			if point != "admission_committed" {
				awaitColdWorker(t, service, acceptedOperation.ID)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			step := awaitColdWorker(t, service, acceptedOperation.ID)
			incomplete := point == "restore_candidate_prepared" || point == "restore_candidate_copied" || point == "restore_candidate_renamed" || point == "restore_candidate_synced"
			cloneLiability(t, service, step, !incomplete)
			if incomplete && (step.Status != "failed" || !step.State.RestoreBlocked || step.State.Witness != "") {
				t.Fatal("incomplete restore was promoted", step.State)
			}
			launches := 2
			if incomplete {
				launches = 1
			}
			if backend.Count("launch") != launches {
				t.Fatal("restore allocated another instance", backend.Count("launch"), launches)
			}
		})
	}
}

func TestHavenRestoreSourceDeletionAfterHandoff(t *testing.T) {
	parallelLifecycle(t)
	service, backend, source := cloneFixtureWithFault(t, func(point string) error {
		if point == "restore_resources_ready" {
			return errs.ErrInterrupted
		}
		return nil
	})
	acceptedOperation := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
	step := awaitColdWorker(t, service, acceptedOperation.ID)
	if !step.State.BackingReady {
		t.Fatal("fixture did not transfer source lease")
	}
	snapshot := step.Input.Restore.Snapshot
	// This private fixture models a completed source catalog deletion. Its backing
	// holder and conservative D+M charge deliberately survive removal of the names.
	tx, err := service.State.Database().Begin()
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
	service = testapp.ReopenCloneService(t, service, backend)
	step = awaitColdWorker(t, service, acceptedOperation.ID)
	cloneLiability(t, service, step, true)
	if err = loom.VerifyRestoreMaterials(step.State.Materials, step.Input.Resource, snapshot.Manifest.Files, false); err != nil {
		t.Fatal("retained backing proof depended on deleted source", err)
	}
	if backend.Count("launch") != 2 {
		t.Fatal("handoff adoption relaunched candidate")
	}
}

func TestHavenRestoreKeepsRunningWrites(t *testing.T) {
	parallelLifecycle(t)
	faults := &testfixture.SnapshotFaults{}
	service, backend, source := cloneFixtureWithFault(t, faults.Check)
	faults.Set(func(point string) error {
		if point == "effect_launched" {
			return errs.ErrInterrupted
		}
		return nil
	})
	acceptedOperation := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
	before := awaitColdWorker(t, service, acceptedOperation.ID)
	file, err := os.OpenFile(before.Input.Resource.Disk, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt([]byte("retained guest writes"), 0)
	if err = errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	after := awaitColdWorker(t, service, acceptedOperation.ID)
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
	if backend.Count("launch") != 2 {
		t.Fatal("live candidate relaunched")
	}
}

func TestHavenRestoreHandoffRejectsSQL(t *testing.T) {
	parallelLifecycle(t)
	for _, write := range []struct {
		event     string
		table     string
		condition string
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
				if _, err := service.State.Database().Exec(sql); err != nil {
					t.Fatal(err)
				}
				acceptedOperation := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
				step := awaitColdWorker(t, service, acceptedOperation.ID)
				if step.State.BackingReady || terminal(step.Status) || step.State.Phase != "restore_materials_complete" {
					t.Fatal("rejected handoff became visible", step.State)
				}
				cloneLiability(t, service, step, false)
				if backend.Count("launch") != 1 {
					t.Fatal("candidate launched before source handoff")
				}
				if _, err := service.State.Database().Exec("DROP TRIGGER deny_restore_handoff"); err != nil {
					t.Fatal(err)
				}
				service = testapp.ReopenCloneService(t, service, backend)
				step = awaitColdWorker(t, service, acceptedOperation.ID)
				cloneLiability(t, service, step, true)
			})
		}
	}
}
