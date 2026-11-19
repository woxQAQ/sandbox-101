package workflows_test

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenRestoreResponsibility(t *testing.T) {
	fixtures.ParallelLifecycle(t)
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
			service, backend, source := fixtures.CloneFixtureWithFault(t, faults.Check)
			faults.Set(func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation := fixtures.AdmitCloneFixture(t, service, source.Result.SnapshotID, "clone")
			if point != "admission_committed" {
				fixtures.AwaitColdWorker(t, service, acceptedOperation.ID)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			step := fixtures.AwaitColdWorker(t, service, acceptedOperation.ID)
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
	fixtures.ParallelLifecycle(t)
	service, backend, source := fixtures.CloneFixtureWithFault(t, func(point string) error {
		if point == "restore_resources_ready" {
			return errs.ErrInterrupted
		}
		return nil
	})
	acceptedOperation := fixtures.AdmitCloneFixture(t, service, source.Result.SnapshotID, "clone")
	step := fixtures.AwaitColdWorker(t, service, acceptedOperation.ID)
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
	step = fixtures.AwaitColdWorker(t, service, acceptedOperation.ID)
	cloneLiability(t, service, step, true)
	if err = loom.VerifyRestoreMaterials(step.State.Materials, step.Input.Resource, snapshot.Manifest.Files, false); err != nil {
		t.Fatal("retained backing proof depended on deleted source", err)
	}
	if backend.Count("launch") != 2 {
		t.Fatal("handoff adoption relaunched candidate")
	}
}

func TestHavenRestoreKeepsRunningWrites(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	faults := &testfixture.SnapshotFaults{}
	service, backend, source := fixtures.CloneFixtureWithFault(t, faults.Check)
	faults.Set(func(point string) error {
		if point == "effect_launched" {
			return errs.ErrInterrupted
		}
		return nil
	})
	acceptedOperation := fixtures.AdmitCloneFixture(t, service, source.Result.SnapshotID, "clone")
	before := fixtures.AwaitColdWorker(t, service, acceptedOperation.ID)
	file, err := os.OpenFile(before.Input.Resource.Disk, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt([]byte("retained guest writes"), 0)
	if err = errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	after := fixtures.AwaitColdWorker(t, service, acceptedOperation.ID)
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
	fixtures.ParallelLifecycle(t)
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
				acceptedOperation := fixtures.AdmitCloneFixture(t, service, source.Result.SnapshotID, "clone")
				step := fixtures.AwaitColdWorker(t, service, acceptedOperation.ID)
				if step.State.BackingReady || fixtures.Terminal(step.Status) || step.State.Phase != "restore_materials_complete" {
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
				step = fixtures.AwaitColdWorker(t, service, acceptedOperation.ID)
				cloneLiability(t, service, step, true)
			})
		}
	}
}
