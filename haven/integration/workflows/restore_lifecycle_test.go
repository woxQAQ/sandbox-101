package workflows_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenRestoreReleaseRetiresOnlyClone(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, point := range []string{
		"release_admitted",
		"release_unlinked",
		"release_completion_before_commit",
	} {
		t.Run(point, func(t *testing.T) {
			service, backend, source := testapp.CloneFixtureWithFault(t, func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			clone := testfixture.AdmitCloneFixture(t, service, source.Result.SnapshotID, "clone")
			historical := testutil.AwaitColdWorker(t, service, clone.ID)
			cloneLiability(t, service, historical, true)
			frozen := operation.Encode(historical)
			release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
				TenantID:  "tenant",
				Key:       "release-clone",
				SandboxID: clone.Result.SandboxID,
			})
			if err != nil && !errors.Is(err, errs.ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "release_admitted" {
				testutil.AwaitColdWorker(t, service, release.ID)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			deleted := testutil.AwaitColdWorker(t, service, release.ID)
			if deleted.Status != "succeeded" {
				reason := service.Workers.State(release.ID).Err
				t.Fatal("clone cleanup did not converge", deleted.State, reason)
			}
			if _, err = os.Lstat(historical.Input.Resource.Dir); !os.IsNotExist(err) {
				t.Fatal("clone files retained after successful deletion", err)
			}
			var backing, held, sourceCost int
			if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM backing WHERE vm_id=?),(SELECT COUNT(*) FROM quota_resources WHERE source_operation_id=?),(SELECT SUM(storage_mib) FROM quota_resources WHERE kind='snapshot' AND resource_key=?)", clone.Result.VMID, clone.ID, source.Result.SnapshotID).Scan(&backing, &held, &sourceCost); err != nil || backing != 0 || held != 0 || sourceCost != 192 {
				t.Fatal("clone release retired source cost or retained clone debt", backing, held, sourceCost, err)
			}
			after, err := machine.LoadResourceStep(context.Background(), service.State.Database(), clone.ID)
			if err != nil || !bytes.Equal(operation.Encode(after), frozen) {
				t.Fatal("cleanup rewrote original restore witness", err)
			}
			snapshot, err := machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), "tenant", source.Result.SnapshotID)
			if err != nil || snapshot.Manifest == nil {
				t.Fatal("clone release retired source snapshot", err)
			}
		})
	}
}

func TestHavenRestoreLeaseAndIncompleteProof(t *testing.T) {
	testutil.ParallelLifecycle(t)
	t.Run("source-lease-fk", func(t *testing.T) {
		faults := &testfixture.SnapshotFaults{}
		service, backend, source := testapp.CloneFixtureWithFault(t, faults.Check)
		faults.Set(func(point string) error {
			if point == "admission_committed" {
				return errs.ErrInterrupted
			}
			return nil
		})
		clone := testfixture.AdmitCloneFixture(t, service, source.Result.SnapshotID, "clone")
		if _, err := service.State.Database().Exec("DELETE FROM snapshots WHERE id=?", source.Result.SnapshotID); err == nil {
			t.Fatal("source catalog retired while leased")
		}
		if backend.Count("launch") != 1 {
			t.Fatal("unexecuted clone already launched")
		}
		service = testapp.ReopenCloneService(t, service, backend)
		cloneLiability(t, service, testutil.AwaitColdWorker(t, service, clone.ID), true)
	})
	for _, damage := range []string{
		"disk",
		"memory-inode",
		"missing-proof",
	} {
		t.Run(damage, func(t *testing.T) {
			service, backend, source := testapp.CloneFixtureWithFault(t, func(point string) error {
				if point == "restore_resources_ready" {
					return errs.ErrInterrupted
				}
				return nil
			})
			clone := testfixture.AdmitCloneFixture(t, service, source.Result.SnapshotID, "clone")
			before := testutil.AwaitColdWorker(t, service, clone.ID)
			switch damage {
			case "disk":
				if err := os.Truncate(before.Input.Resource.Disk, 0); err != nil {
					t.Fatal(err)
				}
			case "memory-inode":
				path := filepath.Join(before.Input.Resource.RestoreDir, "memory-ranges")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o444)
				if err != nil {
					t.Fatal(err)
				}
				truncateErr := file.Truncate(before.Input.Resource.Config.Shape.MemoryMiB << 20)
				if err = errors.Join(truncateErr, file.Close()); err != nil {
					t.Fatal(err)
				}
			case "missing-proof":
				state := before.State
				state.Materials = nil
				if _, err := service.State.Database().Exec("UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='resource'", operation.Encode(state), clone.ID); err != nil {
					t.Fatal(err)
				}
			}
			service = testapp.ReopenCloneService(t, service, backend)
			after := testutil.AwaitColdWorker(t, service, clone.ID)
			if after.Status == "succeeded" || after.State.Witness != "" || backend.Count("launch") != 1 {
				t.Fatal("damaged handoff became a runtime witness", after.State)
			}
			if !after.State.BackingReady {
				t.Fatal("failure reconstructed source lease instead of retaining backing")
			}
			var leases, backing, held int
			if err := service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM source_leases WHERE operation_id=?),(SELECT COUNT(*) FROM backing WHERE vm_id=?),(SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?)", clone.ID, clone.Result.VMID, clone.ID).Scan(&leases, &backing, &held); err != nil || leases != 0 || backing != 1 || held != 64 {
				t.Fatal("damaged restore lost conservative debt", leases, backing, held, err)
			}
		})
	}
}

func TestHavenRestoreCloneCanBeCaptured(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend, source := cloneFixture(t)
	clone := testfixture.AdmitCloneFixture(t, service, source.Result.SnapshotID, "clone")
	ready := testutil.AwaitColdWorker(t, service, clone.ID)
	cloneLiability(t, service, ready, true)
	capture, err := service.Captures.AdmitCapture(context.Background(), machine.CaptureRequest{
		TenantID:         "tenant",
		Key:              "clone-capture",
		SandboxID:        clone.Result.SandboxID,
		ExpectedInstance: ready.State.Witness,
	})
	if err != nil {
		t.Fatal(err)
	}
	proof := testutil.AwaitCapture(t, service, capture.ID)
	testutil.CapturePublished(t, service, proof, true)
	if backend.Count("launch") != 2 || backend.Count("capture") != 2 {
		t.Fatal("resnapshot unexpectedly restarted a clone")
	}
}

func TestHavenRestoreHandoffRequiresRunningStep(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend, source := cloneFixture(t)
	// Change the step inside the attempted handoff, after its original proof was
	// loaded. The conditional write must roll back backing, lease and costs.
	if _, err := service.State.Database().Exec(`CREATE TRIGGER change_restore_status
AFTER INSERT ON backing BEGIN
 UPDATE operation_steps SET status='pending'
 WHERE operation_id=(SELECT operation_id FROM vms WHERE id=NEW.vm_id) AND step_key='resource';
END`); err != nil {
		t.Fatal(err)
	}
	accepted := testfixture.AdmitCloneFixture(t, service, source.Result.SnapshotID, "running-fence")
	step := testutil.AwaitColdWorker(t, service, accepted.ID)
	if step.Status != "running" || step.State.BackingReady || step.State.Phase != "restore_materials_complete" {
		t.Fatalf("handoff crossed running fence or failed to roll back: status=%s state=%+v", step.Status, step.State)
	}
	cloneLiability(t, service, step, false)
	if backend.Count("launch") != 1 {
		t.Fatal("candidate launched before source handoff")
	}
	if _, err := service.State.Database().Exec("DROP TRIGGER change_restore_status"); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	step = testutil.AwaitColdWorker(t, service, accepted.ID)
	cloneLiability(t, service, step, true)
	if backend.Count("launch") != 2 {
		t.Fatal("handoff retry repeated a launch")
	}
}

func cloneFixture(t *testing.T) (*application.App, *testfixture.RestorableRuntime, operation.State) {
	t.Helper()
	return testapp.CloneFixtureWithFault(t, nil)
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
