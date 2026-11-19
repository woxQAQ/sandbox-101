package machine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenSnapshotDeleteAtomicRetirement(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, write := range []struct {
		event     string
		table     string
		condition string
	}{
		{"DELETE", "checkpoints", "1"},
		{"DELETE", "snapshots", "1"},
		{"DELETE", "reservations", "OLD.owner LIKE 'snapshot:%'"},
		{"DELETE", "quota_resources", "OLD.kind='snapshot'"},
		{"DELETE", "deletion_intents", "1"},
		{"DELETE", "resource_refs", "OLD.holder_kind='snapshot'"},
		{"UPDATE", "operation_steps", "NEW.action='snapshot_delete' AND NEW.status='succeeded'"},
		{"UPDATE", "operations", "NEW.status='succeeded'"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture snapshot retirement rejection'",
			"IGNORE",
		} {
			t.Run(write.table+mode, func(t *testing.T) {
				fixture := snapshotDeleteFixture(t, 0)
				service := fixture.Service
				statement := fmt.Sprintf("CREATE TRIGGER deny_snapshot_retirement BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				operation := admitSnapshotDeleteFixture(t, fixture)
				step := awaitSnapshotDelete(t, service, operation.ID)
				if fixtures.Terminal(step.Status) || step.Phase != "files_removed" {
					t.Fatal("snapshot retirement denial committed", step.Phase)
				}
				snapshotDeleteCost(t, fixture, 192)
				if _, err := os.Stat(step.Input.Source.Snapshot.Dir); !os.IsNotExist(err) {
					t.Fatal("fixture did not reclaim snapshot bytes", err)
				}
				if _, err := service.State.Database().Exec("DROP TRIGGER deny_snapshot_retirement"); err != nil {
					t.Fatal(err)
				}
				fixture.Service = testapp.ReopenCloneService(t, service, fixture.Backend)
				if done := awaitSnapshotDelete(t, fixture.Service, operation.ID); done.Status != "succeeded" {
					reason := fixture.Service.Workers.State(operation.ID).Err
					t.Fatal(done.Phase, reason)
				}
				snapshotDeleteCost(t, fixture, 0)
			})
		}
	}
}

func TestHavenSnapshotDeleteBackingRetirement(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, mode := range []string{
		"ABORT,'fixture backing fee rejection'",
		"IGNORE",
	} {
		t.Run(mode, func(t *testing.T) {
			fixture := snapshotDeleteFixture(t, 1)
			service := fixture.Service
			operation := admitSnapshotDeleteFixture(t, fixture)
			if step := awaitSnapshotDelete(t, service, operation.ID); step.Status != "succeeded" {
				t.Fatal(step.Phase)
			}
			if _, err := service.State.Database().Exec(fmt.Sprintf("CREATE TRIGGER deny_backing_fee BEFORE DELETE ON quota_resources WHEN OLD.kind='snapshot' BEGIN SELECT RAISE(%s); END", mode)); err != nil {
				t.Fatal(err)
			}
			clone := fixture.Clones[0]
			release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
				TenantID:  "tenant",
				Key:       "release-last-backing",
				SandboxID: clone.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			step := fixtures.AwaitColdWorker(t, service, release.ID)
			if fixtures.Terminal(step.Status) || step.State.Phase != "files_removed" {
				t.Fatal("backing fee denial partially retired VM", step.State)
			}
			snapshotDeleteCost(t, fixture, 192)
			var backing, versions int
			if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM backing WHERE vm_id=?),(SELECT COUNT(*) FROM resource_versions WHERE vm_id=?)", clone.Result.VMID, clone.Result.VMID).Scan(&backing, &versions); err != nil || backing != 1 || versions != 1 {
				t.Fatal("last backing retirement not atomic", backing, versions, err)
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER deny_backing_fee"); err != nil {
				t.Fatal(err)
			}
			fixture.Service = testapp.ReopenCloneService(t, service, fixture.Backend)
			if done := fixtures.AwaitColdWorker(t, fixture.Service, release.ID); done.Status != "succeeded" {
				reason := fixture.Service.Workers.State(release.ID).Err
				t.Fatal(done.State, reason)
			}
			snapshotDeleteCost(t, fixture, 0)
		})
	}
}

func TestHavenSnapshotDeleteRetainsUncertainObjects(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, kind := range []string{
		"unknown",
		"replaced",
		"missing",
		"corrupt",
		"reader",
		"raw_reappeared",
	} {
		t.Run(kind, func(t *testing.T) {
			fixture := snapshotDeleteFixture(t, 0)
			service := fixture.Service
			snapshot, err := machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), "tenant", fixture.Capture.Result.SnapshotID)
			if err != nil {
				t.Fatal(err)
			}
			marker := ""
			switch kind {
			case "unknown":
				marker = filepath.Join(snapshot.Dir, "unknown")
				if err = os.WriteFile(marker, []byte("retained"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "replaced", "missing":
				if err = os.Rename(snapshot.Dir, snapshot.Dir+".held"); err != nil {
					t.Fatal(err)
				}
				if kind == "replaced" {
					if err = os.Mkdir(snapshot.Dir, 0o700); err != nil {
						t.Fatal(err)
					}
					marker = filepath.Join(snapshot.Dir, "marker")
					if err = os.WriteFile(marker, []byte("retained"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			case "corrupt":
				path := filepath.Join(snapshot.Dir, "ch/state.json")
				if err = os.Chmod(path, 0o600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, []byte("damaged"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "reader":
				file, err := os.Open(filepath.Join(snapshot.Dir, "ch/memory-ranges"))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = file.Close()
				}()
			case "raw_reappeared":
				capture, err := machine.LoadCapture(context.Background(), service.State.Database(), fixture.Capture.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.MkdirAll(capture.State.Boundary.Path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			operation := admitSnapshotDeleteFixture(t, fixture)
			if step := awaitSnapshotDelete(t, service, operation.ID); fixtures.Terminal(step.Status) {
				t.Fatal("uncertain snapshot retired", step.Phase)
			}
			snapshotDeleteCost(t, fixture, 192)
			if marker != "" {
				if data, err := os.ReadFile(marker); err != nil || string(data) != "retained" {
					t.Fatal("unknown material removed", err)
				}
			}
		})
	}
}

func TestHavenSnapshotDeleteFailedBacking(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 1)
	service := fixture.Service
	fixture.Runtime.FailBoot = true
	failedOperation := fixtures.AdmitCloneFixture(t, service, fixture.Capture.Result.SnapshotID, "failed-retained-clone")
	failed := fixtures.AwaitColdWorker(t, service, failedOperation.ID)
	fixture.Runtime.FailBoot = false
	if failed.Status != "failed" || !failed.State.BackingReady {
		t.Fatal("fixture did not retain failed backing", failed.State)
	}
	deletion := admitSnapshotDeleteFixture(t, fixture)
	if step := awaitSnapshotDelete(t, service, deletion.ID); step.Status != "succeeded" {
		reason := service.Workers.State(deletion.ID).Err
		t.Fatal(step.Phase, reason)
	}
	release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release-other-backing",
		SandboxID: fixture.Clones[0].Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if step := fixtures.AwaitColdWorker(t, service, release.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	snapshotDeleteCost(t, fixture, 192)
	discard, err := service.Deletion.AdmitDiscard(context.Background(), machine.DiscardRequest{
		TenantID:          "tenant",
		Key:               "discard-last-backing",
		SandboxID:         failedOperation.Result.SandboxID,
		FailedOperationID: failedOperation.ID,
		ExpectedInstance:  failed.Input.Resource.Instance,
	})
	if err != nil {
		t.Fatal(err)
	}
	if step := fixtures.AwaitDiscard(t, service, discard.ID); step.Status != "succeeded" {
		reason := service.Workers.State(discard.ID).Err
		t.Fatal(step.State, reason)
	}
	snapshotDeleteCost(t, fixture, 0)
}

func TestHavenSnapshotDeleteUnlinkedReader(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 0)
	service := fixture.Service
	fixture.Faults.Set(func(point string) error {
		if point == "snapshot_delete_receipt" {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation := admitSnapshotDeleteFixture(t, fixture)
	step := awaitSnapshotDelete(t, service, operation.ID)
	if step.Phase != "deleting" {
		t.Fatal(step.Phase)
	}
	file, err := os.Open(filepath.Join(step.Input.Source.Snapshot.Dir, "ch/memory-ranges"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = file.Close()
	}()
	// Model loss of all directory entries after the exact receipt committed. The
	// still-open inode remains real material even though pathname lookup is absent.
	if err = os.RemoveAll(step.Input.Source.Snapshot.Dir); err != nil {
		t.Fatal(err)
	}
	fixture.Service = testapp.ReopenCloneService(t, service, fixture.Backend)
	if pending := awaitSnapshotDelete(t, fixture.Service, operation.ID); fixtures.Terminal(pending.Status) {
		t.Fatal("unlinked reader was treated as reclaimed")
	}
	snapshotDeleteCost(t, fixture, 192)
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.Service = testapp.ReopenCloneService(t, fixture.Service, fixture.Backend)
	if done := awaitSnapshotDelete(t, fixture.Service, operation.ID); done.Status != "succeeded" {
		t.Fatal(done.Phase)
	}
	snapshotDeleteCost(t, fixture, 0)
}

func TestHavenSnapshotDelete(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, clones := range []int{
		0,
		2,
	} {
		t.Run(fmt.Sprint(clones), func(t *testing.T) {
			fixture := snapshotDeleteFixture(t, clones)
			service := fixture.Service
			original, err := service.Operations.GetOperation(context.Background(), "tenant", fixture.Capture.ID)
			if err != nil {
				t.Fatal(err)
			}
			acceptedOperation := admitSnapshotDeleteFixture(t, fixture)
			done := awaitSnapshotDelete(t, service, acceptedOperation.ID)
			if done.Status != "succeeded" {
				reason := service.Workers.State(acceptedOperation.ID).Err
				t.Fatal("snapshot delete failed", done.Phase, reason)
			}
			if _, err = os.Stat(done.Input.Source.Snapshot.Dir); !os.IsNotExist(err) {
				t.Fatal("snapshot bytes retained", err)
			}
			if _, err = machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), "tenant", fixture.Capture.Result.SnapshotID); err == nil {
				t.Fatal("deleted snapshot still in catalog")
			}
			if _, err = service.Provisioning.AdmitClone(context.Background(), machine.CloneRequest{
				TenantID:   "tenant",
				Key:        "clone-deleted",
				SnapshotID: fixture.Capture.Result.SnapshotID,
			}); err == nil {
				t.Fatal("deleted snapshot acquired new user")
			}
			want := 0
			if clones > 0 {
				want = 192
			}
			snapshotDeleteCost(t, fixture, want)
			fixture.Service = testapp.ReopenCloneService(t, service, fixture.Backend)
			service = fixture.Service
			for index, clone := range fixture.Clones {
				source, err := machine.LoadResourceStep(context.Background(), service.State.Database(), clone.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err = loom.VerifyRestoreMaterials(source.State.Materials, source.Input.Resource, source.Input.Restore.Snapshot.Manifest.Files, false); err != nil {
					t.Fatal("deletion damaged retained backing", err)
				}
				release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
					TenantID:  "tenant",
					Key:       fmt.Sprintf("release-deleted-clone-%d", index),
					SandboxID: clone.Result.SandboxID,
				})
				if err != nil {
					t.Fatal(err)
				}
				if proof := fixtures.AwaitColdWorker(t, service, release.ID); proof.Status != "succeeded" {
					reason := service.Workers.State(release.ID).Err
					t.Fatal("backing release failed", proof.State, reason)
				}
				want = 192
				if index == len(fixture.Clones)-1 {
					want = 0
				}
				snapshotDeleteCost(t, fixture, want)
			}
			after, err := service.Operations.GetOperation(context.Background(), "tenant", fixture.Capture.ID)
			if err != nil || !reflect.DeepEqual(after, original) {
				t.Fatal("snapshot delete changed original capture result", err)
			}
			replay, err := service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
				TenantID:   "tenant",
				Key:        "delete-snapshot",
				SnapshotID: fixture.Capture.Result.SnapshotID,
			})
			if err != nil || replay.ID != acceptedOperation.ID || replay.Status != "succeeded" {
				t.Fatal("snapshot delete same-key replay changed", replay, err)
			}
		})
	}
}

func TestHavenSnapshotDeleteRecovery(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, point := range []string{
		"snapshot_delete_admitted",
		"snapshot_delete_receipt",
		"snapshot_delete_unlinked",
		"snapshot_delete_directory_removed",
		"snapshot_delete_files_removed",
		"snapshot_delete_completion_before_commit",
		"snapshot_delete_completed",
	} {
		t.Run(point, func(t *testing.T) {
			fixture := snapshotDeleteFixture(t, 0)
			fixture.Faults.Set(func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation := admitSnapshotDeleteFixture(t, fixture)
			if point != "snapshot_delete_admitted" {
				awaitSnapshotDelete(t, fixture.Service, acceptedOperation.ID)
			}
			fixture.Service = testapp.ReopenCloneService(t, fixture.Service, fixture.Backend)
			if step := awaitSnapshotDelete(t, fixture.Service, acceptedOperation.ID); step.Status != "succeeded" {
				reason := fixture.Service.Workers.State(acceptedOperation.ID).Err
				t.Fatal(step.Phase, reason)
			}
			snapshotDeleteCost(t, fixture, 0)
		})
	}
}

type snapshotDeletionFixture struct {
	Faults  *testfixture.SnapshotFaults
	Service *application.App
	Backend loom.Backend
	Runtime *testfixture.ColdRuntime
	Capture operation.Record
	Clones  []operation.Record
}

func snapshotDeleteFixture(t *testing.T, clones int) *snapshotDeletionFixture {
	t.Helper()
	faults := &testfixture.SnapshotFaults{}
	var factory func(*testfixture.ColdRuntime) loom.Backend
	if clones > 0 {
		factory = fixtures.RestorableBackend
	}
	service, runtime := fixtures.ColdServiceWithBackend(t, faults.Check, factory)
	backend := service.Options.Backend
	capture, _ := fixtures.CaptureReady(t, service, "delete-source")
	if step := fixtures.AwaitCapture(t, service, capture.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	fixture := &snapshotDeletionFixture{
		Faults:  faults,
		Service: service,
		Runtime: runtime,
		Backend: backend,
		Capture: capture,
	}
	for index := range clones {
		acceptedOperation := fixtures.AdmitCloneFixture(t, service, capture.Result.SnapshotID, fmt.Sprintf("delete-clone-%d", index))
		if step := fixtures.AwaitColdWorker(t, service, acceptedOperation.ID); step.Status != "succeeded" {
			t.Fatal(step.State)
		}
		fixture.Clones = append(fixture.Clones, acceptedOperation)
	}
	original, err := machine.LoadCapture(context.Background(), service.State.Database(), capture.ID)
	if err != nil {
		t.Fatal(err)
	}
	release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release-snapshot-source",
		SandboxID: original.Input.Request.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if step := fixtures.AwaitColdWorker(t, service, release.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	return fixture
}

func awaitSnapshotDelete(t *testing.T, service *application.App, id string) machine.SnapshotDeleteStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, seen := worker.Active, worker.Completed
		step, err := machine.LoadSnapshotDelete(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || fixtures.Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("snapshot deletion worker timed out", step.Phase)
		}
		time.Sleep(time.Millisecond)
	}
}

func snapshotDeleteCost(t *testing.T, fixture *snapshotDeletionFixture, want int) {
	t.Helper()
	var cost, owner int
	if err := fixture.Service.State.Database().QueryRow("SELECT (SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE kind='snapshot' AND resource_key=?),(SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=?)", fixture.Capture.Result.SnapshotID, fixture.Capture.Result.SnapshotID).Scan(&cost, &owner); err != nil || cost != want || owner != 1 {
		t.Fatal("snapshot debt or permanent owner changed", cost, owner, err)
	}
}

func admitSnapshotDeleteFixture(t *testing.T, fixture *snapshotDeletionFixture) operation.Record {
	t.Helper()
	acceptedOperation, err := fixture.Service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
		TenantID:   "tenant",
		Key:        "delete-snapshot",
		SnapshotID: fixture.Capture.Result.SnapshotID,
	})
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation
}
