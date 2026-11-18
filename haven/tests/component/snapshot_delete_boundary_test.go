package component

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/machine"
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
				fixture := fixtures.SnapshotDeleteFixture(t, 0)
				service := fixture.Service
				statement := fmt.Sprintf("CREATE TRIGGER deny_snapshot_retirement BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				operation := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
				step := fixtures.AwaitSnapshotDelete(t, service, operation.ID)
				if fixtures.Terminal(step.Status) || step.Phase != "files_removed" {
					t.Fatal("snapshot retirement denial committed", step.Phase)
				}
				fixtures.SnapshotDeleteCost(t, fixture, 192)
				if _, err := os.Stat(step.Input.Source.Snapshot.Dir); !os.IsNotExist(err) {
					t.Fatal("fixture did not reclaim snapshot bytes", err)
				}
				if _, err := service.State.Database().Exec("DROP TRIGGER deny_snapshot_retirement"); err != nil {
					t.Fatal(err)
				}
				fixture.Service = testapp.ReopenCloneService(t, service, fixture.Backend)
				if done := fixtures.AwaitSnapshotDelete(t, fixture.Service, operation.ID); done.Status != "succeeded" {
					reason := fixture.Service.Workers.State(operation.ID).Err
					t.Fatal(done.Phase, reason)
				}
				fixtures.SnapshotDeleteCost(t, fixture, 0)
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
			fixture := fixtures.SnapshotDeleteFixture(t, 1)
			service := fixture.Service
			operation := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
			if step := fixtures.AwaitSnapshotDelete(t, service, operation.ID); step.Status != "succeeded" {
				t.Fatal(step.Phase)
			}
			if _, err := service.State.Database().Exec(fmt.Sprintf("CREATE TRIGGER deny_backing_fee BEFORE DELETE ON quota_resources WHEN OLD.kind='snapshot' BEGIN SELECT RAISE(%s); END", mode)); err != nil {
				t.Fatal(err)
			}
			clone := fixture.Clones[0]
			release, err := service.VMDeletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
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
			fixtures.SnapshotDeleteCost(t, fixture, 192)
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
			fixtures.SnapshotDeleteCost(t, fixture, 0)
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
			fixture := fixtures.SnapshotDeleteFixture(t, 0)
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
			operation := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
			if step := fixtures.AwaitSnapshotDelete(t, service, operation.ID); fixtures.Terminal(step.Status) {
				t.Fatal("uncertain snapshot retired", step.Phase)
			}
			fixtures.SnapshotDeleteCost(t, fixture, 192)
			if marker != "" {
				if data, err := os.ReadFile(marker); err != nil || string(data) != "retained" {
					t.Fatal("unknown material removed", err)
				}
			}
		})
	}
}

func TestHavenSnapshotDeleteAdmissionAtomicity(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, write := range []struct {
		event     string
		table     string
		condition string
	}{
		{"INSERT", "operations", "1"},
		{"UPDATE", "snapshots", "NEW.deleting=1"},
		{"UPDATE", "resource_refs", "NEW.holder_kind='cleanup'"},
		{"UPDATE", "quota_refs", "NEW.holder_kind='cleanup'"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture snapshot admission rejection'",
			"IGNORE",
		} {
			t.Run(write.table+mode, func(t *testing.T) {
				fixture := fixtures.SnapshotDeleteFixture(t, 0)
				service := fixture.Service
				statement := fmt.Sprintf("CREATE TRIGGER deny_snapshot_admission BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				if _, err := service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
					TenantID:   "tenant",
					Key:        "delete-denied",
					SnapshotID: fixture.Capture.Result.SnapshotID,
				}); err == nil {
					t.Fatal("snapshot admission denial ignored")
				}
				var operations, deleting int
				if err := service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM operations WHERE idem_key='delete-denied'),(SELECT deleting FROM snapshots WHERE id=?)", fixture.Capture.Result.SnapshotID).Scan(&operations, &deleting); err != nil || operations != 0 || deleting != 0 {
					t.Fatal("snapshot fence partially committed", operations, deleting, err)
				}
				fixtures.SnapshotDeleteCost(t, fixture, 192)
			})
		}
	}
}

func TestHavenSnapshotDeleteFence(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	fixture := fixtures.SnapshotDeleteFixture(t, 0)
	service := fixture.Service
	if _, err := service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
		TenantID:   "foreign",
		Key:        "foreign-delete",
		SnapshotID: fixture.Capture.Result.SnapshotID,
	}); err == nil {
		t.Fatal("foreign snapshot deletion accepted")
	}
	fixture.Faults.Set(func(point string) error {
		if point == "snapshot_delete_admitted" {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
	if _, err := service.VMProvisioning.AdmitClone(context.Background(), machine.CloneRequest{
		TenantID:   "tenant",
		Key:        "clone-tombstone",
		SnapshotID: fixture.Capture.Result.SnapshotID,
	}); err == nil {
		t.Fatal("new reader crossed deleting fence")
	}
	if _, err := service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
		TenantID:   "tenant",
		Key:        "second-delete",
		SnapshotID: fixture.Capture.Result.SnapshotID,
	}); err == nil {
		t.Fatal("second collector crossed deleting fence")
	}
	fixture.Service = testapp.ReopenCloneService(t, service, fixture.Backend)
	if done := fixtures.AwaitSnapshotDelete(t, fixture.Service, operation.ID); done.Status != "succeeded" {
		t.Fatal(done.Phase)
	}
	fixtures.SnapshotDeleteCost(t, fixture, 0)
}

func TestHavenSnapshotDeleteLeaseFence(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	fixture := fixtures.SnapshotDeleteFixture(t, 1)
	service := fixture.Service
	fixture.Faults.Set(func(point string) error {
		if point == "admission_committed" {
			return errs.ErrInterrupted
		}
		return nil
	})
	leased := fixtures.AdmitCloneFixture(t, service, fixture.Capture.Result.SnapshotID, "leased-clone")
	var leases int
	if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM source_leases WHERE operation_id=?", leased.ID).Scan(&leases); err != nil || leases != 1 {
		t.Fatal("fixture did not retain source lease", leases, err)
	}
	if _, err := service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
		TenantID:   "tenant",
		Key:        "delete-leased",
		SnapshotID: fixture.Capture.Result.SnapshotID,
	}); err == nil {
		t.Fatal("snapshot deleted with unresolved source lease")
	}
	fixtures.SnapshotDeleteCost(t, fixture, 192)
}

func TestHavenSnapshotDeleteFailedBacking(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	fixture := fixtures.SnapshotDeleteFixture(t, 1)
	service := fixture.Service
	fixture.Runtime.FailBoot = true
	failedOperation := fixtures.AdmitCloneFixture(t, service, fixture.Capture.Result.SnapshotID, "failed-retained-clone")
	failed := fixtures.AwaitColdWorker(t, service, failedOperation.ID)
	fixture.Runtime.FailBoot = false
	if failed.Status != "failed" || !failed.State.BackingReady {
		t.Fatal("fixture did not retain failed backing", failed.State)
	}
	deletion := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
	if step := fixtures.AwaitSnapshotDelete(t, service, deletion.ID); step.Status != "succeeded" {
		reason := service.Workers.State(deletion.ID).Err
		t.Fatal(step.Phase, reason)
	}
	release, err := service.VMDeletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
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
	fixtures.SnapshotDeleteCost(t, fixture, 192)
	discard, err := service.VMDeletion.AdmitDiscard(context.Background(), machine.DiscardRequest{
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
	fixtures.SnapshotDeleteCost(t, fixture, 0)
}

func TestHavenSnapshotDeleteUnlinkedReader(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	fixture := fixtures.SnapshotDeleteFixture(t, 0)
	service := fixture.Service
	fixture.Faults.Set(func(point string) error {
		if point == "snapshot_delete_receipt" {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
	step := fixtures.AwaitSnapshotDelete(t, service, operation.ID)
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
	if pending := fixtures.AwaitSnapshotDelete(t, fixture.Service, operation.ID); fixtures.Terminal(pending.Status) {
		t.Fatal("unlinked reader was treated as reclaimed")
	}
	fixtures.SnapshotDeleteCost(t, fixture, 192)
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.Service = testapp.ReopenCloneService(t, fixture.Service, fixture.Backend)
	if done := fixtures.AwaitSnapshotDelete(t, fixture.Service, operation.ID); done.Status != "succeeded" {
		t.Fatal(done.Phase)
	}
	fixtures.SnapshotDeleteCost(t, fixture, 0)
}
