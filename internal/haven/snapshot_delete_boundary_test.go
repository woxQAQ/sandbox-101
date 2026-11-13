package haven

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestHavenSnapshotDeleteAtomicRetirement(t *testing.T) {
	parallelLifecycle(t)
	for _, write := range []struct{ event, table, condition string }{
		{"DELETE", "checkpoints", "1"}, {"DELETE", "snapshots", "1"}, {"DELETE", "reservations", "OLD.owner LIKE 'snapshot:%'"}, {"DELETE", "quota_resources", "OLD.kind='snapshot'"}, {"DELETE", "deletion_intents", "1"}, {"DELETE", "resource_refs", "OLD.holder_kind='snapshot'"}, {"UPDATE", "operation_steps", "NEW.action='snapshot_delete' AND NEW.status='succeeded'"}, {"UPDATE", "operations", "NEW.status='succeeded'"},
	} {
		for _, mode := range []string{"ABORT,'fixture snapshot retirement rejection'", "IGNORE"} {
			t.Run(write.table+mode, func(t *testing.T) {
				fixture := snapshotDeleteFixture(t, 0)
				service := fixture.service
				statement := fmt.Sprintf("CREATE TRIGGER deny_snapshot_retirement BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.store.db.Exec(statement); err != nil {
					t.Fatal(err)
				}
				operation := admitSnapshotDeleteFixture(t, fixture)
				step := awaitSnapshotDelete(t, service, operation.ID)
				if terminal(step.Status) || step.Phase != "files_removed" {
					t.Fatal("snapshot retirement denial committed", step.Phase)
				}
				snapshotDeleteCost(t, fixture, 192)
				if _, err := os.Stat(step.Input.Source.Snapshot.Dir); !os.IsNotExist(err) {
					t.Fatal("fixture did not reclaim snapshot bytes", err)
				}
				if _, err := service.store.db.Exec("DROP TRIGGER deny_snapshot_retirement"); err != nil {
					t.Fatal(err)
				}
				fixture.service = reopenCloneService(t, service, fixture.backend)
				if done := awaitSnapshotDelete(t, fixture.service, operation.ID); done.Status != "succeeded" {
					fixture.service.mu.Lock()
					reason := fixture.service.lastErrors[operation.ID]
					fixture.service.mu.Unlock()
					t.Fatal(done.Phase, reason)
				}
				snapshotDeleteCost(t, fixture, 0)
			})
		}
	}
}

func TestHavenSnapshotDeleteBackingRetirement(t *testing.T) {
	parallelLifecycle(t)
	for _, mode := range []string{"ABORT,'fixture backing fee rejection'", "IGNORE"} {
		t.Run(mode, func(t *testing.T) {
			fixture := snapshotDeleteFixture(t, 1)
			service := fixture.service
			operation := admitSnapshotDeleteFixture(t, fixture)
			if step := awaitSnapshotDelete(t, service, operation.ID); step.Status != "succeeded" {
				t.Fatal(step.Phase)
			}
			if _, err := service.store.db.Exec(fmt.Sprintf("CREATE TRIGGER deny_backing_fee BEFORE DELETE ON quota_resources WHEN OLD.kind='snapshot' BEGIN SELECT RAISE(%s); END", mode)); err != nil {
				t.Fatal(err)
			}
			clone := fixture.clones[0]
			release, err := service.AdmitRelease(context.Background(), ReleaseRequest{TenantID: "tenant", Key: "release-last-backing", SandboxID: clone.Result.SandboxID})
			if err != nil {
				t.Fatal(err)
			}
			step := awaitColdWorker(t, service, release.ID)
			if terminal(step.Status) || step.State.Phase != "files_removed" {
				t.Fatal("backing fee denial partially retired VM", step.State)
			}
			snapshotDeleteCost(t, fixture, 192)
			var backing, versions int
			if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM backing WHERE vm_id=?),(SELECT COUNT(*) FROM resource_versions WHERE vm_id=?)", clone.Result.VMID, clone.Result.VMID).Scan(&backing, &versions); err != nil || backing != 1 || versions != 1 {
				t.Fatal("last backing retirement not atomic", backing, versions, err)
			}
			if _, err = service.store.db.Exec("DROP TRIGGER deny_backing_fee"); err != nil {
				t.Fatal(err)
			}
			fixture.service = reopenCloneService(t, service, fixture.backend)
			if done := awaitColdWorker(t, fixture.service, release.ID); done.Status != "succeeded" {
				fixture.service.mu.Lock()
				reason := fixture.service.lastErrors[release.ID]
				fixture.service.mu.Unlock()
				t.Fatal(done.State, reason)
			}
			snapshotDeleteCost(t, fixture, 0)
		})
	}
}

func TestHavenSnapshotDeleteRetainsUncertainObjects(t *testing.T) {
	parallelLifecycle(t)
	for _, kind := range []string{"unknown", "replaced", "missing", "corrupt", "reader", "raw_reappeared"} {
		t.Run(kind, func(t *testing.T) {
			fixture := snapshotDeleteFixture(t, 0)
			service := fixture.service
			snapshot, err := service.GetSnapshot(context.Background(), "tenant", fixture.capture.Result.SnapshotID)
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
				defer func() { _ = file.Close() }()
			case "raw_reappeared":
				capture, err := loadCapture(context.Background(), service.store.db, fixture.capture.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.MkdirAll(capture.State.Boundary.Path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			operation := admitSnapshotDeleteFixture(t, fixture)
			if step := awaitSnapshotDelete(t, service, operation.ID); terminal(step.Status) {
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

func TestHavenSnapshotDeleteAdmissionAtomicity(t *testing.T) {
	parallelLifecycle(t)
	for _, write := range []struct{ event, table, condition string }{
		{"INSERT", "operations", "1"}, {"UPDATE", "snapshots", "NEW.deleting=1"}, {"UPDATE", "resource_refs", "NEW.holder_kind='cleanup'"}, {"UPDATE", "quota_refs", "NEW.holder_kind='cleanup'"},
	} {
		for _, mode := range []string{"ABORT,'fixture snapshot admission rejection'", "IGNORE"} {
			t.Run(write.table+mode, func(t *testing.T) {
				fixture := snapshotDeleteFixture(t, 0)
				service := fixture.service
				statement := fmt.Sprintf("CREATE TRIGGER deny_snapshot_admission BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.store.db.Exec(statement); err != nil {
					t.Fatal(err)
				}
				if _, err := service.AdmitSnapshotDelete(context.Background(), SnapshotDeleteRequest{TenantID: "tenant", Key: "delete-denied", SnapshotID: fixture.capture.Result.SnapshotID}); err == nil {
					t.Fatal("snapshot admission denial ignored")
				}
				var operations, deleting int
				if err := service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM operations WHERE idem_key='delete-denied'),(SELECT deleting FROM snapshots WHERE id=?)", fixture.capture.Result.SnapshotID).Scan(&operations, &deleting); err != nil || operations != 0 || deleting != 0 {
					t.Fatal("snapshot fence partially committed", operations, deleting, err)
				}
				snapshotDeleteCost(t, fixture, 192)
			})
		}
	}
}

func TestHavenSnapshotDeleteFence(t *testing.T) {
	parallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 0)
	service := fixture.service
	if _, err := service.AdmitSnapshotDelete(context.Background(), SnapshotDeleteRequest{TenantID: "foreign", Key: "foreign-delete", SnapshotID: fixture.capture.Result.SnapshotID}); err == nil {
		t.Fatal("foreign snapshot deletion accepted")
	}
	service.opts.Fault = func(point string) error {
		if point == "snapshot_delete_admitted" {
			return ErrInterrupted
		}
		return nil
	}
	operation := admitSnapshotDeleteFixture(t, fixture)
	if _, err := service.AdmitClone(context.Background(), CloneRequest{TenantID: "tenant", Key: "clone-tombstone", SnapshotID: fixture.capture.Result.SnapshotID}); err == nil {
		t.Fatal("new reader crossed deleting fence")
	}
	if _, err := service.AdmitSnapshotDelete(context.Background(), SnapshotDeleteRequest{TenantID: "tenant", Key: "second-delete", SnapshotID: fixture.capture.Result.SnapshotID}); err == nil {
		t.Fatal("second collector crossed deleting fence")
	}
	fixture.service = reopenCloneService(t, service, fixture.backend)
	if done := awaitSnapshotDelete(t, fixture.service, operation.ID); done.Status != "succeeded" {
		t.Fatal(done.Phase)
	}
	snapshotDeleteCost(t, fixture, 0)
}

func TestHavenSnapshotDeleteLeaseFence(t *testing.T) {
	parallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 1)
	service := fixture.service
	service.opts.Fault = func(point string) error {
		if point == "admission_committed" {
			return ErrInterrupted
		}
		return nil
	}
	leased := admitCloneFixture(t, service, fixture.capture.Result.SnapshotID, "leased-clone")
	var leases int
	if err := service.store.db.QueryRow("SELECT COUNT(*) FROM source_leases WHERE operation_id=?", leased.ID).Scan(&leases); err != nil || leases != 1 {
		t.Fatal("fixture did not retain source lease", leases, err)
	}
	if _, err := service.AdmitSnapshotDelete(context.Background(), SnapshotDeleteRequest{TenantID: "tenant", Key: "delete-leased", SnapshotID: fixture.capture.Result.SnapshotID}); err == nil {
		t.Fatal("snapshot deleted with unresolved source lease")
	}
	snapshotDeleteCost(t, fixture, 192)
}

func TestHavenSnapshotDeleteFailedBacking(t *testing.T) {
	parallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 1)
	service := fixture.service
	fixture.runtime.failBoot = true
	failedOperation := admitCloneFixture(t, service, fixture.capture.Result.SnapshotID, "failed-retained-clone")
	failed := awaitColdWorker(t, service, failedOperation.ID)
	fixture.runtime.failBoot = false
	if failed.Status != "failed" || !failed.State.BackingReady {
		t.Fatal("fixture did not retain failed backing", failed.State)
	}
	deletion := admitSnapshotDeleteFixture(t, fixture)
	if step := awaitSnapshotDelete(t, service, deletion.ID); step.Status != "succeeded" {
		service.mu.Lock()
		reason := service.lastErrors[deletion.ID]
		service.mu.Unlock()
		t.Fatal(step.Phase, reason)
	}
	release, err := service.AdmitRelease(context.Background(), ReleaseRequest{TenantID: "tenant", Key: "release-other-backing", SandboxID: fixture.clones[0].Result.SandboxID})
	if err != nil {
		t.Fatal(err)
	}
	if step := awaitColdWorker(t, service, release.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	snapshotDeleteCost(t, fixture, 192)
	discard, err := service.AdmitDiscard(context.Background(), DiscardRequest{TenantID: "tenant", Key: "discard-last-backing", SandboxID: failedOperation.Result.SandboxID, FailedOperationID: failedOperation.ID, ExpectedInstance: failed.Input.Resource.Instance})
	if err != nil {
		t.Fatal(err)
	}
	if step := awaitDiscard(t, service, discard.ID); step.Status != "succeeded" {
		service.mu.Lock()
		reason := service.lastErrors[discard.ID]
		service.mu.Unlock()
		t.Fatal(step.State, reason)
	}
	snapshotDeleteCost(t, fixture, 0)
}

func TestHavenSnapshotDeleteUnlinkedReader(t *testing.T) {
	parallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 0)
	service := fixture.service
	service.opts.Fault = func(point string) error {
		if point == "snapshot_delete_receipt" {
			return ErrInterrupted
		}
		return nil
	}
	operation := admitSnapshotDeleteFixture(t, fixture)
	step := awaitSnapshotDelete(t, service, operation.ID)
	if step.Phase != "deleting" {
		t.Fatal(step.Phase)
	}
	file, err := os.Open(filepath.Join(step.Input.Source.Snapshot.Dir, "ch/memory-ranges"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	// Model loss of all directory entries after the exact receipt committed. The
	// still-open inode remains real material even though pathname lookup is absent.
	if err = os.RemoveAll(step.Input.Source.Snapshot.Dir); err != nil {
		t.Fatal(err)
	}
	fixture.service = reopenCloneService(t, service, fixture.backend)
	if pending := awaitSnapshotDelete(t, fixture.service, operation.ID); terminal(pending.Status) {
		t.Fatal("unlinked reader was treated as reclaimed")
	}
	snapshotDeleteCost(t, fixture, 192)
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.service = reopenCloneService(t, fixture.service, fixture.backend)
	if done := awaitSnapshotDelete(t, fixture.service, operation.ID); done.Status != "succeeded" {
		t.Fatal(done.Phase)
	}
	snapshotDeleteCost(t, fixture, 0)
}
