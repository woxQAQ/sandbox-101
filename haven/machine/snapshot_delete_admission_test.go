package machine_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
)

func TestHavenSnapshotDeleteAdmissionAtomicity(t *testing.T) {
	testutil.ParallelLifecycle(t)
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
				fixture := snapshotDeleteFixture(t, 0)
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
				snapshotDeleteCost(t, fixture, 192)
			})
		}
	}
}

func TestHavenSnapshotDeleteFence(t *testing.T) {
	testutil.ParallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 0)
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
	operation := admitSnapshotDeleteFixture(t, fixture)
	if _, err := service.Provisioning.AdmitClone(context.Background(), machine.CloneRequest{
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
	if done := awaitSnapshotDelete(t, fixture.Service, operation.ID); done.Status != "succeeded" {
		t.Fatal(done.Phase)
	}
	snapshotDeleteCost(t, fixture, 0)
}

func TestHavenSnapshotDeleteLeaseFence(t *testing.T) {
	testutil.ParallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 1)
	service := fixture.Service
	fixture.Faults.Set(func(point string) error {
		if point == "admission_committed" {
			return errs.ErrInterrupted
		}
		return nil
	})
	leased := testfixture.AdmitCloneFixture(t, service, fixture.Capture.Result.SnapshotID, "leased-clone")
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
	snapshotDeleteCost(t, fixture, 192)
}

func TestHavenSnapshotDeleteConflictDoesNotDispatch(t *testing.T) {
	testutil.ParallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 0)
	fixture.Faults.Set(func(point string) error {
		if point == "snapshot_delete_admitted" {
			return errs.ErrInterrupted
		}
		return nil
	})
	accepted := admitSnapshotDeleteFixture(t, fixture)
	original, err := machine.LoadSnapshotDelete(context.Background(), fixture.Service.State.Database(), accepted.ID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Faults.Set(nil)
	deletions := fixture.Service.SnapshotDeletion
	_, err = deletions.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
		TenantID:   "tenant",
		Key:        "delete-snapshot",
		SnapshotID: testfixture.NewID("snap-"),
	})
	worker := fixture.Service.Workers.State(accepted.ID)
	if err == nil || worker.Active || worker.Completed {
		t.Fatal("conflicting request dispatched retained deletion", err, worker)
	}
	if _, err = os.Stat(original.Input.Source.Snapshot.Dir); err != nil {
		t.Fatal("conflicting request removed publication", err)
	}
	replay := admitSnapshotDeleteFixture(t, fixture)
	if replay.ID != accepted.ID {
		t.Fatal("matching replay changed operation identity", replay)
	}
	done := awaitSnapshotDelete(t, fixture.Service, accepted.ID)
	if done.Status != "succeeded" || done.Hash != original.Hash || !reflect.DeepEqual(done.Input, original.Input) {
		t.Fatal("matching replay failed to preserve frozen deletion", done)
	}
	snapshotDeleteCost(t, fixture, 0)
}
