package component

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenPublishedRawCleanup(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, capture, request := fixtures.RawCleanupFixture(t, nil)
	operation, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	step := fixtures.AwaitRawCleanup(t, service, operation.ID)
	if step.Status != "succeeded" {
		t.Fatal(step)
	}
	if _, err = os.Lstat(capture.State.Boundary.Path); !os.IsNotExist(err) {
		t.Fatal("raw output retained", err)
	}
	current, err := machine.LoadCapture(context.Background(), service.State.Database(), capture.OperationID)
	if err != nil || !reflect.DeepEqual(current, capture) {
		t.Fatal("original capture changed", err)
	}
	if _, err = os.Stat(filepath.Join(capture.Input.Snapshot.Dir, "manifest.json")); err != nil {
		t.Fatal("publication removed", err)
	}
	var slots, storage, vms, raw int
	if err = service.State.Database().QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources),(SELECT COUNT(*) FROM vms),(SELECT COUNT(*) FROM resource_refs WHERE holder_kind='capture')").Scan(&slots, &storage, &vms, &raw); err != nil || slots != 1 || storage != 256 || vms != 1 || raw != 0 {
		t.Fatal(slots, storage, vms, raw, err)
	}
}

func TestHavenPublishedRawCleanupRecovery(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, boundary := range []string{
		"capture_cleanup_receipt",
		"capture_cleanup_unlinked",
		"capture_cleanup_files_removed",
		"capture_cleanup_completion_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, backend, capture, request := fixtures.RawCleanupFixture(t, func(point string) error {
				if point == boundary {
					return errs.ErrInterrupted
				}
				return nil
			})
			operation, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
			if err != nil {
				t.Fatal(err)
			}
			fixtures.AwaitRetainedRaw(t, service, operation.ID)
			service = testapp.ReopenCloneService(t, service, backend)
			if err = service.Maintenance.Reconcile(); err != nil {
				t.Fatal(err)
			}
			fixtures.AwaitRawCleanup(t, service, operation.ID)
			if _, err = os.Lstat(capture.State.Boundary.Path); !os.IsNotExist(err) {
				t.Fatal(err)
			}
		})
	}
}

func TestHavenPublishedRawCleanupRejectsUnknown(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend, capture, request := fixtures.RawCleanupFixture(t, nil)
	unknown := filepath.Join(capture.State.Boundary.Path, "unowned")
	if err := os.WriteFile(unknown, []byte("unowned private fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	operation, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	fixtures.AwaitRetainedRaw(t, service, operation.ID)
	var status string
	if err = service.State.Database().QueryRow("SELECT status FROM cleanup_intents WHERE id=?", "capture:"+capture.OperationID).Scan(&status); err != nil || status != "transferred" {
		t.Fatal(status, err)
	}
	if _, err = os.Stat(unknown); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	if err = os.Remove(unknown); err != nil {
		t.Fatal(err)
	}
	if err = service.Maintenance.Reconcile(); err != nil {
		t.Fatal(err)
	}
	fixtures.AwaitRawCleanup(t, service, operation.ID)
}
