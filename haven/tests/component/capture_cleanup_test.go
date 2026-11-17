package component

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
)

func rawCleanupFixture(t *testing.T, fault func(string) error) (*application.App, *testfixture.ColdRuntime, machine.CaptureStep, CaptureCleanupRequest) {
	t.Helper()
	service, backend := coldService(t, fault)
	_, request := captureReady(t, service, "source")
	operation, err := service.Captures.AdmitCapture(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	capture := awaitCapture(t, service, operation.ID)
	stopped, err := service.VMStops.AdmitStop(context.Background(), machine.StopRequest{
		TenantID:         "tenant",
		Key:              "stop",
		SandboxID:        request.SandboxID,
		ExpectedInstance: request.ExpectedInstance,
	})
	if err != nil {
		t.Fatal(err)
	}
	awaitStop(t, service, stopped.ID)
	return service, backend, capture, CaptureCleanupRequest{
		TenantID:           "tenant",
		Key:                "raw",
		CaptureOperationID: capture.OperationID,
		StopOperationID:    stopped.ID,
	}
}

func awaitRawCleanup(t *testing.T, service *application.App, id string) captureCleanupStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		step, err := loadCaptureCleanup(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if terminal(step.Status) {
			return step
		}
		reason := service.Workers.Snapshot(id).Err
		if reason != nil {
			t.Fatal(reason)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("raw cleanup did not complete")
	return captureCleanupStep{}
}

func awaitRetainedRaw(t *testing.T, service *application.App, id string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		worker := service.Workers.Snapshot(id)
		active, reason, seen := worker.Active, worker.Err, worker.Completed
		if !active && seen {
			if reason == nil {
				t.Fatal("raw cleanup unexpectedly completed")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("raw cleanup worker did not retain interruption")
}

func TestHavenPublishedRawCleanup(t *testing.T) {
	parallelLifecycle(t)
	service, _, capture, request := rawCleanupFixture(t, nil)
	operation, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	step := awaitRawCleanup(t, service, operation.ID)
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
	parallelLifecycle(t)
	for _, boundary := range []string{
		"capture_cleanup_receipt",
		"capture_cleanup_unlinked",
		"capture_cleanup_files_removed",
		"capture_cleanup_completion_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, backend, capture, request := rawCleanupFixture(t, func(point string) error {
				if point == boundary {
					return errs.ErrInterrupted
				}
				return nil
			})
			operation, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
			if err != nil {
				t.Fatal(err)
			}
			awaitRetainedRaw(t, service, operation.ID)
			service = testapp.ReopenCloneService(t, service, backend)
			if err = service.Maintenance.Reconcile(); err != nil {
				t.Fatal(err)
			}
			awaitRawCleanup(t, service, operation.ID)
			if _, err = os.Lstat(capture.State.Boundary.Path); !os.IsNotExist(err) {
				t.Fatal(err)
			}
		})
	}
}

func TestHavenPublishedRawCleanupRejectsUnknown(t *testing.T) {
	parallelLifecycle(t)
	service, backend, capture, request := rawCleanupFixture(t, nil)
	unknown := filepath.Join(capture.State.Boundary.Path, "unowned")
	if err := os.WriteFile(unknown, []byte("unowned private fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	operation, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	awaitRetainedRaw(t, service, operation.ID)
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
	awaitRawCleanup(t, service, operation.ID)
}
