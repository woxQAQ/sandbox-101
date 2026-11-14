package haven

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func rawCleanupFixture(t *testing.T) (*Service, *coldRuntime, captureStep, CaptureCleanupRequest) {
	t.Helper()
	service, backend := coldService(t, nil)
	_, request := captureReady(t, service, "source")
	operation, err := service.AdmitCapture(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	capture := awaitCapture(t, service, operation.ID)
	stopped, err := service.AdmitStop(context.Background(), StopRequest{
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

func awaitRawCleanup(t *testing.T, service *Service, id string) captureCleanupStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		step, err := loadCaptureCleanup(context.Background(), service.store.db, id)
		if err != nil {
			t.Fatal(err)
		}
		if terminal(step.Status) {
			return step
		}
		service.mu.Lock()
		reason := service.lastErrors[id]
		service.mu.Unlock()
		if reason != nil {
			t.Fatal(reason)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("raw cleanup did not complete")
	return captureCleanupStep{}
}

func awaitRetainedRaw(t *testing.T, service *Service, id string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		service.mu.Lock()
		active := service.active[id]
		reason, seen := service.lastErrors[id]
		service.mu.Unlock()
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
	service, _, capture, request := rawCleanupFixture(t)
	operation, err := service.AdmitCaptureCleanup(context.Background(), request)
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
	current, err := loadCapture(context.Background(), service.store.db, capture.OperationID)
	if err != nil || !reflect.DeepEqual(current, capture) {
		t.Fatal("original capture changed", err)
	}
	if _, err = os.Stat(filepath.Join(capture.Input.Snapshot.Dir, "manifest.json")); err != nil {
		t.Fatal("publication removed", err)
	}
	var slots, storage, vms, raw int
	if err = service.store.db.QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources),(SELECT COUNT(*) FROM vms),(SELECT COUNT(*) FROM resource_refs WHERE holder_kind='capture')").Scan(&slots, &storage, &vms, &raw); err != nil || slots != 1 || storage != 256 || vms != 1 || raw != 0 {
		t.Fatal(slots, storage, vms, raw, err)
	}
}

func TestHavenPublishedRawCleanupRecovery(t *testing.T) {
	for _, boundary := range []string{
		"capture_cleanup_receipt",
		"capture_cleanup_unlinked",
		"capture_cleanup_files_removed",
		"capture_cleanup_completion_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, backend, capture, request := rawCleanupFixture(t)
			service.opts.Fault = func(point string) error {
				if point == boundary {
					return ErrInterrupted
				}
				return nil
			}
			operation, err := service.AdmitCaptureCleanup(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			awaitRetainedRaw(t, service, operation.ID)
			service = reopenCloneService(t, service, backend)
			if err = service.Reconcile(); err != nil {
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
	service, backend, capture, request := rawCleanupFixture(t)
	unknown := filepath.Join(capture.State.Boundary.Path, "unowned")
	if err := os.WriteFile(unknown, []byte("unowned private fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	operation, err := service.AdmitCaptureCleanup(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	awaitRetainedRaw(t, service, operation.ID)
	var status string
	if err = service.store.db.QueryRow("SELECT status FROM cleanup_intents WHERE id=?", "capture:"+capture.OperationID).Scan(&status); err != nil || status != "transferred" {
		t.Fatal(status, err)
	}
	if _, err = os.Stat(unknown); err != nil {
		t.Fatal(err)
	}
	service = reopenCloneService(t, service, backend)
	if err = os.Remove(unknown); err != nil {
		t.Fatal(err)
	}
	if err = service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	awaitRawCleanup(t, service, operation.ID)
}
