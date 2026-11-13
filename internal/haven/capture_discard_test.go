package haven

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func awaitCaptureDiscard(t *testing.T, service *Service, id string) captureDiscardStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		service.mu.Lock()
		active := service.active[id]
		_, seen := service.lastErrors[id]
		service.mu.Unlock()
		step, err := loadCaptureDiscard(context.Background(), service.store.db, id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("capture discard worker timed out", step.Phase)
		}
		time.Sleep(time.Millisecond)
	}
}

func failedCaptureFixture(t *testing.T, final bool) (*Service, *coldRuntime, CaptureDiscardRequest, captureStep) {
	t.Helper()
	point := "captured"
	if final {
		point = "publish_intent"
	}
	service, backend := coldService(t, func(candidate string) error {
		if candidate == point {
			return ErrInterrupted
		}
		return nil
	})
	operation, request := captureReady(t, service, "failed-capture")
	before := awaitCapture(t, service, operation.ID)
	if final {
		if err := os.Chmod(filepath.Join(before.Input.Snapshot.Dir, "ch/state.json"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(before.Input.Snapshot.Dir, "ch/state.json"), []byte("damaged captured bytes"), 0o444); err != nil {
			t.Fatal(err)
		}
	}
	service = resumeColdService(t, service, backend)
	failed := awaitCapture(t, service, operation.ID)
	if failed.Status != "failed" || failed.State.ResumeRequired {
		t.Fatal("fixture capture did not fail with resumed source", failed.State)
	}
	stop, err := service.AdmitStop(context.Background(), StopRequest{TenantID: request.TenantID, Key: "stop-failed-capture", SandboxID: request.SandboxID, ExpectedInstance: request.ExpectedInstance})
	if err != nil {
		t.Fatal(err)
	}
	if stopped := awaitStop(t, service, stop.ID); stopped.Status != "succeeded" {
		t.Fatal("fixture source stop failed", stopped)
	}
	return service, backend, CaptureDiscardRequest{TenantID: request.TenantID, Key: "discard-capture", SandboxID: request.SandboxID, FailedOperationID: operation.ID, ExpectedInstance: request.ExpectedInstance, StopOperationID: stop.ID}, failed
}

func captureDiscardLiability(t *testing.T, service *Service, failed captureStep, want int) {
	t.Helper()
	var cost, reserved int
	if err := service.store.db.QueryRow("SELECT (SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE source_operation_id=?),(SELECT COUNT(*) FROM reservations WHERE owner=?)", failed.OperationID, "snapshot:"+failed.Input.Snapshot.ID).Scan(&cost, &reserved); err != nil || cost != want || (want > 0) != (reserved == 1) {
		t.Fatal("capture liability changed without retirement", cost, reserved, err)
	}
}

func TestHavenCaptureDiscard(t *testing.T) {
	parallelLifecycle(t)
	for _, final := range []bool{false, true} {
		t.Run(fmt.Sprint(final), func(t *testing.T) {
			service, backend, request, failed := failedCaptureFixture(t, final)
			original, err := service.GetOperation(context.Background(), request.TenantID, failed.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			operation, err := service.AdmitCaptureDiscard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			done := awaitCaptureDiscard(t, service, operation.ID)
			if done.Status != "succeeded" {
				service.mu.Lock()
				reason := service.lastErrors[operation.ID]
				service.mu.Unlock()
				t.Fatal("capture discard failed", done, reason)
			}
			captureDiscardLiability(t, service, failed, 0)
			for _, path := range captureDiscardPaths(done) {
				if _, err = os.Lstat(path); !os.IsNotExist(err) {
					t.Fatal("capture directory retained", path, err)
				}
			}
			if err = loom.VerifyWritableDisk(failed.Input.Source.Input.Resource, *failed.Input.Source.State.DiskIdentity); err != nil {
				t.Fatal("cleanup removed source disk", err)
			}
			current, err := loadCapture(context.Background(), service.store.db, failed.OperationID)
			if err != nil || !reflect.DeepEqual(current, failed) {
				t.Fatal("cleanup rewrote failed capture", err)
			}
			result, err := service.GetOperation(context.Background(), request.TenantID, failed.OperationID)
			if err != nil || !reflect.DeepEqual(result, original) {
				t.Fatal("cleanup changed original public result", err)
			}
			var owner, slot, disk int
			if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=?),(SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'),(SELECT SUM(storage_mib) FROM quota_resources WHERE kind='vm_disk' AND tenant_id='tenant')", failed.Input.Snapshot.ID).Scan(&owner, &slot, &disk); err != nil || owner != 1 || slot != 1 || disk != 64 {
				t.Fatal("cleanup changed permanent owner or source debt", owner, slot, disk, err)
			}
			replay, err := service.AdmitCaptureDiscard(context.Background(), request)
			if err != nil || replay.ID != operation.ID || replay.Status != "succeeded" {
				t.Fatal("discard same-key replay changed", replay, err)
			}
			release, err := service.AdmitRelease(context.Background(), ReleaseRequest{TenantID: request.TenantID, Key: "release-source", SandboxID: request.SandboxID})
			if err != nil {
				t.Fatal("cleanup did not unblock source release", err)
			}
			if step := awaitColdWorker(t, service, release.ID); step.Status != "succeeded" {
				t.Fatal(step.State)
			}
			if backend.count("launch") != 1 {
				t.Fatal("cleanup relaunched source")
			}
		})
	}
}

func TestHavenCaptureDiscardRecovery(t *testing.T) {
	parallelLifecycle(t)
	for _, point := range []string{"capture_discard_admitted", "capture_discard_receipts", "capture_discard_unlinked", "capture_discard_directory_removed", "capture_discard_files_removed", "capture_discard_completion_before_commit", "capture_discard_completed"} {
		t.Run(point, func(t *testing.T) {
			service, backend, request, failed := failedCaptureFixture(t, true)
			service.opts.Fault = func(candidate string) error {
				if candidate == point {
					return ErrInterrupted
				}
				return nil
			}
			operation, err := service.AdmitCaptureDiscard(context.Background(), request)
			if err != nil && !errors.Is(err, ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "capture_discard_admitted" {
				awaitCaptureDiscard(t, service, operation.ID)
			}
			service = resumeColdService(t, service, backend)
			step := awaitCaptureDiscard(t, service, operation.ID)
			if step.Status != "succeeded" {
				service.mu.Lock()
				reason := service.lastErrors[operation.ID]
				service.mu.Unlock()
				t.Fatal(step, reason)
			}
			captureDiscardLiability(t, service, failed, 0)
			original, err := loadCapture(context.Background(), service.store.db, failed.OperationID)
			if err != nil || !reflect.DeepEqual(original, failed) {
				t.Fatal("recovery changed original failure", err)
			}
			if backend.count("launch") != 1 {
				t.Fatal("recovery launched source")
			}
		})
	}
}

func TestHavenCaptureDiscardRetirementAtomicity(t *testing.T) {
	parallelLifecycle(t)
	for _, write := range []struct{ event, table, condition string }{
		{"DELETE", "quota_resources", "OLD.kind='pending_storage'"},
		{"DELETE", "snapshots", "1"},
		{"DELETE", "reservations", "OLD.owner LIKE 'snapshot:%'"},
		{"UPDATE", "operation_steps", "NEW.action='capture_discard' AND NEW.status='succeeded'"},
		{"UPDATE", "operations", "NEW.status='succeeded'"},
	} {
		for _, mode := range []string{"ABORT,'fixture capture retirement rejection'", "IGNORE"} {
			t.Run(write.table+mode, func(t *testing.T) {
				service, backend, request, failed := failedCaptureFixture(t, true)
				statement := fmt.Sprintf("CREATE TRIGGER deny_capture_retirement BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.store.db.Exec(statement); err != nil {
					t.Fatal(err)
				}
				operation, err := service.AdmitCaptureDiscard(context.Background(), request)
				if err != nil {
					t.Fatal(err)
				}
				pending := awaitCaptureDiscard(t, service, operation.ID)
				if terminal(pending.Status) || pending.Phase != "files_removed" {
					t.Fatal("retirement denial committed", pending)
				}
				captureDiscardLiability(t, service, failed, 192)
				for _, path := range captureDiscardPaths(pending) {
					if _, err = os.Lstat(path); !os.IsNotExist(err) {
						t.Fatal("fixture did not reclaim physical material", err)
					}
				}
				if _, err = service.store.db.Exec("DROP TRIGGER deny_capture_retirement"); err != nil {
					t.Fatal(err)
				}
				service = resumeColdService(t, service, backend)
				if step := awaitCaptureDiscard(t, service, operation.ID); step.Status != "succeeded" {
					t.Fatal(step)
				}
				captureDiscardLiability(t, service, failed, 0)
			})
		}
	}
}
