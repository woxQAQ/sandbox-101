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
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenPublishedRawCleanupRequiresTransfer(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, mode := range []string{
		"ABORT,'denied'",
		"IGNORE",
	} {
		t.Run(mode, func(t *testing.T) {
			service, _, capture, request := rawCleanupFixture(t, nil)
			if _, err := service.State.Database().Exec(fmt.Sprintf("CREATE TRIGGER deny_raw_transfer BEFORE UPDATE OF status ON cleanup_intents WHEN OLD.resource_kind='capture' AND NEW.status='transferred' BEGIN SELECT RAISE(%s); END", mode)); err != nil {
				t.Fatal(err)
			}
			accepted, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
			if err == nil || accepted.ID != "" {
				t.Fatal("raw cleanup accepted without responsibility transfer", accepted, err)
			}
			var status string
			if err = service.State.Database().QueryRow("SELECT status FROM cleanup_intents WHERE id=?", "capture:"+capture.OperationID).Scan(&status); err != nil || status != "pending" {
				t.Fatal("rejected handoff changed original responsibility", status, err)
			}
			var operations, owners, storage int
			if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM operations WHERE idem_key='raw'),(SELECT COUNT(*) FROM vms WHERE operation_id IS NOT NULL),(SELECT SUM(storage_mib) FROM quota_resources)").Scan(&operations, &owners, &storage); err != nil || operations != 0 || owners != 0 || storage != 256 {
				t.Fatal("rejected handoff leaked admission or changed custody", operations, owners, storage, err)
			}
			if _, err = os.Stat(capture.State.Boundary.Path); err != nil {
				t.Fatal("rejected handoff removed raw output", err)
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER deny_raw_transfer"); err != nil {
				t.Fatal(err)
			}
			accepted, err = service.CaptureCleanup.Admit(context.Background(), request, nil)
			if err != nil {
				t.Fatal(err)
			}
			if done := awaitRawCleanup(t, service, accepted.ID); done.Status != "succeeded" {
				t.Fatal(done)
			}
		})
	}
}

func TestHavenPublishedRawCleanupRetainsUnknownInstance(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend, capture, request := rawCleanupFixture(t, nil)
	backend.Mu.Lock()
	backend.InspectError = errors.New("private fixture observation unavailable")
	backend.Mu.Unlock()
	accepted, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	awaitRetainedRaw(t, service, accepted.ID)
	if _, err = os.Stat(capture.State.Boundary.Path); err != nil {
		t.Fatal("unknown instance allowed raw removal", err)
	}
	var holders, storage int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM resource_refs WHERE holder_kind='capture'),(SELECT SUM(storage_mib) FROM quota_resources)").Scan(&holders, &storage); err != nil || holders != 2 || storage != 256 {
		t.Fatal("unknown instance lost original responsibility", holders, storage, err)
	}
	backend.Mu.Lock()
	backend.InspectError = nil
	backend.Mu.Unlock()
	replay, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
	if err != nil || replay.ID != accepted.ID {
		t.Fatal("replay changed raw cleanup identity", replay, err)
	}
	fixtures.WaitFixtureWorker(t, service, accepted.ID)
	if done := awaitRawCleanup(t, service, accepted.ID); done.Status != "succeeded" {
		t.Fatal(done)
	}
}

func TestHavenPublishedRawCleanup(t *testing.T) {
	fixtures.ParallelLifecycle(t)
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
	fixtures.ParallelLifecycle(t)
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
			if err = service.Reconcile.ResumePending(); err != nil {
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
	fixtures.ParallelLifecycle(t)
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
	if err = service.Reconcile.ResumePending(); err != nil {
		t.Fatal(err)
	}
	awaitRawCleanup(t, service, operation.ID)
}

func rawCleanupFixture(t *testing.T, fault func(string) error) (*application.App, *testfixture.ColdRuntime, machine.CaptureStep, machine.CaptureCleanupRequest) {
	t.Helper()
	service, backend := fixtures.ColdService(t, fault)
	_, request := fixtures.CaptureReady(t, service, "source")
	operation, err := service.Captures.AdmitCapture(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	capture := fixtures.AwaitCapture(t, service, operation.ID)
	stopped, err := service.Stops.AdmitStop(context.Background(), machine.StopRequest{
		TenantID:         "tenant",
		Key:              "stop",
		SandboxID:        request.SandboxID,
		ExpectedInstance: request.ExpectedInstance,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixtures.AwaitStop(t, service, stopped.ID)
	return service, backend, capture, machine.CaptureCleanupRequest{
		TenantID:           "tenant",
		Key:                "raw",
		CaptureOperationID: capture.OperationID,
		StopOperationID:    stopped.ID,
	}
}

func awaitRawCleanup(t *testing.T, service *application.App, id string) machine.CaptureCleanupStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		step, err := machine.LoadCaptureCleanup(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if fixtures.Terminal(step.Status) {
			return step
		}
		reason := service.Workers.State(id).Err
		if reason != nil {
			t.Fatal(reason)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("raw cleanup did not complete")
	return machine.CaptureCleanupStep{}
}

func awaitRetainedRaw(t *testing.T, service *application.App, id string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		worker := service.Workers.State(id)
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
