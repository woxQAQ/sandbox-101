package fixtures

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
)

func AwaitCaptureDiscard(t *testing.T, service *application.App, id string) machine.CaptureDiscardStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, seen := worker.Active, worker.Completed
		step, err := machine.LoadCaptureDiscard(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("capture discard worker timed out", step.Phase)
		}
		time.Sleep(time.Millisecond)
	}
}

func FailedCaptureFixture(t *testing.T, final bool) (*application.App, *testfixture.ColdRuntime, machine.CaptureDiscardRequest, machine.CaptureStep) {
	t.Helper()
	return FailedCaptureFixtureWithFault(t, final, nil)
}

func FailedCaptureFixtureWithFault(t *testing.T, final bool, fault func(string) error) (*application.App, *testfixture.ColdRuntime, machine.CaptureDiscardRequest, machine.CaptureStep) {
	t.Helper()
	point := "captured"
	if final {
		point = "publish_intent"
	}
	service, backend := ColdService(t, func(candidate string) error {
		if candidate == point {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation, request := CaptureReady(t, service, "failed-capture")
	before := AwaitCapture(t, service, operation.ID)
	if final {
		if err := os.Chmod(filepath.Join(before.Input.Snapshot.Dir, "ch/state.json"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(before.Input.Snapshot.Dir, "ch/state.json"), []byte("damaged captured bytes"), 0o444); err != nil {
			t.Fatal(err)
		}
	}
	service = ResumeColdServiceWithFault(t, service, backend, fault)
	failed := AwaitCapture(t, service, operation.ID)
	if failed.Status != "failed" || failed.State.ResumeRequired {
		t.Fatal("fixture capture did not fail with resumed source", failed.State)
	}
	stop, err := service.Stops.AdmitStop(context.Background(), machine.StopRequest{
		TenantID:         request.TenantID,
		Key:              "stop-failed-capture",
		SandboxID:        request.SandboxID,
		ExpectedInstance: request.ExpectedInstance,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stopped := AwaitStop(t, service, stop.ID); stopped.Status != "succeeded" {
		t.Fatal("fixture source stop failed", stopped)
	}
	return service, backend, machine.CaptureDiscardRequest{
		TenantID:          request.TenantID,
		Key:               "discard-capture",
		SandboxID:         request.SandboxID,
		FailedOperationID: operation.ID,
		ExpectedInstance:  request.ExpectedInstance,
		StopOperationID:   stop.ID,
	}, failed
}

func CaptureDiscardLiability(t *testing.T, service *application.App, failed machine.CaptureStep, want int) {
	t.Helper()
	var cost, reserved int
	if err := service.State.Database().QueryRow("SELECT (SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE source_operation_id=?),(SELECT COUNT(*) FROM reservations WHERE owner=?)", failed.OperationID, "snapshot:"+failed.Input.Snapshot.ID).Scan(&cost, &reserved); err != nil || cost != want || (want > 0) != (reserved == 1) {
		t.Fatal("capture liability changed without retirement", cost, reserved, err)
	}
}

func CaptureDiscardPaths(step machine.CaptureDiscardStep) []string {
	source := step.Input.Source
	return []string{
		filepath.Join(source.Input.Source.Input.Resource.Dir, "capture", source.OperationID),
		source.Input.Snapshot.Dir,
	}
}
