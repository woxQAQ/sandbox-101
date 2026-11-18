package fixtures

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
)

func RawCleanupFixture(t *testing.T, fault func(string) error) (*application.App, *testfixture.ColdRuntime, machine.CaptureStep, machine.CaptureCleanupRequest) {
	t.Helper()
	service, backend := ColdService(t, fault)
	_, request := CaptureReady(t, service, "source")
	operation, err := service.Captures.AdmitCapture(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	capture := AwaitCapture(t, service, operation.ID)
	stopped, err := service.VMStops.AdmitStop(context.Background(), machine.StopRequest{
		TenantID:         "tenant",
		Key:              "stop",
		SandboxID:        request.SandboxID,
		ExpectedInstance: request.ExpectedInstance,
	})
	if err != nil {
		t.Fatal(err)
	}
	AwaitStop(t, service, stopped.ID)
	return service, backend, capture, machine.CaptureCleanupRequest{
		TenantID:           "tenant",
		Key:                "raw",
		CaptureOperationID: capture.OperationID,
		StopOperationID:    stopped.ID,
	}
}

func AwaitRawCleanup(t *testing.T, service *application.App, id string) machine.CaptureCleanupStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		step, err := machine.LoadCaptureCleanup(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if Terminal(step.Status) {
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

func AwaitRetainedRaw(t *testing.T, service *application.App, id string) {
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
