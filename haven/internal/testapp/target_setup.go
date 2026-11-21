package testapp

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
)

func TargetSetup(t *testing.T) (*application.App, *testfixture.GenerationRuntime, machine.RestoreRequest, machine.ResourceStep) {
	t.Helper()
	return TargetSetupWithFault(t, nil)
}

func TargetSetupWithFault(t *testing.T, fault func(string) error) (*application.App, *testfixture.GenerationRuntime, machine.RestoreRequest, machine.ResourceStep) {
	t.Helper()
	service, _ := ColdServiceWithBackend(t, fault, testfixture.GenerationBackend)
	backend := service.Options.Backend.(*testfixture.GenerationRuntime)
	capture, _ := testfixture.CaptureReady(t, service, "target-source")
	if proof := testutil.AwaitCapture(t, service, capture.ID); proof.Status != "succeeded" {
		t.Fatal(proof.State)
	}
	target := testfixture.ColdClaim(t, service, "target")
	ready := testutil.AwaitColdWorker(t, service, target.ID)
	if ready.Status != "succeeded" {
		t.Fatal(ready.State)
	}
	stop, err := service.Stops.AdmitStop(context.Background(), machine.StopRequest{
		TenantID:         "tenant",
		Key:              "target-stop",
		SandboxID:        target.Result.SandboxID,
		ExpectedInstance: ready.State.Witness,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(stop.ID)
		active, seen := worker.Active, worker.Completed
		if !active && seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stop did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	result, err := service.Operations.GetOperation(context.Background(), "tenant", stop.ID)
	if err != nil || result.Status != "succeeded" {
		reason := service.Workers.State(stop.ID).Err
		t.Fatal("stop did not succeed", result, reason, err)
	}
	request := machine.RestoreRequest{
		TenantID:         "tenant",
		Key:              "restore",
		SandboxID:        target.Result.SandboxID,
		SnapshotID:       capture.Result.SnapshotID,
		ExpectedInstance: ready.State.Witness,
		StopOperationID:  stop.ID,
	}
	return service, backend, request, ready
}
