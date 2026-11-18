package fixtures

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func CaptureReady(t *testing.T, service *application.App, key string) (operation.Record, machine.CaptureRequest) {
	t.Helper()
	claim := ColdClaim(t, service, "source-"+key)
	source := AwaitColdWorker(t, service, claim.ID)
	if source.Status != "succeeded" {
		t.Fatal(source.State)
	}
	request := machine.CaptureRequest{
		TenantID:         "tenant",
		Key:              key,
		SandboxID:        claim.Result.SandboxID,
		ExpectedInstance: source.State.Witness,
	}
	acceptedOperation, err := service.Captures.AdmitCapture(context.Background(), request)
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation, request
}

func AwaitCapture(t *testing.T, service *application.App, id string) machine.CaptureStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, last, seen := worker.Active, worker.Err, worker.Completed
		step, err := machine.LoadCapture(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture worker did not stop: %+v %v", step.State, last)
		}
		time.Sleep(time.Millisecond)
	}
}

func CapturePublished(t *testing.T, service *application.App, step machine.CaptureStep, want bool) {
	t.Helper()
	snapshot, err := machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), step.Input.Request.TenantID, step.Input.Snapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	if want != (snapshot.Manifest != nil) || want != (step.Status == "succeeded") {
		t.Fatalf("snapshot and step publication disagree: status=%s manifest=%v phase=%s", step.Status, snapshot.Manifest != nil, step.State.Phase)
	}
	var storage, pending, charged int64
	if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(storage_mib),0),COALESCE(SUM(kind='pending_storage'),0),COALESCE(SUM(kind='snapshot'),0) FROM quota_resources WHERE source_operation_id=?", step.OperationID).Scan(&storage, &pending, &charged); err != nil {
		t.Fatal(err)
	}
	if storage != 192 || want && (charged != 1 || pending != 0) || !want && (charged != 0 || pending != 1) {
		t.Fatal("capture lost or partially transferred its liability", storage, pending, charged)
	}
	if want {
		actual, err := loom.HashTree(snapshot.Dir)
		if err != nil || !reflect.DeepEqual(actual, snapshot.Manifest.Files) {
			t.Fatal("published bytes differ from fixed proof", err)
		}
	}
}
