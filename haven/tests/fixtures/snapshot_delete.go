package fixtures

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

type snapshotDeletionFixture struct {
	Faults  *testfixture.SnapshotFaults
	Service *application.App
	Backend loom.Backend
	Runtime *testfixture.ColdRuntime
	Capture operation.Record
	Clones  []operation.Record
}

func SnapshotDeleteFixture(t *testing.T, clones int) *snapshotDeletionFixture {
	t.Helper()
	faults := &testfixture.SnapshotFaults{}
	var factory func(*testfixture.ColdRuntime) loom.Backend
	if clones > 0 {
		factory = RestorableBackend
	}
	service, runtime := ColdServiceWithBackend(t, faults.Check, factory)
	backend := service.Options.Backend
	capture, _ := CaptureReady(t, service, "delete-source")
	if step := AwaitCapture(t, service, capture.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	fixture := &snapshotDeletionFixture{
		Faults:  faults,
		Service: service,
		Runtime: runtime,
		Backend: backend,
		Capture: capture,
	}
	for index := range clones {
		acceptedOperation := AdmitCloneFixture(t, service, capture.Result.SnapshotID, fmt.Sprintf("delete-clone-%d", index))
		if step := AwaitColdWorker(t, service, acceptedOperation.ID); step.Status != "succeeded" {
			t.Fatal(step.State)
		}
		fixture.Clones = append(fixture.Clones, acceptedOperation)
	}
	original, err := machine.LoadCapture(context.Background(), service.State.Database(), capture.ID)
	if err != nil {
		t.Fatal(err)
	}
	release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release-snapshot-source",
		SandboxID: original.Input.Request.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if step := AwaitColdWorker(t, service, release.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	return fixture
}

func AwaitSnapshotDelete(t *testing.T, service *application.App, id string) machine.SnapshotDeleteStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, seen := worker.Active, worker.Completed
		step, err := machine.LoadSnapshotDelete(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("snapshot deletion worker timed out", step.Phase)
		}
		time.Sleep(time.Millisecond)
	}
}

func SnapshotDeleteCost(t *testing.T, fixture *snapshotDeletionFixture, want int) {
	t.Helper()
	var cost, owner int
	if err := fixture.Service.State.Database().QueryRow("SELECT (SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE kind='snapshot' AND resource_key=?),(SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=?)", fixture.Capture.Result.SnapshotID, fixture.Capture.Result.SnapshotID).Scan(&cost, &owner); err != nil || cost != want || owner != 1 {
		t.Fatal("snapshot debt or permanent owner changed", cost, owner, err)
	}
}

func AdmitSnapshotDeleteFixture(t *testing.T, fixture *snapshotDeletionFixture) operation.Record {
	t.Helper()
	acceptedOperation, err := fixture.Service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
		TenantID:   "tenant",
		Key:        "delete-snapshot",
		SnapshotID: fixture.Capture.Result.SnapshotID,
	})
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation
}
