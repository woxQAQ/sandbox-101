package component

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

type snapshotDeletionFixture struct {
	faults  *testfixture.SnapshotFaults
	service *application.App
	backend loom.Backend
	runtime *testfixture.ColdRuntime
	capture operation.Record
	clones  []operation.Record
}

func snapshotDeleteFixture(t *testing.T, clones int) *snapshotDeletionFixture {
	t.Helper()
	faults := &testfixture.SnapshotFaults{}
	var factory func(*testfixture.ColdRuntime) loom.Backend
	if clones > 0 {
		factory = restorableBackend
	}
	service, runtime := coldServiceWithBackend(t, faults.Check, factory)
	backend := service.Options.Backend
	capture, _ := captureReady(t, service, "delete-source")
	if step := awaitCapture(t, service, capture.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	fixture := &snapshotDeletionFixture{
		faults:  faults,
		service: service,
		runtime: runtime,
		backend: backend,
		capture: capture,
	}
	for index := range clones {
		acceptedOperation := admitCloneFixture(t, service, capture.Result.SnapshotID, fmt.Sprintf("delete-clone-%d", index))
		if step := awaitColdWorker(t, service, acceptedOperation.ID); step.Status != "succeeded" {
			t.Fatal(step.State)
		}
		fixture.clones = append(fixture.clones, acceptedOperation)
	}
	original, err := machine.LoadCapture(context.Background(), service.State.Database(), capture.ID)
	if err != nil {
		t.Fatal(err)
	}
	release, err := service.VMDeletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release-snapshot-source",
		SandboxID: original.Input.Request.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if step := awaitColdWorker(t, service, release.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	return fixture
}

func awaitSnapshotDelete(t *testing.T, service *application.App, id string) snapshotDeleteStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, seen := worker.Active, worker.Completed
		step, err := loadSnapshotDelete(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("snapshot deletion worker timed out", step.Phase)
		}
		time.Sleep(time.Millisecond)
	}
}

func snapshotDeleteCost(t *testing.T, fixture *snapshotDeletionFixture, want int) {
	t.Helper()
	var cost, owner int
	if err := fixture.service.State.Database().QueryRow("SELECT (SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE kind='snapshot' AND resource_key=?),(SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=?)", fixture.capture.Result.SnapshotID, fixture.capture.Result.SnapshotID).Scan(&cost, &owner); err != nil || cost != want || owner != 1 {
		t.Fatal("snapshot debt or permanent owner changed", cost, owner, err)
	}
}

func admitSnapshotDeleteFixture(t *testing.T, fixture *snapshotDeletionFixture) operation.Record {
	t.Helper()
	acceptedOperation, err := fixture.service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), SnapshotDeleteRequest{
		TenantID:   "tenant",
		Key:        "delete-snapshot",
		SnapshotID: fixture.capture.Result.SnapshotID,
	})
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation
}

func TestHavenSnapshotDelete(t *testing.T) {
	parallelLifecycle(t)
	for _, clones := range []int{
		0,
		2,
	} {
		t.Run(fmt.Sprint(clones), func(t *testing.T) {
			fixture := snapshotDeleteFixture(t, clones)
			service := fixture.service
			original, err := service.Operations.GetOperation(context.Background(), "tenant", fixture.capture.ID)
			if err != nil {
				t.Fatal(err)
			}
			acceptedOperation := admitSnapshotDeleteFixture(t, fixture)
			done := awaitSnapshotDelete(t, service, acceptedOperation.ID)
			if done.Status != "succeeded" {
				reason := service.Workers.State(acceptedOperation.ID).Err
				t.Fatal("snapshot delete failed", done.Phase, reason)
			}
			if _, err = os.Stat(done.Input.Source.Snapshot.Dir); !os.IsNotExist(err) {
				t.Fatal("snapshot bytes retained", err)
			}
			if _, err = machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), "tenant", fixture.capture.Result.SnapshotID); err == nil {
				t.Fatal("deleted snapshot still in catalog")
			}
			if _, err = service.VMProvisioning.AdmitClone(context.Background(), machine.CloneRequest{
				TenantID:   "tenant",
				Key:        "clone-deleted",
				SnapshotID: fixture.capture.Result.SnapshotID,
			}); err == nil {
				t.Fatal("deleted snapshot acquired new user")
			}
			want := 0
			if clones > 0 {
				want = 192
			}
			snapshotDeleteCost(t, fixture, want)
			fixture.service = testapp.ReopenCloneService(t, service, fixture.backend)
			service = fixture.service
			for index, clone := range fixture.clones {
				source, err := machine.LoadResourceStep(context.Background(), service.State.Database(), clone.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err = loom.VerifyRestoreMaterials(source.State.Materials, source.Input.Resource, source.Input.Restore.Snapshot.Manifest.Files, false); err != nil {
					t.Fatal("deletion damaged retained backing", err)
				}
				release, err := service.VMDeletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
					TenantID:  "tenant",
					Key:       fmt.Sprintf("release-deleted-clone-%d", index),
					SandboxID: clone.Result.SandboxID,
				})
				if err != nil {
					t.Fatal(err)
				}
				if proof := awaitColdWorker(t, service, release.ID); proof.Status != "succeeded" {
					reason := service.Workers.State(release.ID).Err
					t.Fatal("backing release failed", proof.State, reason)
				}
				want = 192
				if index == len(fixture.clones)-1 {
					want = 0
				}
				snapshotDeleteCost(t, fixture, want)
			}
			after, err := service.Operations.GetOperation(context.Background(), "tenant", fixture.capture.ID)
			if err != nil || !reflect.DeepEqual(after, original) {
				t.Fatal("snapshot delete changed original capture result", err)
			}
			replay, err := service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), SnapshotDeleteRequest{
				TenantID:   "tenant",
				Key:        "delete-snapshot",
				SnapshotID: fixture.capture.Result.SnapshotID,
			})
			if err != nil || replay.ID != acceptedOperation.ID || replay.Status != "succeeded" {
				t.Fatal("snapshot delete same-key replay changed", replay, err)
			}
		})
	}
}

func TestHavenSnapshotDeleteConflictDoesNotDispatch(t *testing.T) {
	parallelLifecycle(t)
	fixture := snapshotDeleteFixture(t, 0)
	fixture.faults.Set(func(point string) error {
		if point == "snapshot_delete_admitted" {
			return errs.ErrInterrupted
		}
		return nil
	})
	accepted := admitSnapshotDeleteFixture(t, fixture)
	original, err := loadSnapshotDelete(context.Background(), fixture.service.State.Database(), accepted.ID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.faults.Set(nil)
	deletions := fixture.service.SnapshotDeletion
	_, err = deletions.AdmitSnapshotDelete(context.Background(), SnapshotDeleteRequest{
		TenantID:   "tenant",
		Key:        "delete-snapshot",
		SnapshotID: testfixture.NewID("snap-"),
	})
	worker := fixture.service.Workers.State(accepted.ID)
	if err == nil || worker.Active || worker.Completed {
		t.Fatal("conflicting request dispatched retained deletion", err, worker)
	}
	if _, err = os.Stat(original.Input.Source.Snapshot.Dir); err != nil {
		t.Fatal("conflicting request removed publication", err)
	}
	replay := admitSnapshotDeleteFixture(t, fixture)
	if replay.ID != accepted.ID {
		t.Fatal("matching replay changed operation identity", replay)
	}
	done := awaitSnapshotDelete(t, fixture.service, accepted.ID)
	if done.Status != "succeeded" || done.Hash != original.Hash || !reflect.DeepEqual(done.Input, original.Input) {
		t.Fatal("matching replay failed to preserve frozen deletion", done)
	}
	snapshotDeleteCost(t, fixture, 0)
}

func TestHavenSnapshotDeleteRecovery(t *testing.T) {
	parallelLifecycle(t)
	for _, point := range []string{
		"snapshot_delete_admitted",
		"snapshot_delete_receipt",
		"snapshot_delete_unlinked",
		"snapshot_delete_directory_removed",
		"snapshot_delete_files_removed",
		"snapshot_delete_completion_before_commit",
		"snapshot_delete_completed",
	} {
		t.Run(point, func(t *testing.T) {
			fixture := snapshotDeleteFixture(t, 0)
			fixture.faults.Set(func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation := admitSnapshotDeleteFixture(t, fixture)
			if point != "snapshot_delete_admitted" {
				awaitSnapshotDelete(t, fixture.service, acceptedOperation.ID)
			}
			fixture.service = testapp.ReopenCloneService(t, fixture.service, fixture.backend)
			if step := awaitSnapshotDelete(t, fixture.service, acceptedOperation.ID); step.Status != "succeeded" {
				reason := fixture.service.Workers.State(acceptedOperation.ID).Err
				t.Fatal(step.Phase, reason)
			}
			snapshotDeleteCost(t, fixture, 0)
		})
	}
}
