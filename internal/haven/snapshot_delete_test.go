package haven

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/loom"
)

type snapshotDeletionFixture struct {
	service *Service
	backend loom.Backend
	runtime *coldRuntime
	capture Operation
	clones  []Operation
}

func snapshotDeleteFixture(t *testing.T, clones int) *snapshotDeletionFixture {
	t.Helper()
	service, runtime := coldService(t, nil)
	var backend loom.Backend = runtime
	if clones > 0 {
		backend = &restorableRuntime{runtime}
		service.opts.Backend = backend
	}
	capture, _ := captureReady(t, service, "delete-source")
	if step := awaitCapture(t, service, capture.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	fixture := &snapshotDeletionFixture{
		service: service,
		runtime: runtime,
		backend: backend,
		capture: capture,
	}
	for index := range clones {
		operation := admitCloneFixture(t, service, capture.Result.SnapshotID, fmt.Sprintf("delete-clone-%d", index))
		if step := awaitColdWorker(t, service, operation.ID); step.Status != "succeeded" {
			t.Fatal(step.State)
		}
		fixture.clones = append(fixture.clones, operation)
	}
	original, err := loadCapture(context.Background(), service.store.db, capture.ID)
	if err != nil {
		t.Fatal(err)
	}
	release, err := service.AdmitRelease(context.Background(), ReleaseRequest{
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

func awaitSnapshotDelete(t *testing.T, service *Service, id string) snapshotDeleteStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		service.mu.Lock()
		active := service.active[id]
		_, seen := service.lastErrors[id]
		service.mu.Unlock()
		step, err := loadSnapshotDelete(context.Background(), service.store.db, id)
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
	if err := fixture.service.store.db.QueryRow("SELECT (SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE kind='snapshot' AND resource_key=?),(SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=?)", fixture.capture.Result.SnapshotID, fixture.capture.Result.SnapshotID).Scan(&cost, &owner); err != nil || cost != want || owner != 1 {
		t.Fatal("snapshot debt or permanent owner changed", cost, owner, err)
	}
}

func admitSnapshotDeleteFixture(t *testing.T, fixture *snapshotDeletionFixture) Operation {
	t.Helper()
	operation, err := fixture.service.AdmitSnapshotDelete(context.Background(), SnapshotDeleteRequest{
		TenantID:   "tenant",
		Key:        "delete-snapshot",
		SnapshotID: fixture.capture.Result.SnapshotID,
	})
	if err != nil && !errors.Is(err, ErrInterrupted) {
		t.Fatal(err)
	}
	return operation
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
			original, err := service.GetOperation(context.Background(), "tenant", fixture.capture.ID)
			if err != nil {
				t.Fatal(err)
			}
			operation := admitSnapshotDeleteFixture(t, fixture)
			done := awaitSnapshotDelete(t, service, operation.ID)
			if done.Status != "succeeded" {
				service.mu.Lock()
				reason := service.lastErrors[operation.ID]
				service.mu.Unlock()
				t.Fatal("snapshot delete failed", done.Phase, reason)
			}
			if _, err = os.Stat(done.Input.Source.Snapshot.Dir); !os.IsNotExist(err) {
				t.Fatal("snapshot bytes retained", err)
			}
			if _, err = service.GetSnapshot(context.Background(), "tenant", fixture.capture.Result.SnapshotID); err == nil {
				t.Fatal("deleted snapshot still in catalog")
			}
			if _, err = service.AdmitClone(context.Background(), CloneRequest{
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
			fixture.service = reopenCloneService(t, service, fixture.backend)
			service = fixture.service
			for index, clone := range fixture.clones {
				source, err := loadStep(context.Background(), service.store.db, clone.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err = loom.VerifyRestoreMaterials(source.State.Materials, source.Input.Resource, source.Input.Restore.Snapshot.Manifest.Files, false); err != nil {
					t.Fatal("deletion damaged retained backing", err)
				}
				release, err := service.AdmitRelease(context.Background(), ReleaseRequest{
					TenantID:  "tenant",
					Key:       fmt.Sprintf("release-deleted-clone-%d", index),
					SandboxID: clone.Result.SandboxID,
				})
				if err != nil {
					t.Fatal(err)
				}
				if proof := awaitColdWorker(t, service, release.ID); proof.Status != "succeeded" {
					service.mu.Lock()
					reason := service.lastErrors[release.ID]
					service.mu.Unlock()
					t.Fatal("backing release failed", proof.State, reason)
				}
				want = 192
				if index == len(fixture.clones)-1 {
					want = 0
				}
				snapshotDeleteCost(t, fixture, want)
			}
			after, err := service.GetOperation(context.Background(), "tenant", fixture.capture.ID)
			if err != nil || !reflect.DeepEqual(after, original) {
				t.Fatal("snapshot delete changed original capture result", err)
			}
			replay, err := service.AdmitSnapshotDelete(context.Background(), SnapshotDeleteRequest{
				TenantID:   "tenant",
				Key:        "delete-snapshot",
				SnapshotID: fixture.capture.Result.SnapshotID,
			})
			if err != nil || replay.ID != operation.ID || replay.Status != "succeeded" {
				t.Fatal("snapshot delete same-key replay changed", replay, err)
			}
		})
	}
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
			fixture.service.opts.Fault = func(candidate string) error {
				if candidate == point {
					return ErrInterrupted
				}
				return nil
			}
			operation := admitSnapshotDeleteFixture(t, fixture)
			if point != "snapshot_delete_admitted" {
				awaitSnapshotDelete(t, fixture.service, operation.ID)
			}
			fixture.service = reopenCloneService(t, fixture.service, fixture.backend)
			if step := awaitSnapshotDelete(t, fixture.service, operation.ID); step.Status != "succeeded" {
				fixture.service.mu.Lock()
				reason := fixture.service.lastErrors[operation.ID]
				fixture.service.mu.Unlock()
				t.Fatal(step.Phase, reason)
			}
			snapshotDeleteCost(t, fixture, 0)
		})
	}
}
