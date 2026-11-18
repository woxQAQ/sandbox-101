package component

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenSnapshotDelete(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, clones := range []int{
		0,
		2,
	} {
		t.Run(fmt.Sprint(clones), func(t *testing.T) {
			fixture := fixtures.SnapshotDeleteFixture(t, clones)
			service := fixture.Service
			original, err := service.Operations.GetOperation(context.Background(), "tenant", fixture.Capture.ID)
			if err != nil {
				t.Fatal(err)
			}
			acceptedOperation := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
			done := fixtures.AwaitSnapshotDelete(t, service, acceptedOperation.ID)
			if done.Status != "succeeded" {
				reason := service.Workers.State(acceptedOperation.ID).Err
				t.Fatal("snapshot delete failed", done.Phase, reason)
			}
			if _, err = os.Stat(done.Input.Source.Snapshot.Dir); !os.IsNotExist(err) {
				t.Fatal("snapshot bytes retained", err)
			}
			if _, err = machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), "tenant", fixture.Capture.Result.SnapshotID); err == nil {
				t.Fatal("deleted snapshot still in catalog")
			}
			if _, err = service.Provisioning.AdmitClone(context.Background(), machine.CloneRequest{
				TenantID:   "tenant",
				Key:        "clone-deleted",
				SnapshotID: fixture.Capture.Result.SnapshotID,
			}); err == nil {
				t.Fatal("deleted snapshot acquired new user")
			}
			want := 0
			if clones > 0 {
				want = 192
			}
			fixtures.SnapshotDeleteCost(t, fixture, want)
			fixture.Service = testapp.ReopenCloneService(t, service, fixture.Backend)
			service = fixture.Service
			for index, clone := range fixture.Clones {
				source, err := machine.LoadResourceStep(context.Background(), service.State.Database(), clone.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err = loom.VerifyRestoreMaterials(source.State.Materials, source.Input.Resource, source.Input.Restore.Snapshot.Manifest.Files, false); err != nil {
					t.Fatal("deletion damaged retained backing", err)
				}
				release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
					TenantID:  "tenant",
					Key:       fmt.Sprintf("release-deleted-clone-%d", index),
					SandboxID: clone.Result.SandboxID,
				})
				if err != nil {
					t.Fatal(err)
				}
				if proof := fixtures.AwaitColdWorker(t, service, release.ID); proof.Status != "succeeded" {
					reason := service.Workers.State(release.ID).Err
					t.Fatal("backing release failed", proof.State, reason)
				}
				want = 192
				if index == len(fixture.Clones)-1 {
					want = 0
				}
				fixtures.SnapshotDeleteCost(t, fixture, want)
			}
			after, err := service.Operations.GetOperation(context.Background(), "tenant", fixture.Capture.ID)
			if err != nil || !reflect.DeepEqual(after, original) {
				t.Fatal("snapshot delete changed original capture result", err)
			}
			replay, err := service.SnapshotDeletion.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
				TenantID:   "tenant",
				Key:        "delete-snapshot",
				SnapshotID: fixture.Capture.Result.SnapshotID,
			})
			if err != nil || replay.ID != acceptedOperation.ID || replay.Status != "succeeded" {
				t.Fatal("snapshot delete same-key replay changed", replay, err)
			}
		})
	}
}

func TestHavenSnapshotDeleteConflictDoesNotDispatch(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	fixture := fixtures.SnapshotDeleteFixture(t, 0)
	fixture.Faults.Set(func(point string) error {
		if point == "snapshot_delete_admitted" {
			return errs.ErrInterrupted
		}
		return nil
	})
	accepted := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
	original, err := machine.LoadSnapshotDelete(context.Background(), fixture.Service.State.Database(), accepted.ID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Faults.Set(nil)
	deletions := fixture.Service.SnapshotDeletion
	_, err = deletions.AdmitSnapshotDelete(context.Background(), machine.SnapshotDeleteRequest{
		TenantID:   "tenant",
		Key:        "delete-snapshot",
		SnapshotID: testfixture.NewID("snap-"),
	})
	worker := fixture.Service.Workers.State(accepted.ID)
	if err == nil || worker.Active || worker.Completed {
		t.Fatal("conflicting request dispatched retained deletion", err, worker)
	}
	if _, err = os.Stat(original.Input.Source.Snapshot.Dir); err != nil {
		t.Fatal("conflicting request removed publication", err)
	}
	replay := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
	if replay.ID != accepted.ID {
		t.Fatal("matching replay changed operation identity", replay)
	}
	done := fixtures.AwaitSnapshotDelete(t, fixture.Service, accepted.ID)
	if done.Status != "succeeded" || done.Hash != original.Hash || !reflect.DeepEqual(done.Input, original.Input) {
		t.Fatal("matching replay failed to preserve frozen deletion", done)
	}
	fixtures.SnapshotDeleteCost(t, fixture, 0)
}

func TestHavenSnapshotDeleteRecovery(t *testing.T) {
	fixtures.ParallelLifecycle(t)
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
			fixture := fixtures.SnapshotDeleteFixture(t, 0)
			fixture.Faults.Set(func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation := fixtures.AdmitSnapshotDeleteFixture(t, fixture)
			if point != "snapshot_delete_admitted" {
				fixtures.AwaitSnapshotDelete(t, fixture.Service, acceptedOperation.ID)
			}
			fixture.Service = testapp.ReopenCloneService(t, fixture.Service, fixture.Backend)
			if step := fixtures.AwaitSnapshotDelete(t, fixture.Service, acceptedOperation.ID); step.Status != "succeeded" {
				reason := fixture.Service.Workers.State(acceptedOperation.ID).Err
				t.Fatal(step.Phase, reason)
			}
			fixtures.SnapshotDeleteCost(t, fixture, 0)
		})
	}
}
