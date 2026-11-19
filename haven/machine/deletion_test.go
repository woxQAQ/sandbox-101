package machine_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenDeletionConflictDoesNotDispatch(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, action := range []string{
		"release",
		"discard",
	} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			faults := &testfixture.SnapshotFaults{}
			faults.Set(func(point string) error {
				if point == action+"_admitted" {
					return errs.ErrInterrupted
				}
				return nil
			})
			service, backend := fixtures.ColdService(t, faults.Check)
			backend.FailBoot = action == "discard"
			claim := fixtures.ColdClaim(t, service, "source")
			source := fixtures.AwaitColdWorker(t, service, claim.ID)
			want := "succeeded"
			if action == "discard" {
				want = "failed"
			}
			if source.Status != want {
				t.Fatal("fixture did not reach original source state", source)
			}
			backend.Mu.Lock()
			backend.FailBoot = false
			backend.Mu.Unlock()
			admit := func(conflict bool) (operation.Record, error) {
				if action == "discard" {
					request := discardRequest(source)
					if conflict {
						request.ExpectedInstance = testfixture.NewID("run-")
					}
					return service.Deletion.AdmitDiscard(ctx, request)
				}
				request := machine.ReleaseRequest{
					TenantID:  "tenant",
					Key:       "release",
					SandboxID: source.Input.SandboxID,
				}
				if conflict {
					request.SandboxID = testfixture.NewID("sandbox-")
				}
				return service.Deletion.AdmitRelease(ctx, request)
			}
			accepted, err := admit(false)
			if !errors.Is(err, errs.ErrInterrupted) || accepted.ID == "" {
				t.Fatal("fixture did not retain committed cleanup admission", accepted, err)
			}
			key := "resource"
			if action == "discard" {
				key = "discard"
			}
			before, err := testfixture.ObserveStep(ctx, service.State.Database(), accepted.ID, key)
			if err != nil {
				t.Fatal(err)
			}
			stops, removals := backend.Count("stop"), backend.Count("remove")
			faults.Set(nil)
			_, err = admit(true)
			worker := service.Workers.State(accepted.ID)
			if !errors.Is(err, errs.ErrConflict) || worker.Active || worker.Completed || backend.Count("stop") != stops || backend.Count("remove") != removals {
				t.Fatal("conflicting cleanup request dispatched retained work", worker, err)
			}
			current, err := machine.LoadOwnership(ctx, service.State.Database(), source.Candidate)
			if err != nil || !current.Owned || current.Owner != accepted.ID {
				t.Fatal("conflicting replay changed cleanup custody", current, err)
			}
			var storage, slots int
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib),SUM(sandbox_slots) FROM quota_resources").Scan(&storage, &slots); err != nil || storage != 64 || slots != 1 {
				t.Fatal("conflicting replay changed retained responsibility", storage, slots, err)
			}
			replay, err := admit(false)
			if err != nil || replay.ID != accepted.ID {
				t.Fatal("matching cleanup replay changed operation", replay, err)
			}
			if action == "discard" {
				fixtures.AwaitDiscard(t, service, accepted.ID)
			} else {
				fixtures.AwaitColdWorker(t, service, accepted.ID)
			}
			done, err := testfixture.ObserveStep(ctx, service.State.Database(), accepted.ID, key)
			if err != nil || done.Status != "succeeded" || done.Hash != before.Hash || !bytes.Equal(done.Input, before.Input) || done.Candidate != before.Candidate || done.ExpectedInstance != before.ExpectedInstance {
				t.Fatal("matching replay did not complete frozen cleanup proof", done, service.Workers.State(accepted.ID).Err, err)
			}
			history, err := machine.LoadResourceStep(ctx, service.State.Database(), source.OperationID)
			if err != nil || !bytes.Equal(testfixture.Encode(history), testfixture.Encode(source)) {
				t.Fatal("cleanup rewrote original generation history", history, err)
			}
		})
	}
}
