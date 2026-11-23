package sandbox_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/types"
)

func TestHavenForkBatchSourcesAndDelivery(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	source := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "source", map[string]types.EnvVar{
		"SOURCE_ONLY": {
			Value: "not-inherited",
		},
	}, egress.Policy{
		Mode: "none",
	})
	// One owned source transitions running -> paused -> checkpoint. Each case
	// retires its branches through Haven, preserving the source and its slot.
	for _, kind := range []string{
		"running",
		"paused",
		"checkpoint",
	} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			command := operation.Intent{
				Action:    "fork",
				SandboxID: source.Result.SandboxID,
				Count:     3,
			}
			if kind == "paused" {
				if _, err := service.CommandAdmission.Execute(ctx, principal, "pause-source", operation.Intent{
					Action:    "pause",
					SandboxID: source.Result.SandboxID,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "checkpoint" {
				captures, launches := runtime.Count("capture"), runtime.Count("launch")
				checkpoint, err := service.CommandAdmission.Execute(ctx, principal, "checkpoint-source", operation.Intent{
					Action:    "checkpoint",
					SandboxID: source.Result.SandboxID,
				})
				if err != nil {
					t.Fatal(err)
				}
				if runtime.Count("capture") != captures || runtime.Count("launch") != launches {
					t.Fatal("paused checkpoint captured or woke its source")
				}
				command.SandboxID = ""
				command.CheckpointID = checkpoint.Result.CheckpointID
				command.Policy = egress.Policy{
					Mode: "none",
				}
			}
			before := runtime.Count("capture")
			acceptedOperation, err := service.CommandAdmission.Execute(ctx, principal, "fork-batch:"+kind, command)
			if err != nil {
				reason := service.Workers.State(acceptedOperation.ID).Err
				t.Fatal(acceptedOperation, err, reason)
			}
			view, err := service.OperationViews.Get(ctx, principal, acceptedOperation.ID)
			if err != nil || len(view.Result.Sandboxes) != 3 || view.Result.Sandbox != nil || len(view.Result.Tokens) != 3 || len(view.IDs) != 3 {
				t.Fatal(view, err)
			}
			if kind == "running" && runtime.Count("capture") != before+1 {
				t.Fatal("running batch did not share exactly one capture")
			}
			if kind != "running" && runtime.Count("capture") != before {
				t.Fatal("existing snapshot was captured again")
			}
			seen := map[string]bool{}
			for _, sandbox := range view.Result.Sandboxes {
				if sandbox.State != "running" || sandbox.TenantID != "tenant" || seen[sandbox.ID] || sandbox.TTLSeconds != 300 {
					t.Fatal(sandbox)
				}
				seen[sandbox.ID] = true
				ready, err := service.Dataplane.Ready(ctx, principal, sandbox.ID)
				if err != nil || len(ready.Env) != 0 || ready.Policy.Mode != "none" {
					t.Fatal("fork inherited source environment or lost source policy", ready, err)
				}
			}
			replay, err := service.Forks.AdmitFork(ctx, principal, "fork-batch:"+kind, command)
			if err != nil || replay.ID != acceptedOperation.ID {
				t.Fatal(replay, err)
			}
			var slots int
			if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots); err != nil || slots != 4 {
				t.Fatal(slots, err)
			}
			for _, id := range acceptedOperation.Result.SandboxIDs {
				if _, err = service.CommandAdmission.Execute(ctx, principal, "retire:"+id, operation.Intent{
					Action:    "release",
					SandboxID: id,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots); err != nil || slots != 1 {
				t.Fatal("batch retirement changed the source slot", slots, err)
			}
		})
	}
}

func TestHavenForkBatchCompensatesFailedBirth(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	source := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "source", nil, egress.Policy{
		Mode: "none",
	})
	var failed atomic.Bool
	runtime.Before = func(name string, resource loom.Resource) error {
		if name == "boot" && resource.VMID != source.Result.VMID && !failed.Swap(true) {
			return fmt.Errorf("owned branch boot fault")
		}
		return nil
	}
	acceptedOperation, err := service.CommandAdmission.Execute(ctx, principal, "failed-batch", operation.Intent{
		Action:    "fork",
		SandboxID: source.Result.SandboxID,
		Count:     2,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err == nil || acceptedOperation.Status != "failed" {
		reason := service.Workers.State(acceptedOperation.ID).Err
		t.Fatal(acceptedOperation, err, reason)
	}
	for _, id := range acceptedOperation.Result.SandboxIDs {
		sandbox, err := service.Catalog.Get(ctx, principal, id)
		if err != nil || sandbox.State != "terminated" || sandbox.VMID != "" {
			t.Fatal("batch failure retained a delivered branch", sandbox, err)
		}
	}
	var slots, vms int
	if err = service.State.Database().QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT COUNT(*) FROM vms)").Scan(&slots, &vms); err != nil || slots != 1 || vms != 1 {
		t.Fatal("compensation retired unproven or unrelated responsibility", slots, vms, err)
	}
}

func TestHavenForkBatchReopensFrozenTargets(t *testing.T) {
	testutil.ParallelLifecycle(t)
	faults := &testfixture.SnapshotFaults{}
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, faults.Check, testfixture.RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	source := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "source", nil, egress.Policy{
		Mode: "none",
	})
	checkpoint, err := service.CommandAdmission.Execute(context.Background(), principal, "checkpoint", operation.Intent{
		Action:    "checkpoint",
		SandboxID: source.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Reuse the same immutable source across independently keyed admissions and
	// real store closes. Completed targets retire before the next interruption.
	fixtureOwner := t
	for _, boundary := range []string{
		"fork_admitted",
		"fork_capture_before_commit",
		"fork_allocation_before_commit",
		"fork_allocated",
		"fork_completion_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			faults.Set(func(point string) error {
				if point == boundary {
					return errs.ErrInterrupted
				}
				return nil
			})
			command := operation.Intent{
				Action:       "fork",
				CheckpointID: checkpoint.Result.CheckpointID,
				Count:        2,
				Policy: egress.Policy{
					Mode: "none",
				},
			}
			if boundary == "fork_capture_before_commit" {
				command.CheckpointID = ""
				command.SandboxID = source.Result.SandboxID
			}
			acceptedOperation, err := service.Forks.AdmitFork(ctx, principal, "restart-batch:"+boundary, command)
			if acceptedOperation.ID == "" {
				t.Fatal(err)
			}
			if boundary == "fork_admitted" {
				if !errors.Is(err, errs.ErrInterrupted) {
					t.Fatal(err)
				}
			} else {
				if _, err = service.Operations.Wait(ctx, principal, acceptedOperation.ID); !errors.Is(err, errs.ErrInterrupted) {
					t.Fatal(err)
				}
			}
			expected := append([]string(nil), acceptedOperation.Result.SandboxIDs...)
			var originalDeadline time.Time
			if boundary == "fork_completion_before_commit" {
				group, err := sandbox.LoadFork(ctx, service.State.Database(), acceptedOperation.ID)
				if err != nil {
					t.Fatal(err)
				}
				child, err := operation.LoadPublication(ctx, service.State.Database(), group.State.Children[0])
				if err != nil || child.Result.Sandbox == nil {
					t.Fatal(child, err)
				}
				originalDeadline = child.Result.Sandbox.Deadline
				if _, err = service.CommandAdmission.Execute(ctx, principal, "renew-before-group-completion", operation.Intent{
					Action:     "renew",
					SandboxID:  expected[0],
					TTLSeconds: 120,
				}); err != nil {
					t.Fatal(err)
				}
			}
			faults.Set(nil)
			service = testapp.ReopenCloneServiceWithFault(fixtureOwner, service, service.Options.Backend, faults.Check)
			result, err := service.Operations.Wait(ctx, principal, acceptedOperation.ID)
			if err != nil || result.Status != "succeeded" || !slices.Equal(expected, result.Result.SandboxIDs) {
				reason := service.Workers.State(acceptedOperation.ID).Err
				t.Fatal("reopen changed batch targets or lost completion", result, err, reason)
			}
			view, err := service.OperationViews.Get(ctx, principal, acceptedOperation.ID)
			if err != nil || len(view.Result.Sandboxes) != 2 {
				t.Fatal(view, err)
			}
			if !originalDeadline.IsZero() && !view.Result.Sandboxes[0].Deadline.Equal(originalDeadline) {
				t.Fatal("parent recomputed a historical branch from the renewed lease")
			}
			for _, id := range expected {
				if _, err = service.CommandAdmission.Execute(ctx, principal, "retire:"+id, operation.Intent{
					Action:    "release",
					SandboxID: id,
				}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestHavenForkCompletionRejectsSQL(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, runtime, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			source := (testfixture.SandboxClaims{
				App: service,
			}).Claim(t, "source", nil, egress.Policy{
				Mode: "none",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			checkpoint, err := service.CommandAdmission.Execute(ctx, principal, "checkpoint", operation.Intent{
				Action:    "checkpoint",
				SandboxID: source.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			raise := "RAISE(IGNORE)"
			if rejection == "ABORT" {
				raise = "RAISE(ABORT,'fork completion rejected')"
			}
			if _, err = service.State.Database().ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER reject_fork_completion BEFORE UPDATE OF status ON operation_steps WHEN OLD.step_key='fork_group' AND NEW.status='succeeded' BEGIN SELECT %s; END", raise)); err != nil {
				t.Fatal(err)
			}
			command := operation.Intent{
				Action:       "fork",
				CheckpointID: checkpoint.Result.CheckpointID,
				Count:        2,
				Policy: egress.Policy{
					Mode: "none",
				},
			}
			accepted, err := service.Forks.AdmitFork(ctx, principal, "fork-held", command)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Operations.Wait(ctx, principal, accepted.ID); err == nil {
				t.Fatal("rejected group completion reported success")
			}
			group, err := sandbox.LoadFork(ctx, service.State.Database(), accepted.ID)
			if err != nil || testutil.Terminal(group.Status) || group.State.Phase != "allocated" || len(group.State.Children) != 2 {
				t.Fatal("failed completion lost committed branch progress", group, err)
			}
			var leases, publications int
			if err = service.State.Database().QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM snapshot_source_leases WHERE operation_id=? AND step_key='fork_group'),(SELECT COUNT(*) FROM operation_publications WHERE operation_id=?)", accepted.ID, accepted.ID).Scan(&leases, &publications); err != nil || leases != 1 || publications != 0 {
				t.Fatal("failed completion retired parent custody or published a result", leases, publications, err)
			}
			before, err := operation.LoadPublication(ctx, service.State.Database(), group.State.Children[0])
			if err != nil || before.Status != "succeeded" || before.Result.Sandbox == nil {
				t.Fatal("child publication was not retained", before, err)
			}
			launches, clones, captures := runtime.Count("launch"), runtime.Count("clone"), runtime.Count("capture")
			if _, err = service.State.Database().ExecContext(ctx, "DROP TRIGGER reject_fork_completion"); err != nil {
				t.Fatal(err)
			}
			replayed, err := service.CommandAdmission.Execute(ctx, principal, "fork-held", command)
			if err != nil || replayed.ID != accepted.ID || replayed.Status != "succeeded" {
				t.Fatal("repaired group did not complete original admission", replayed, err)
			}
			if runtime.Count("launch") != launches || runtime.Count("clone") != clones || runtime.Count("capture") != captures {
				t.Fatal("group replay repeated resource effects")
			}
			after, err := operation.LoadPublication(ctx, service.State.Database(), group.State.Children[0])
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("group replay changed original branch delivery", after, err)
			}
		})
	}
}
