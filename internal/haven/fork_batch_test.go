package haven

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
	"github.com/woxQAQ/haven/loom"
)

func TestHavenForkBatchSourcesAndDelivery(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{TenantID: "tenant"}
	source := claimSandbox(t, service, "source", map[string]EnvVar{"SOURCE_ONLY": {Value: "not-inherited"}}, egress.Policy{Mode: "none"})
	// One owned source transitions running -> paused -> checkpoint. Each case
	// retires its branches through Haven, preserving the source and its slot.
	for _, kind := range []string{"running", "paused", "checkpoint"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			command := Command{Action: "fork", SandboxID: source.Result.SandboxID, Count: 3}
			if kind == "paused" {
				if _, err := service.Execute(ctx, principal, "pause-source", Command{Action: "pause", SandboxID: source.Result.SandboxID}); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "checkpoint" {
				captures, launches := runtime.count("capture"), runtime.count("launch")
				checkpoint, err := service.Execute(ctx, principal, "checkpoint-source", Command{Action: "checkpoint", SandboxID: source.Result.SandboxID})
				if err != nil {
					t.Fatal(err)
				}
				if runtime.count("capture") != captures || runtime.count("launch") != launches {
					t.Fatal("paused checkpoint captured or woke its source")
				}
				command.SandboxID = ""
				command.CheckpointID = checkpoint.Result.CheckpointID
				command.Policy = egress.Policy{Mode: "none"}
			}
			before := runtime.count("capture")
			operation, err := service.Execute(ctx, principal, "fork-batch:"+kind, command)
			if err != nil {
				service.mu.Lock()
				reason := service.lastErrors[operation.ID]
				service.mu.Unlock()
				t.Fatal(operation, err, reason)
			}
			view, err := service.PublicOperation(ctx, principal, operation.ID)
			if err != nil || len(view.Result.Sandboxes) != 3 || view.Result.Sandbox != nil || len(view.Result.Tokens) != 3 || len(view.IDs) != 3 {
				t.Fatal(view, err)
			}
			if kind == "running" && runtime.count("capture") != before+1 {
				t.Fatal("running batch did not share exactly one capture")
			}
			if kind != "running" && runtime.count("capture") != before {
				t.Fatal("existing snapshot was captured again")
			}
			seen := map[string]bool{}
			for _, sandbox := range view.Result.Sandboxes {
				if sandbox.State != "running" || sandbox.TenantID != "tenant" || seen[sandbox.ID] || sandbox.TTLSeconds != 300 {
					t.Fatal(sandbox)
				}
				seen[sandbox.ID] = true
				ready, err := service.DataReady(ctx, principal, sandbox.ID)
				if err != nil || len(ready.Env) != 0 || ready.Policy.Mode != "none" {
					t.Fatal("fork inherited source environment or lost source policy", ready, err)
				}
			}
			replay, err := service.AdmitFork(ctx, principal, "fork-batch:"+kind, command)
			if err != nil || replay.ID != operation.ID {
				t.Fatal(replay, err)
			}
			var slots int
			if err = service.store.db.QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots); err != nil || slots != 4 {
				t.Fatal(slots, err)
			}
			for _, id := range operation.Result.SandboxIDs {
				if _, err = service.Execute(ctx, principal, "retire:"+id, Command{Action: "release", SandboxID: id}); err != nil {
					t.Fatal(err)
				}
			}
			if err = service.store.db.QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots); err != nil || slots != 1 {
				t.Fatal("batch retirement changed the source slot", slots, err)
			}
		})
	}
}

func TestHavenForkBatchAdmissionIsAtomic(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{TenantID: "tenant"}
	source := claimSandbox(t, service, "source", nil, egress.Policy{Mode: "none"})
	if _, err := service.PutTenant(context.Background(), Principal{Root: true}, Tenant{ID: "tenant", Token: "quota-test-token", MaxSandboxes: 2, MaxStorageMiB: 4096}); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := service.store.db.QueryRow("SELECT COUNT(*) FROM operations").Scan(&before); err != nil {
		t.Fatal(err)
	}
	operation, err := service.AdmitFork(context.Background(), principal, "too-many", Command{Action: "fork", SandboxID: source.Result.SandboxID, Count: 2})
	if !isEffectCode(err, loom.Exhausted) || operation.ID != "" {
		t.Fatal(operation, err)
	}
	var after, slots int
	if err := service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM operations),(SELECT SUM(sandbox_slots) FROM quota_resources)").Scan(&after, &slots); err != nil || after != before || slots != 1 || runtime.count("capture") != 0 || runtime.count("launch") != 1 {
		t.Fatal("rejected batch left intent or launched effects", after, before, slots, err)
	}
}

func TestHavenForkBatchCompensatesFailedBirth(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{TenantID: "tenant"}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	source := claimSandbox(t, service, "source", nil, egress.Policy{Mode: "none"})
	var failed atomic.Bool
	runtime.before = func(name string, resource loom.Resource) error {
		if name == "boot" && resource.VMID != source.Result.VMID && !failed.Swap(true) {
			return fmt.Errorf("owned branch boot fault")
		}
		return nil
	}
	operation, err := service.Execute(ctx, principal, "failed-batch", Command{Action: "fork", SandboxID: source.Result.SandboxID, Count: 2, Policy: egress.Policy{Mode: "none"}})
	if err == nil || operation.Status != "failed" {
		service.mu.Lock()
		reason := service.lastErrors[operation.ID]
		service.mu.Unlock()
		t.Fatal(operation, err, reason)
	}
	for _, id := range operation.Result.SandboxIDs {
		sandbox, err := service.Get(ctx, principal, id)
		if err != nil || sandbox.State != "terminated" || sandbox.VMID != "" {
			t.Fatal("batch failure retained a delivered branch", sandbox, err)
		}
	}
	var slots, vms int
	if err = service.store.db.QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT COUNT(*) FROM vms)").Scan(&slots, &vms); err != nil || slots != 1 || vms != 1 {
		t.Fatal("compensation retired unproven or unrelated responsibility", slots, vms, err)
	}
}

func TestHavenForkBatchReopensFrozenTargets(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{TenantID: "tenant"}
	source := claimSandbox(t, service, "source", nil, egress.Policy{Mode: "none"})
	checkpoint, err := service.Execute(context.Background(), principal, "checkpoint", Command{Action: "checkpoint", SandboxID: source.Result.SandboxID})
	if err != nil {
		t.Fatal(err)
	}
	// Reuse the same immutable source across independently keyed admissions and
	// real store closes. Completed targets retire before the next interruption.
	fixtureOwner := t
	for _, boundary := range []string{"fork_admitted", "fork_capture_before_commit", "fork_allocation_before_commit", "fork_allocated", "fork_completion_before_commit"} {
		t.Run(boundary, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			service.opts.Fault = func(point string) error {
				if point == boundary {
					return ErrInterrupted
				}
				return nil
			}
			command := Command{Action: "fork", CheckpointID: checkpoint.Result.CheckpointID, Count: 2, Policy: egress.Policy{Mode: "none"}}
			if boundary == "fork_capture_before_commit" {
				command.CheckpointID = ""
				command.SandboxID = source.Result.SandboxID
			}
			operation, err := service.AdmitFork(ctx, principal, "restart-batch:"+boundary, command)
			if operation.ID == "" {
				t.Fatal(err)
			}
			if boundary == "fork_admitted" {
				if !errors.Is(err, ErrInterrupted) {
					t.Fatal(err)
				}
			} else {
				if _, err = service.Wait(ctx, principal, operation.ID); !errors.Is(err, ErrInterrupted) {
					t.Fatal(err)
				}
			}
			expected := append([]string(nil), operation.Result.SandboxIDs...)
			var originalDeadline time.Time
			if boundary == "fork_completion_before_commit" {
				group, err := loadFork(ctx, service.store.db, operation.ID)
				if err != nil {
					t.Fatal(err)
				}
				child, err := loadPublication(ctx, service.store.db, group.State.Children[0])
				if err != nil || child.Result.Sandbox == nil {
					t.Fatal(child, err)
				}
				originalDeadline = child.Result.Sandbox.Deadline
				if _, err = service.Execute(ctx, principal, "renew-before-group-completion", Command{Action: "renew", SandboxID: expected[0], TTLSeconds: 120}); err != nil {
					t.Fatal(err)
				}
			}
			service = reopenCloneService(fixtureOwner, service, service.opts.Backend)
			result, err := service.Wait(ctx, principal, operation.ID)
			if err != nil || result.Status != "succeeded" || !sameForkTargets(expected, result.Result.SandboxIDs) {
				service.mu.Lock()
				reason := service.lastErrors[operation.ID]
				service.mu.Unlock()
				t.Fatal("reopen changed batch targets or lost completion", result, err, reason)
			}
			view, err := service.PublicOperation(ctx, principal, operation.ID)
			if err != nil || len(view.Result.Sandboxes) != 2 {
				t.Fatal(view, err)
			}
			if !originalDeadline.IsZero() && !view.Result.Sandboxes[0].Deadline.Equal(originalDeadline) {
				t.Fatal("parent recomputed a historical branch from the renewed lease")
			}
			for _, id := range expected {
				if _, err = service.Execute(ctx, principal, "retire:"+id, Command{Action: "release", SandboxID: id}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
