package component

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenDirectActivationAndExactRetirement(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, runtime, manager := fixtures.DirectFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	direct := egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	}
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, direct)
	fixtures.WaitFixtureWorker(t, service, claim.ID)
	runtime.PolicyMu.Lock()
	events := append([]string(nil), runtime.Events...)
	runtime.PolicyMu.Unlock()
	indexes := map[string]int{}
	for index, event := range events {
		if _, found := indexes[event]; !found {
			indexes[event] = index
		}
	}
	if indexes["off"] >= indexes["policy"] || indexes["policy"] >= indexes["admin"] || indexes["admin"] >= indexes["on"] {
		t.Fatal("direct activation order changed", events)
	}
	manager.CleanupMu.Lock()
	oldCleanup := manager.Cleanups[claim.Result.SandboxID]
	manager.CleanupMu.Unlock()
	if oldCleanup == nil {
		t.Fatal("direct binding lacks frozen cleanup")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.SandboxRequests.Execute(ctx, principal, "none", operation.Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err != nil {
		t.Fatal(err)
	}
	old, err := machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
	if err != nil || old.Status != "succeeded" {
		t.Fatal(old, err)
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "direct", operation.Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy:    direct,
	}); err != nil {
		t.Fatal(err)
	}
	runtime.PolicyMu.Lock()
	before := len(runtime.Events)
	runtime.PolicyMu.Unlock()
	if err = oldCleanup(); err != nil {
		t.Fatal(err)
	}
	runtime.PolicyMu.Lock()
	after := len(runtime.Events)
	runtime.PolicyMu.Unlock()
	if before != after {
		t.Fatal("old binding cleanup mutated successor policy", before, after)
	}
	sandbox, err := service.Dataplane.Ready(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	source, err := service.Catalog.ResourceForData(ctx, sandbox)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := runtime.Inspect(ctx, source.Input.Resource)
	if err != nil || !actual.Network {
		t.Fatal("old cleanup disabled successor", actual, err)
	}
}

func TestHavenDirectRetirementUnknownRetainsFence(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, runtime, _ := fixtures.DirectFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	})
	fixtures.WaitFixtureWorker(t, service, claim.ID)
	runtime.PolicyMu.Lock()
	runtime.RejectClear = true
	runtime.PolicyMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	failed, err := service.SandboxRequests.Execute(ctx, principal, "none", operation.Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err == nil || failed.Status != "failed" {
		t.Fatal(failed, err)
	}
	retirement, err := machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
	if err != nil || retirement.State.Phase != "pending" || retirement.Status != "running" {
		t.Fatal(retirement, err)
	}
	if _, err = service.VMDeletion.AdmitRelease(ctx, machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "bypass",
		SandboxID: claim.Result.SandboxID,
	}); err == nil {
		t.Fatal("resource admission passed unresolved direct retirement")
	}
	runtime.PolicyMu.Lock()
	runtime.RejectClear = false
	runtime.PolicyMu.Unlock()
	if err = service.Maintenance.Reconcile(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		retirement, err = machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
		if err != nil {
			t.Fatal(err)
		}
		if retirement.Status == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("direct retirement did not resume", retirement)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err = service.Dataplane.Ready(ctx, principal, claim.Result.SandboxID); err == nil {
		t.Fatal("retirement removed failed configuration fence")
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "repair", operation.Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err != nil {
		t.Fatal(err)
	}
	original, err := service.Operations.GetOperation(ctx, "tenant", claim.ID)
	if err != nil || !reflect.DeepEqual(original, claim) {
		t.Fatal("independent retirement changed claim result", original, claim, err)
	}
}
