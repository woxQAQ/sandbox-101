package component

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenDirectOldBindingCannotRevokeResumedInstance(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, runtime, manager := fixtures.DirectFixture(t)
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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	manager.CleanupMu.Lock()
	oldCleanup := manager.Cleanups[before.ID]
	manager.CleanupMu.Unlock()
	if _, err = service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
		Action:    "pause",
		SandboxID: before.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "resume", operation.Command{
		Action:    "resume",
		SandboxID: before.ID,
	}); err != nil {
		t.Fatal(err)
	}
	after, err := service.Dataplane.Ready(ctx, principal, before.ID)
	if err != nil || after.Instance == before.Instance || after.VMID == before.VMID {
		t.Fatal(before, after, err)
	}
	runtime.PolicyMu.Lock()
	count := len(runtime.Events)
	runtime.PolicyMu.Unlock()
	if err = oldCleanup(); err != nil {
		t.Fatal(err)
	}
	runtime.PolicyMu.Lock()
	current := len(runtime.Events)
	runtime.PolicyMu.Unlock()
	if current != count {
		t.Fatal("old binding changed resumed instance networking")
	}
	if _, err = service.Dataplane.Ready(ctx, principal, before.ID); err != nil {
		t.Fatal("old binding revoked resumed direct access", err)
	}
}
