package haven

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/egress"
)

func TestHavenDirectOldBindingCannotRevokeResumedInstance(t *testing.T) {
	service, runtime, manager := directFixture(t)
	principal := Principal{
		TenantID: "tenant",
	}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	})
	waitFixtureWorker(t, service, claim.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before, err := service.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	manager.cleanupMu.Lock()
	oldCleanup := manager.cleanups[before.ID]
	manager.cleanupMu.Unlock()
	if _, err = service.Execute(ctx, principal, "pause", Command{
		Action:    "pause",
		SandboxID: before.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Execute(ctx, principal, "resume", Command{
		Action:    "resume",
		SandboxID: before.ID,
	}); err != nil {
		t.Fatal(err)
	}
	after, err := service.DataReady(ctx, principal, before.ID)
	if err != nil || after.Instance == before.Instance || after.VMID == before.VMID {
		t.Fatal(before, after, err)
	}
	runtime.policyMu.Lock()
	count := len(runtime.events)
	runtime.policyMu.Unlock()
	if err = oldCleanup(); err != nil {
		t.Fatal(err)
	}
	runtime.policyMu.Lock()
	current := len(runtime.events)
	runtime.policyMu.Unlock()
	if current != count {
		t.Fatal("old binding changed resumed instance networking")
	}
	if _, err = service.DataReady(ctx, principal, before.ID); err != nil {
		t.Fatal("old binding revoked resumed direct access", err)
	}
}
