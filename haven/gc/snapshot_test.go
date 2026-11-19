package gc_test

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
	"github.com/woxQAQ/haven/types"
)

func TestHavenGCReclaimsInternalSleepSnapshot(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if _, err := service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
		Action:    "pause",
		SandboxID: claim.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	paused, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	protected, err := service.GC.Collect(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "", types.GCOptions{
		DryRun: true,
		Budget: 20 * time.Second,
	})
	if err != nil || len(protected.Candidates) != 0 {
		t.Fatal(protected, err)
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "release", operation.Command{
		Action:    "release",
		SandboxID: paused.ID,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := service.GC.Collect(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "gc", types.GCOptions{
		Budget: 20 * time.Second,
	})
	if err != nil || len(report.Collected) != 1 || report.Collected[0].ID != paused.SnapshotID {
		t.Fatal(report, err)
	}
	var storage int
	if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources").Scan(&storage); err != nil || storage != 0 {
		t.Fatal(storage, err)
	}
}
