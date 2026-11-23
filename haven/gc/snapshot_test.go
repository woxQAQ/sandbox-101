package gc_test

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestHavenGCReclaimsInternalSleepSnapshot(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if _, err := service.CommandAdmission.Execute(ctx, principal, "pause", operation.Intent{
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
	if _, err = service.CommandAdmission.Execute(ctx, principal, "release", operation.Intent{
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
	if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE deleted_at IS NULL").Scan(&storage); err != nil || storage != 0 {
		t.Fatal(storage, err)
	}
}
