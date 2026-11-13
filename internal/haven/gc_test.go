package haven

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
)

func TestHavenGCDryRunAndSupplyCollection(t *testing.T) {
	service, runtime, _ := goldenSupplyFixture(t, 1)
	runtime.mu.Lock()
	runtime.failBoot = true
	runtime.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	failed, err := service.Execute(ctx, Principal{TenantID: "tenant"}, "refill", Command{Action: "refill", TemplateID: "golden"})
	if err == nil || failed.Status != "failed" {
		t.Fatal(failed, err)
	}
	unknown := filepath.Join(service.store.directory, "vms", "unknown-private-entry")
	if err = os.Mkdir(unknown, 0o700); err != nil {
		t.Fatal(err)
	}
	var before, after int64
	if err = service.store.db.QueryRow("SELECT epoch FROM store_metadata").Scan(&before); err != nil {
		t.Fatal(err)
	}
	report, err := service.CollectGarbage(ctx, Principal{Root: true, TenantID: "root"}, "", GCOptions{DryRun: true, Budget: 20 * time.Second})
	if err != nil || report.Blocked || len(report.Candidates) != 1 || report.Candidates[0].Kind != "supply" {
		t.Fatal(report, err)
	}
	if err = service.store.db.QueryRow("SELECT epoch FROM store_metadata").Scan(&after); err != nil || before != after {
		t.Fatal("dry-run changed metadata", before, after, err)
	}
	result, err := service.CollectGarbage(ctx, Principal{Root: true, TenantID: "root"}, "gc", GCOptions{Batch: 1, Budget: 20 * time.Second})
	if err != nil || len(result.Collected) != 1 {
		t.Fatal(result, err)
	}
	if _, err = os.Stat(unknown); err != nil {
		t.Fatal("collector removed unknown entry", err)
	}
	var count int
	if err = service.store.db.QueryRow("SELECT COUNT(*) FROM pool_slots").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}

func TestHavenGCReclaimsInternalSleepSnapshot(t *testing.T) {
	service, _, _, _ := sandboxFixture(t)
	principal := Principal{TenantID: "tenant"}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{Mode: "none"})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if _, err := service.Execute(ctx, principal, "pause", Command{Action: "pause", SandboxID: claim.Result.SandboxID}); err != nil {
		t.Fatal(err)
	}
	paused, err := service.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	protected, err := service.CollectGarbage(ctx, Principal{Root: true, TenantID: "root"}, "", GCOptions{DryRun: true, Budget: 20 * time.Second})
	if err != nil || len(protected.Candidates) != 0 {
		t.Fatal(protected, err)
	}
	if _, err = service.Execute(ctx, principal, "release", Command{Action: "release", SandboxID: paused.ID}); err != nil {
		t.Fatal(err)
	}
	report, err := service.CollectGarbage(ctx, Principal{Root: true, TenantID: "root"}, "gc", GCOptions{Budget: 20 * time.Second})
	if err != nil || len(report.Collected) != 1 || report.Collected[0].ID != paused.SnapshotID {
		t.Fatal(report, err)
	}
	var storage int
	if err = service.store.db.QueryRow("SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources").Scan(&storage); err != nil || storage != 0 {
		t.Fatal(storage, err)
	}
}
