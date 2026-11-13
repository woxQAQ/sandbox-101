package haven

import (
	"context"
	"testing"
	"time"
)

func TestHavenGCUnknownNormalizedRootBlocksCollection(t *testing.T) {
	service, runtime, _ := goldenSupplyFixture(t, 1)
	runtime.mu.Lock()
	runtime.failBoot = true
	runtime.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if operation, err := service.Execute(ctx, Principal{TenantID: "tenant"}, "refill", Command{Action: "refill", TemplateID: "golden"}); err == nil || operation.Status != "failed" {
		t.Fatal(operation, err)
	}
	if _, err := service.store.db.Exec("INSERT INTO resource_refs VALUES('unknown-resource','private-object','unknown-holder','private-holder')"); err != nil {
		t.Fatal(err)
	}
	var before, after int
	if err := service.store.db.QueryRow("SELECT COUNT(*) FROM operations").Scan(&before); err != nil {
		t.Fatal(err)
	}
	report, err := service.CollectGarbage(ctx, Principal{Root: true, TenantID: "root"}, "gc", GCOptions{Budget: 10 * time.Second})
	if err != nil || !report.Blocked || len(report.Collected) != 0 {
		t.Fatal("unknown root did not fence collection", report, err)
	}
	if err = service.store.db.QueryRow("SELECT COUNT(*) FROM operations").Scan(&after); err != nil || before != after {
		t.Fatal("blocked graph admitted destructive work", before, after, err)
	}
	var pending int
	if err = service.store.db.QueryRow("SELECT COUNT(*) FROM pool_slots WHERE status='cleanup'").Scan(&pending); err != nil || pending != 1 {
		t.Fatal("blocked graph lost supply responsibility", pending, err)
	}
}
