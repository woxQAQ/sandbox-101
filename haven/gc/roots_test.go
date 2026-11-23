package gc_test

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestHavenGCUnknownNormalizedRootBlocksCollection(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, _ := testapp.GoldenSupplyFixture(t, 1)
	runtime.Mu.Lock()
	runtime.FailBoot = true
	runtime.Mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if acceptedRecord, err := service.CommandAdmission.Execute(ctx, authorization.Principal{
		TenantID: "tenant",
	}, "refill", operation.Intent{
		Action:     "refill",
		TemplateID: "golden",
	}); err == nil || acceptedRecord.Status != "failed" {
		t.Fatal(acceptedRecord, err)
	}
	if _, err := service.State.Database().Exec("INSERT INTO resource_refs(resource_kind,resource_id,holder_kind,holder_id) VALUES('unknown-resource','private-object','unknown-holder','private-holder')"); err != nil {
		t.Fatal(err)
	}
	var before, after int
	if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM operations").Scan(&before); err != nil {
		t.Fatal(err)
	}
	report, err := service.GC.Collect(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "gc", types.GCOptions{
		Budget: 10 * time.Second,
	})
	if err != nil || !report.Blocked || len(report.Collected) != 0 {
		t.Fatal("unknown root did not fence collection", report, err)
	}
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM operations").Scan(&after); err != nil || before != after {
		t.Fatal("blocked graph admitted destructive work", before, after, err)
	}
	var pending int
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM pool_slots WHERE deleted_at IS NULL AND status='cleanup'").Scan(&pending); err != nil || pending != 1 {
		t.Fatal("blocked graph lost supply responsibility", pending, err)
	}
}
