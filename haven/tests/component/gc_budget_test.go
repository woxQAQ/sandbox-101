package component

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
	"github.com/woxQAQ/haven/types"
)

func TestHavenGCBatchAndCancellation(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, _ := fixtures.GoldenSupplyFixture(t, 2)
	tenant := authorization.Principal{
		TenantID: "tenant",
	}
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	for _, key := range []string{
		"one",
		"two",
	} {
		if _, err := service.SandboxRequests.Execute(ctx, tenant, key, operation.Command{
			Action:     "refill",
			TemplateID: "golden",
		}); err != nil {
			t.Fatal(err)
		}
	}
	template, err := service.TemplateCatalog.GetTemplate(ctx, tenant, "golden")
	if err != nil {
		t.Fatal(err)
	}
	template.TargetWarm = 0
	if _, err = service.SandboxRequests.Execute(ctx, tenant, "retarget", operation.Command{
		Action:   "template_put",
		Template: &template,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := service.GC.Collect(ctx, root, "bounded", types.GCOptions{
		Batch:  1,
		Budget: 20 * time.Second,
	})
	if err != nil || len(report.Collected) != 1 || len(report.Candidates) != 1 {
		t.Fatal(report, err)
	}
	var remaining int
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM pool_slots").Scan(&remaining); err != nil || remaining != 1 {
		t.Fatal("batch consumed extra pool responsibility", remaining, err)
	}
	var before, after int
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM operations").Scan(&before); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err = service.GC.Collect(canceled, root, "canceled", types.GCOptions{
		Batch: 1,
	}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled collector continued admission", err)
	}
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM operations").Scan(&after); err != nil || before != after {
		t.Fatal("canceled collector wrote an intent", before, after, err)
	}
	report, err = service.GC.Collect(ctx, root, "rest", types.GCOptions{
		Budget: 20 * time.Second,
	})
	if err != nil || len(report.Collected) != 1 {
		t.Fatal(report, err)
	}
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM pool_slots").Scan(&remaining); err != nil || remaining != 0 {
		t.Fatal(remaining, err)
	}
}
