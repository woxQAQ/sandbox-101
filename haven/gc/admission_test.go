package gc_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestHavenGCDryRunAndSupplyCollection(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, _ := testapp.GoldenSupplyFixture(t, 1)
	runtime.Mu.Lock()
	runtime.FailBoot = true
	runtime.Mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	failed, err := service.CommandRouter.Execute(ctx, authorization.Principal{
		TenantID: "tenant",
	}, "refill", operation.Intent{
		Action:     "refill",
		TemplateID: "golden",
	})
	if err == nil || failed.Status != "failed" {
		t.Fatal(failed, err)
	}
	unknown := filepath.Join(service.State.Directory(), "vms", "unknown-private-entry")
	if err = os.Mkdir(unknown, 0o700); err != nil {
		t.Fatal(err)
	}
	var before, after int64
	if err = service.State.Database().QueryRow("SELECT epoch FROM store_metadata").Scan(&before); err != nil {
		t.Fatal(err)
	}
	report, err := service.GC.Collect(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "", types.GCOptions{
		DryRun: true,
		Budget: 20 * time.Second,
	})
	if err != nil || report.Blocked || len(report.Candidates) != 1 || report.Candidates[0].Kind != "supply" {
		t.Fatal(report, err)
	}
	if err = service.State.Database().QueryRow("SELECT epoch FROM store_metadata").Scan(&after); err != nil || before != after {
		t.Fatal("dry-run changed metadata", before, after, err)
	}
	result, err := service.GC.Collect(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "gc", types.GCOptions{
		Batch:  1,
		Budget: 20 * time.Second,
	})
	if err != nil || len(result.Collected) != 1 {
		t.Fatal(result, err)
	}
	if _, err = os.Stat(unknown); err != nil {
		t.Fatal("collector removed unknown entry", err)
	}
	var count int
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM pool_slots").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}

func TestHavenGCBatchAndCancellation(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _ := testapp.GoldenSupplyFixture(t, 2)
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
		if _, err := service.CommandRouter.Execute(ctx, tenant, key, operation.Intent{
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
	if _, err = service.CommandRouter.Execute(ctx, tenant, "retarget", operation.Intent{
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
