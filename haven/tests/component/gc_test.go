package component

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/gc"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
	"github.com/woxQAQ/haven/types"
)

func TestHavenGCDryRunAndSupplyCollection(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, runtime, _ := fixtures.GoldenSupplyFixture(t, 1)
	runtime.Mu.Lock()
	runtime.FailBoot = true
	runtime.Mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	failed, err := service.SandboxRequests.Execute(ctx, authorization.Principal{
		TenantID: "tenant",
	}, "refill", operation.Command{
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

func TestHavenGCUnknownNormalizedRootBlocksCollection(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, runtime, _ := fixtures.GoldenSupplyFixture(t, 1)
	runtime.Mu.Lock()
	runtime.FailBoot = true
	runtime.Mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if acceptedRecord, err := service.SandboxRequests.Execute(ctx, authorization.Principal{
		TenantID: "tenant",
	}, "refill", operation.Command{
		Action:     "refill",
		TemplateID: "golden",
	}); err == nil || acceptedRecord.Status != "failed" {
		t.Fatal(acceptedRecord, err)
	}
	if _, err := service.State.Database().Exec("INSERT INTO resource_refs VALUES('unknown-resource','private-object','unknown-holder','private-holder')"); err != nil {
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
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM pool_slots WHERE status='cleanup'").Scan(&pending); err != nil || pending != 1 {
		t.Fatal("blocked graph lost supply responsibility", pending, err)
	}
}

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

func TestHavenGCCompletionRetainsChildProgress(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT,'private GC completion failure'",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, runtime, _ := fixtures.GoldenSupplyFixture(t, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			runtime.Mu.Lock()
			runtime.FailBoot = true
			runtime.Mu.Unlock()
			original, err := service.SandboxRequests.Execute(ctx, authorization.Principal{
				TenantID: "tenant",
			}, "failed-refill", operation.Command{
				Action:     "refill",
				TemplateID: "golden",
			})
			if err == nil || original.Status != "failed" {
				t.Fatal("fixture did not retain failed supply", original, err)
			}
			runtime.Mu.Lock()
			runtime.FailBoot = false
			runtime.Mu.Unlock()
			if _, err = service.State.Database().Exec("CREATE TRIGGER reject_gc_completion BEFORE UPDATE ON operation_steps WHEN OLD.step_key='gc' AND NEW.status='succeeded' BEGIN SELECT RAISE(" + rejection + "); END"); err != nil {
				t.Fatal(err)
			}
			root := authorization.Principal{
				Root:     true,
				TenantID: "root",
			}
			options := types.GCOptions{
				Batch:  1,
				Budget: 10 * time.Second,
			}
			report, err := service.GC.Collect(ctx, root, "collect", options)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || report.OperationID == "" {
				t.Fatal("GC did not expose rejected final progress", report, err)
			}
			before, err := gc.Load(ctx, service.State.Database(), report.OperationID)
			if err != nil || before.Status != "running" || before.State.Next != 1 || len(before.State.Report.Collected) != 1 || len(before.Input.Report.Candidates) != 1 {
				t.Fatal("failed GC completion lost durable child progress", before, err)
			}
			childID := before.State.Report.Collected[0].OperationID
			child, err := service.Operations.GetManagementOperation(ctx, root, childID)
			if err != nil || child.Status != "succeeded" {
				t.Fatal("fixture did not cross independent child completion", child, err)
			}
			stops, removals := runtime.Count("stop"), runtime.Count("remove")
			if _, err = service.State.Database().Exec("DROP TRIGGER reject_gc_completion"); err != nil {
				t.Fatal(err)
			}
			replayed, err := service.GC.Collect(ctx, root, "collect", options)
			if err != nil || replayed.OperationID != report.OperationID || len(replayed.Collected) != 1 || replayed.Collected[0].OperationID != childID {
				t.Fatal("GC replay changed accepted pass or child result", replayed, err)
			}
			done, err := gc.Load(ctx, service.State.Database(), report.OperationID)
			if err != nil || done.Status != "succeeded" || done.Hash != before.Hash || !reflect.DeepEqual(done.Input, before.Input) || done.State.Next != before.State.Next {
				t.Fatal("GC replay changed frozen inventory, budget or deadline", done, err)
			}
			if runtime.Count("stop") != stops || runtime.Count("remove") != removals {
				t.Fatal("GC replay repeated retired child effects")
			}
			unchanged, err := service.Operations.GetOperation(ctx, "tenant", original.ID)
			if err != nil || !reflect.DeepEqual(unchanged, original) {
				t.Fatal("GC changed original supply failure", unchanged, err)
			}
		})
	}
}
