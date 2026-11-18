package component

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/gc"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
	"github.com/woxQAQ/haven/types"
)

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
