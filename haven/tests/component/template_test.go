package component

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
)

func goldenSupplyFixture(t *testing.T, target int) (*application.App, *testfixture.ColdRuntime, *testfixture.WorkloadFixture) {
	t.Helper()
	return goldenSupplyFixtureWithFault(t, target, nil)
}

func goldenSupplyFixtureWithFault(t *testing.T, target int, fault func(string) error) (*application.App, *testfixture.ColdRuntime, *testfixture.WorkloadFixture) {
	t.Helper()
	service, runtime, guest, _ := testapp.SandboxFixtureWithBackend(t, fault, restorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	cold, err := service.TemplateCatalog.GetTemplate(context.Background(), principal, "cold")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := operation.TemplateSpec{
		ID:         "golden",
		Image:      cold.Image,
		Shape:      cold.Shape,
		TargetWarm: target,
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "build", operation.Command{
		Action:   "template_build",
		Template: &spec,
	}); err != nil {
		t.Fatal(err)
	}
	return service, runtime, guest
}

func TestHavenRefillConservesPoolOccupation(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, _ := goldenSupplyFixture(t, 1)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	refill, err := service.SandboxRequests.Execute(ctx, principal, "refill", operation.Command{
		Action:     "refill",
		TemplateID: "golden",
	})
	if err != nil {
		reason := service.Workers.State(refill.ID).Err
		t.Fatal(refill, reason, err)
	}
	var slots, storage, warm, pool int
	if err = service.State.Database().QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources),(SELECT COUNT(*) FROM warm),(SELECT COUNT(*) FROM pool_slots)").Scan(&slots, &storage, &warm, &pool); err != nil || slots != 0 || storage != 256 || warm != 1 || pool != 1 {
		t.Fatal(slots, storage, warm, pool, err)
	}
	runtime.Mu.Lock()
	launches := runtime.Counts["launch"]
	runtime.Mu.Unlock()
	exhausted, err := service.Supply.AdmitRefill(ctx, principal, "full", operation.Command{
		Action:     "refill",
		TemplateID: "golden",
	})
	if err != nil || exhausted.Status != "failed" {
		t.Fatal(exhausted, err)
	}
	runtime.Mu.Lock()
	after := runtime.Counts["launch"]
	runtime.Mu.Unlock()
	if after != launches {
		t.Fatal("full pool launched another resource")
	}
	service = testapp.ReopenCloneService(t, service, service.Options.Backend)
	if _, err = service.Supply.AdmitRefill(ctx, principal, "backoff", operation.Command{
		Action:     "refill",
		TemplateID: "golden",
	}); err == nil {
		t.Fatal("durable failure backoff reset after restart")
	}
}

func TestHavenFailedRefillRetainsPoolSlot(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, _ := goldenSupplyFixture(t, 1)
	runtime.Mu.Lock()
	runtime.FailBoot = true
	runtime.Mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	var pool, warm, slots int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM pool_slots WHERE status='cleanup'),(SELECT COUNT(*) FROM warm),(SELECT SUM(sandbox_slots) FROM quota_resources)").Scan(&pool, &warm, &slots); err != nil || pool != 1 || warm != 0 || slots != 0 {
		t.Fatal(pool, warm, slots, err)
	}
}

func TestHavenTemplateWarmupCrashDoesNotReplay(t *testing.T) {
	parallelLifecycle(t)
	service, _, guest, _ := testapp.SandboxFixtureWithBackend(t, func(point string) error {
		if point == "build_warmup_intent" {
			return errs.ErrInterrupted
		}
		return nil
	}, restorableBackend)
	var executions atomic.Int64
	guest.Before = func(name string, _ loom.Resource) error {
		if name == "exec" {
			executions.Add(1)
		}
		return nil
	}
	cold, err := service.TemplateCatalog.GetTemplate(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, "cold")
	if err != nil {
		t.Fatal(err)
	}
	spec := operation.TemplateSpec{
		ID:     "golden",
		Image:  cold.Image,
		Shape:  cold.Shape,
		Warmup: "printf fixture",
	}
	acceptedRecord, err := service.Supply.AdmitTemplate(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, "build", operation.Command{
		Action:   "template_build",
		Template: &spec,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(acceptedRecord.ID)
		active, reason := worker.Active, worker.Err
		if !active && reason != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("build interruption missing")
		}
		time.Sleep(time.Millisecond)
	}
	service = testapp.ReopenCloneService(t, service, service.Options.Backend)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	failed, err := service.Operations.Wait(ctx, authorization.Principal{
		TenantID: "tenant",
	}, acceptedRecord.ID)
	if err == nil || failed.Status != "failed" || executions.Load() != 0 {
		t.Fatal(fmt.Sprint(failed), err, executions.Load())
	}
	var slots int
	if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources").Scan(&slots); err != nil || slots != 1 {
		t.Fatal("unknown warmup discarded build responsibility", slots, err)
	}
}

func TestHavenTemplateBuildAndGoldenClaim(t *testing.T) {
	parallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, restorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cold, err := service.TemplateCatalog.GetTemplate(ctx, principal, "cold")
	if err != nil {
		t.Fatal(err)
	}
	spec := operation.TemplateSpec{
		ID:     "golden",
		Image:  cold.Image,
		Shape:  cold.Shape,
		Warmup: "printf fixture",
	}
	acceptedRecord, err := service.SandboxRequests.Execute(ctx, principal, "build", operation.Command{
		Action:   "template_build",
		Template: &spec,
	})
	if err != nil {
		reason := service.Workers.State(acceptedRecord.ID).Err
		t.Fatal(acceptedRecord, reason, err)
	}
	published, err := service.TemplateCatalog.GetTemplate(ctx, principal, "golden")
	if err != nil || published.State != "ready" || published.SnapshotID == "" || published.SnapshotID != acceptedRecord.Result.SnapshotID {
		t.Fatal(published, err)
	}
	var vms, slots, storage int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM vms),(SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources)").Scan(&vms, &slots, &storage); err != nil || vms != 0 || slots != 0 || storage != 192 {
		t.Fatal(vms, slots, storage, err)
	}
	claim, err := service.SandboxRequests.Execute(ctx, principal, "golden-claim", operation.Command{
		Action:     "claim",
		TemplateID: "golden",
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(claim, err)
	}
	sandbox, err := service.Dataplane.Ready(ctx, principal, claim.Result.SandboxID)
	if err != nil || sandbox.State != "running" || sandbox.TemplateID != "golden" {
		t.Fatal(sandbox, err)
	}
}

func TestHavenTemplatePutOwnerAndReplay(t *testing.T) {
	parallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx := context.Background()
	cold, err := service.TemplateCatalog.GetTemplate(ctx, principal, "cold")
	if err != nil {
		t.Fatal(err)
	}
	spec := operation.TemplateSpec{
		ID:    "new-cold",
		Image: cold.Image,
		Shape: cold.Shape,
	}
	first, err := service.Supply.AdmitTemplate(ctx, principal, "put", operation.Command{
		Action:   "template_put",
		Template: &spec,
	})
	if err != nil || first.Status != "succeeded" {
		t.Fatal(first, err)
	}
	replay, err := service.Supply.AdmitTemplate(ctx, principal, "put", operation.Command{
		Action:   "template_put",
		Template: &spec,
	})
	if err != nil || replay.ID != first.ID {
		t.Fatal(replay, err)
	}
	if _, err = service.TemplateCatalog.GetTemplate(ctx, authorization.Principal{
		TenantID: "other",
	}, spec.ID); err == nil {
		t.Fatal("foreign template visible")
	}
	foreign := spec
	foreign.TenantID = "other"
	if _, err = service.Supply.AdmitTemplate(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "foreign-put", operation.Command{
		Action:   "template_put",
		Template: &foreign,
	}); err == nil {
		t.Fatal("root reassigned permanent tenant owner")
	}
}
