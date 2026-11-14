package haven

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
)

func goldenSupplyFixture(t *testing.T, target int) (*Service, *coldRuntime, *workloadFixture) {
	t.Helper()
	service, runtime, guest, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{
		TenantID: "tenant",
	}
	cold, err := service.GetTemplate(context.Background(), principal, "cold")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := Template{
		ID:         "golden",
		Image:      cold.Image,
		Shape:      cold.Shape,
		TargetWarm: target,
	}
	if _, err = service.Execute(ctx, principal, "build", Command{
		Action:   "template_build",
		Template: &spec,
	}); err != nil {
		t.Fatal(err)
	}
	return service, runtime, guest
}

func TestHavenRefillConservesPoolOccupation(t *testing.T) {
	service, runtime, _ := goldenSupplyFixture(t, 1)
	principal := Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	refill, err := service.Execute(ctx, principal, "refill", Command{
		Action:     "refill",
		TemplateID: "golden",
	})
	if err != nil {
		service.mu.Lock()
		reason := service.lastErrors[refill.ID]
		service.mu.Unlock()
		t.Fatal(refill, reason, err)
	}
	var slots, storage, warm, pool int
	if err = service.store.db.QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources),(SELECT COUNT(*) FROM warm),(SELECT COUNT(*) FROM pool_slots)").Scan(&slots, &storage, &warm, &pool); err != nil || slots != 0 || storage != 256 || warm != 1 || pool != 1 {
		t.Fatal(slots, storage, warm, pool, err)
	}
	runtime.mu.Lock()
	launches := runtime.counts["launch"]
	runtime.mu.Unlock()
	exhausted, err := service.AdmitRefill(ctx, principal, "full", Command{
		Action:     "refill",
		TemplateID: "golden",
	})
	if err != nil || exhausted.Status != "failed" {
		t.Fatal(exhausted, err)
	}
	runtime.mu.Lock()
	after := runtime.counts["launch"]
	runtime.mu.Unlock()
	if after != launches {
		t.Fatal("full pool launched another resource")
	}
	service = reopenCloneService(t, service, service.opts.Backend)
	if _, err = service.AdmitRefill(ctx, principal, "backoff", Command{
		Action:     "refill",
		TemplateID: "golden",
	}); err == nil {
		t.Fatal("durable failure backoff reset after restart")
	}
}

func TestHavenFailedRefillRetainsPoolSlot(t *testing.T) {
	service, runtime, _ := goldenSupplyFixture(t, 1)
	runtime.mu.Lock()
	runtime.failBoot = true
	runtime.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	failed, err := service.Execute(ctx, Principal{
		TenantID: "tenant",
	}, "refill", Command{
		Action:     "refill",
		TemplateID: "golden",
	})
	if err == nil || failed.Status != "failed" {
		t.Fatal(failed, err)
	}
	var pool, warm, slots int
	if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM pool_slots WHERE status='cleanup'),(SELECT COUNT(*) FROM warm),(SELECT SUM(sandbox_slots) FROM quota_resources)").Scan(&pool, &warm, &slots); err != nil || pool != 1 || warm != 0 || slots != 0 {
		t.Fatal(pool, warm, slots, err)
	}
}

func TestHavenTemplateWarmupCrashDoesNotReplay(t *testing.T) {
	service, runtime, guest, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	var executions atomic.Int64
	guest.before = func(name string, _ loom.Resource) error {
		if name == "exec" {
			executions.Add(1)
		}
		return nil
	}
	service.opts.Fault = func(point string) error {
		if point == "build_warmup_intent" {
			return ErrInterrupted
		}
		return nil
	}
	cold, err := service.GetTemplate(context.Background(), Principal{
		TenantID: "tenant",
	}, "cold")
	if err != nil {
		t.Fatal(err)
	}
	spec := Template{
		ID:     "golden",
		Image:  cold.Image,
		Shape:  cold.Shape,
		Warmup: "printf fixture",
	}
	operation, err := service.AdmitTemplate(context.Background(), Principal{
		TenantID: "tenant",
	}, "build", Command{
		Action:   "template_build",
		Template: &spec,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		service.mu.Lock()
		active := service.active[operation.ID]
		reason := service.lastErrors[operation.ID]
		service.mu.Unlock()
		if !active && reason != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("build interruption missing")
		}
		time.Sleep(time.Millisecond)
	}
	service = reopenCloneService(t, service, service.opts.Backend)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	failed, err := service.Wait(ctx, Principal{
		TenantID: "tenant",
	}, operation.ID)
	if err == nil || failed.Status != "failed" || executions.Load() != 0 {
		t.Fatal(fmt.Sprint(failed), err, executions.Load())
	}
	var slots int
	if err = service.store.db.QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources").Scan(&slots); err != nil || slots != 1 {
		t.Fatal("unknown warmup discarded build responsibility", slots, err)
	}
}

func TestHavenTemplateBuildAndGoldenClaim(t *testing.T) {
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cold, err := service.GetTemplate(ctx, principal, "cold")
	if err != nil {
		t.Fatal(err)
	}
	spec := Template{
		ID:     "golden",
		Image:  cold.Image,
		Shape:  cold.Shape,
		Warmup: "printf fixture",
	}
	operation, err := service.Execute(ctx, principal, "build", Command{
		Action:   "template_build",
		Template: &spec,
	})
	if err != nil {
		service.mu.Lock()
		reason := service.lastErrors[operation.ID]
		service.mu.Unlock()
		t.Fatal(operation, reason, err)
	}
	published, err := service.GetTemplate(ctx, principal, "golden")
	if err != nil || published.State != "ready" || published.SnapshotID == "" || published.SnapshotID != operation.Result.SnapshotID {
		t.Fatal(published, err)
	}
	var vms, slots, storage int
	if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM vms),(SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources)").Scan(&vms, &slots, &storage); err != nil || vms != 0 || slots != 0 || storage != 192 {
		t.Fatal(vms, slots, storage, err)
	}
	claim, err := service.Execute(ctx, principal, "golden-claim", Command{
		Action:     "claim",
		TemplateID: "golden",
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(claim, err)
	}
	sandbox, err := service.DataReady(ctx, principal, claim.Result.SandboxID)
	if err != nil || sandbox.State != "running" || sandbox.TemplateID != "golden" {
		t.Fatal(sandbox, err)
	}
}

func TestHavenTemplatePutOwnerAndReplay(t *testing.T) {
	service, _, _, _ := sandboxFixture(t)
	principal := Principal{
		TenantID: "tenant",
	}
	ctx := context.Background()
	cold, err := service.GetTemplate(ctx, principal, "cold")
	if err != nil {
		t.Fatal(err)
	}
	spec := Template{
		ID:    "new-cold",
		Image: cold.Image,
		Shape: cold.Shape,
	}
	first, err := service.AdmitTemplate(ctx, principal, "put", Command{
		Action:   "template_put",
		Template: &spec,
	})
	if err != nil || first.Status != "succeeded" {
		t.Fatal(first, err)
	}
	replay, err := service.AdmitTemplate(ctx, principal, "put", Command{
		Action:   "template_put",
		Template: &spec,
	})
	if err != nil || replay.ID != first.ID {
		t.Fatal(replay, err)
	}
	if _, err = service.GetTemplate(ctx, Principal{
		TenantID: "other",
	}, spec.ID); err == nil {
		t.Fatal("foreign template visible")
	}
	foreign := spec
	foreign.TenantID = "other"
	if _, err = service.AdmitTemplate(ctx, Principal{
		Root:     true,
		TenantID: "root",
	}, "foreign-put", Command{
		Action:   "template_put",
		Template: &foreign,
	}); err == nil {
		t.Fatal("root reassigned permanent tenant owner")
	}
}
