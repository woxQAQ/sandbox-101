package component

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenOperationWaitRetainsWorkerCustodyAfterCommit(t *testing.T) {
	parallelLifecycle(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releasedOnce sync.Once
	interrupted := errors.New("fixture interrupted after durable completion")
	service, backend, _, _ := testapp.SandboxFixtureWithFault(t, func(point string) error {
		if point != "activation_completed" {
			return nil
		}
		enteredOnce.Do(func() {
			close(entered)
		})
		<-release
		return interrupted
	})
	t.Cleanup(func() {
		releasedOnce.Do(func() {
			close(release)
		})
	})
	accepted, err := service.Sandboxes.AdmitClaim(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, "wait-custody", operation.Command{
		Action:     "claim",
		TemplateID: "cold",
		TTLSeconds: 60,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not reach durable completion")
	}
	current, err := service.Operations.GetOperation(context.Background(), "tenant", accepted.ID)
	if err != nil || current.Status != "succeeded" {
		t.Fatal("fixture did not publish terminal metadata before blocking", current, err)
	}
	var callers sync.WaitGroup
	for range 16 {
		callers.Go(func() {
			service.Maintenance.Dispatch(accepted.ID)
		})
	}
	callers.Wait()
	worker := service.Workers.State(accepted.ID)
	if !worker.Active || worker.Completed || worker.Err != nil {
		t.Fatal("active worker was replaced or prematurely completed", worker)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = service.Operations.Wait(ctx, authorization.Principal{
		TenantID: "tenant",
	}, accepted.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("terminal metadata escaped the worker completion barrier", err)
	}
	releasedOnce.Do(func() {
		close(release)
	})
	awaitColdWorker(t, service, accepted.ID)
	worker = service.Workers.State(accepted.ID)
	if worker.Active || !worker.Completed || !errors.Is(worker.Err, interrupted) {
		t.Fatal("worker result was not published atomically", worker)
	}
	if result, err := service.Operations.Wait(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, accepted.ID); err != nil || result.Status != "succeeded" {
		t.Fatal("runner diagnostic rewrote committed operation success", result, err)
	}
	service.Maintenance.Dispatch(accepted.ID)
	awaitColdWorker(t, service, accepted.ID)
	worker = service.Workers.State(accepted.ID)
	if worker.Active || !worker.Completed || worker.Err != nil || backend.Count("launch") != 1 {
		t.Fatal("completed replay retained stale diagnostics or duplicated effects", worker)
	}
}
