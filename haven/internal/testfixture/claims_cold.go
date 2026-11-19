// Package fixtures provides test resources shared across Haven packages.
package fixtures

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

var lifecycleTestSlots = make(chan struct{}, 4)

// Each lifecycle case owns its store, files and injected runtime. Four concurrent
// cases bound temporary RAM/copy pressure while retaining every recovery matrix.
func ParallelLifecycle(t *testing.T) {
	t.Helper()
	t.Parallel()
	lifecycleTestSlots <- struct{}{}
	t.Cleanup(func() {
		<-lifecycleTestSlots
	})
}

func ColdService(t *testing.T, fault func(string) error) (*application.App, *testfixture.ColdRuntime) {
	t.Helper()
	return ColdServiceWithBackend(t, fault, nil)
}

func ColdServiceWithBackend(t *testing.T, fault func(string) error, factory func(*testfixture.ColdRuntime) loom.Backend) (*application.App, *testfixture.ColdRuntime) {
	t.Helper()
	return testapp.ColdServiceWithOptions(t, application.Options{
		Fault: fault,
	}, factory)
}

func ColdClaim(t *testing.T, service *application.App, key string) operation.Record {
	t.Helper()
	acceptedOperation, err := service.Provisioning.AdmitCold(context.Background(), machine.FreshRequest{
		TenantID:   "tenant",
		Key:        key,
		TemplateID: "cold",
	})
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation
}

func AwaitColdWorker(t *testing.T, service *application.App, id string) machine.ResourceStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, last, seen := worker.Active, worker.Err, worker.Completed
		step, err := machine.LoadResourceStep(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatalf("resource worker did not stop: phase=%s status=%s error=%v", step.State.Phase, step.Status, last)
		}
		time.Sleep(time.Millisecond)
	}
}

func ResumeColdService(t *testing.T, old *application.App, backend *testfixture.ColdRuntime) *application.App {
	t.Helper()
	return ResumeColdServiceWithFault(t, old, backend, nil)
}

func ResumeColdServiceWithFault(t *testing.T, old *application.App, backend *testfixture.ColdRuntime, fault func(string) error) *application.App {
	t.Helper()
	if err := old.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts := old.Options
	opts.Initialize = false
	opts.Fault = fault
	opts.Backend = backend
	service, err := application.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return service
}
