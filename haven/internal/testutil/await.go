package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/machine"
)

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

func AwaitCapture(t *testing.T, service *application.App, id string) machine.CaptureStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, last, seen := worker.Active, worker.Err, worker.Completed
		step, err := machine.LoadCapture(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture worker did not stop: %+v %v", step.State, last)
		}
		time.Sleep(time.Millisecond)
	}
}

func AwaitDiscard(t *testing.T, service *application.App, id string) machine.DiscardStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, seen := worker.Active, worker.Completed
		step, err := machine.LoadDiscard(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("discard worker did not finish", step.State)
		}
		time.Sleep(time.Millisecond)
	}
}

func WaitImportInterruption(t *testing.T, service *application.App, id string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, last := worker.Active, worker.Err
		if !active && last != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("image effect interruption missing")
		}
		time.Sleep(time.Millisecond)
	}
}
