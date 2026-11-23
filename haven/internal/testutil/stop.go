package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/machine"
)

func AwaitStop(t *testing.T, service *application.App, id string) machine.StopStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, seen := worker.Active, worker.Completed
		if !active && seen {
			step, err := machine.LoadStop(context.Background(), service.State.Database(), id)
			if err != nil {
				t.Fatal(err)
			}
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("stop worker timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
