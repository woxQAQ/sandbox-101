package fixtures

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/machine"
)

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
