package fixtures

import (
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
)

func WaitFixtureWorker(t *testing.T, service *application.App, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		active := service.Workers.State(id).Active
		if !active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("private workflow worker still active")
		}
		time.Sleep(time.Millisecond)
	}
}
