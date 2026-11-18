package fixtures

import (
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
)

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
