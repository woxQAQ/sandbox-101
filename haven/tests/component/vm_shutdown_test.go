package component

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenStepShutdownBackendDeadline(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	release := make(chan struct{})
	var closer *fixtures.BlockingCloseRuntime
	service, _ := fixtures.ColdServiceWithBackend(t, nil, func(backend *testfixture.ColdRuntime) loom.Backend {
		closer = &fixtures.BlockingCloseRuntime{
			ColdRuntime: backend,
			Entered:     make(chan struct{}),
			Release:     release,
		}
		return closer
	})
	var unblock sync.Once
	t.Cleanup(func() {
		unblock.Do(func() {
			close(release)
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- service.Close(ctx)
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("backend close did not retain caller deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("backend close blocked the caller beyond its deadline")
	}
	select {
	case <-closer.Entered:
	case <-time.After(time.Second):
		t.Fatal("backend finalization did not start")
	}
	if other, err := fixtures.OpenStore(service.State.Directory(), false); !errors.Is(err, store.ErrConflict) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal("blocked backend finalization released the writer lock", err)
	}
	unblock.Do(func() {
		close(release)
	})
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := fixtures.OpenStore(service.State.Directory(), false)
	if err != nil {
		t.Fatal("completed finalization did not release the writer", err)
	}
	if err = state.Close(); err != nil {
		t.Fatal(err)
	}
}
