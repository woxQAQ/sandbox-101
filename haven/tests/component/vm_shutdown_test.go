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
)

type blockingCloseRuntime struct {
	*testfixture.ColdRuntime

	entered chan struct{}
	release <-chan struct{}
}

func (backend *blockingCloseRuntime) Close() error {
	close(backend.entered)
	<-backend.release
	return nil
}

func TestHavenStepShutdownBackendDeadline(t *testing.T) {
	parallelLifecycle(t)
	release := make(chan struct{})
	var closer *blockingCloseRuntime
	service, _ := coldServiceWithBackend(t, nil, func(backend *testfixture.ColdRuntime) loom.Backend {
		closer = &blockingCloseRuntime{
			ColdRuntime: backend,
			entered:     make(chan struct{}),
			release:     release,
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
	case <-closer.entered:
	case <-time.After(time.Second):
		t.Fatal("backend finalization did not start")
	}
	if other, err := openStore(service.State.Directory(), false); !errors.Is(err, store.ErrConflict) {
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
	state, err := openStore(service.State.Directory(), false)
	if err != nil {
		t.Fatal("completed finalization did not release the writer", err)
	}
	if err = state.Close(); err != nil {
		t.Fatal(err)
	}
}
