package haven

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type blockingCloseRuntime struct {
	*coldRuntime

	entered chan struct{}
	release <-chan struct{}
}

func (backend *blockingCloseRuntime) Close() error {
	close(backend.entered)
	<-backend.release
	return nil
}

func TestHavenStepShutdownBackendDeadline(t *testing.T) {
	service, backend := coldService(t, nil)
	release := make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	closer := &blockingCloseRuntime{coldRuntime: backend, entered: make(chan struct{}), release: release}
	service.opts.Backend = closer
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- service.Close(ctx) }()
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
	if other, err := openStore(service.store.directory, false); !errors.Is(err, errStoreConflict) {
		if other != nil {
			_ = other.close()
		}
		t.Fatal("blocked backend finalization released the writer lock", err)
	}
	unblock.Do(func() { close(release) })
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := openStore(service.store.directory, false)
	if err != nil {
		t.Fatal("completed finalization did not release the writer", err)
	}
	if err = state.close(); err != nil {
		t.Fatal(err)
	}
}
