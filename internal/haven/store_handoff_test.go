package haven

import (
	"context"
	"testing"

	"golang.org/x/sys/unix"
)

// A fork temporarily shares the writer's open file description before CLOEXEC.
// Completed Service.Close must release ownership even while that duplicate is
// still alive; the next real store writer must not depend on child scheduling.
func TestHavenWriterHandoffWithRetainedLockDescriptor(t *testing.T) {
	service, backend := coldService(t, nil)
	duplicate, err := unix.Dup(int(service.store.lock.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(duplicate)
	if err = service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	next, err := Open(Options{StateDir: service.store.directory, Backend: backend, MaxMemoryMiB: 8192, MaxVCPUs: 64, SafetyBytes: 1})
	if err != nil {
		t.Fatalf("completed close retained writer ownership through another descriptor: %v", err)
	}
	defer next.Close(context.Background())
	if other, err := Open(Options{StateDir: service.store.directory, Backend: backend}); err == nil {
		_ = other.Close(context.Background())
		t.Fatal("new writer failed to exclude another store opener")
	}
}
