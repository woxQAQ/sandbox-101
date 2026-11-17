package store

import (
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// A fork temporarily shares the writer's open file description before CLOEXEC.
// Store.Close must release ownership even while that duplicate is still alive;
// application shutdown tests separately enforce that workers exit before Close.
func TestHavenWriterHandoffWithRetainedLockDescriptor(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	first, err := Open(directory, true, fixtureValidation())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	duplicate, err := unix.Dup(int(first.lock.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(duplicate)
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(directory, false, fixtureValidation())
	if err != nil {
		t.Fatalf("completed close retained writer ownership through another descriptor: %v", err)
	}
	defer next.Close()
	if other, err := Open(directory, false, fixtureValidation()); err == nil {
		_ = other.Close()
		t.Fatal("new writer failed to exclude another store opener")
	}
}
