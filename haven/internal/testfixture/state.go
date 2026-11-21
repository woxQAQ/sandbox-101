package testfixture

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/internal/recovery"
	"github.com/woxQAQ/haven/store"
)

// OpenState creates a private writer with the real startup proof validators,
// without constructing application services or runtime workers.
func OpenState(t *testing.T) *store.State {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	state, err := store.Open(directory, true, recovery.NewRecovery().Validation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	return state
}
