package testutil

import "testing"

var lifecycleTestSlots = make(chan struct{}, 4)

// Each lifecycle case owns its store, files and injected runtime. Four concurrent
// cases bound temporary RAM/copy pressure while retaining every recovery matrix.
func ParallelLifecycle(t *testing.T) {
	t.Helper()
	t.Parallel()
	lifecycleTestSlots <- struct{}{}
	t.Cleanup(func() {
		<-lifecycleTestSlots
	})
}
