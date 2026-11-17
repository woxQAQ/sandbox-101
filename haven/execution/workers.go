package execution

import (
	"context"
	"sync"
)

// WorkerState is an atomic observation of one process-local runner. Completed
// distinguishes a successful nil result from an operation that has never run.
// It is not a durable operation status or a resource ownership proof.
type WorkerState struct {
	Active    bool
	Completed bool
	Err       error
}

// Workers deduplicates runners within the shared lifetime. A completed runner
// may be dispatched again for recovery; only simultaneous duplicates are denied.
type Workers struct {
	mu       sync.Mutex
	lifetime *Lifetime
	states   map[string]WorkerState
}

func NewWorkers(lifetime *Lifetime) *Workers {
	return &Workers{
		lifetime: lifetime,
		states:   make(map[string]WorkerState),
	}
}

func (w *Workers) Context() context.Context {
	return w.lifetime.Context()
}

func (w *Workers) Snapshot(id string) WorkerState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.states[id]
}

// Start registers custody before launching the callback. The callback releases
// workflow guards and finishes HEAD before its result becomes observable here.
// No callback executes while the registry mutex is held by its caller.
func (w *Workers) Start(id string, run func() error) bool {
	w.mu.Lock()
	state := w.states[id]
	if state.Active || !w.lifetime.Begin() {
		w.mu.Unlock()
		return false
	}
	state.Active = true
	w.states[id] = state
	w.mu.Unlock()
	go func() {
		defer w.lifetime.End()
		err := run()
		w.mu.Lock()
		w.states[id] = WorkerState{
			Completed: true,
			Err:       err,
		}
		w.mu.Unlock()
	}()
	return true
}
