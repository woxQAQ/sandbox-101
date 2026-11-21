// Package execution owns process-local admission and worker lifetime mechanics.
package execution

import (
	"context"
	"sync"
)

// Lifetime closes admission before waiting for work. Begin and Close share one
// mutex, so no positive WaitGroup addition can race the final zero-count wait.
// Revocation runs before draining, while persistence closes only after draining.
type Lifetime struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	closing  bool
	workers  sync.WaitGroup
	stopped  chan struct{}
	closeErr error
}

func NewLifetime(parent context.Context) *Lifetime {
	ctx, cancel := context.WithCancel(parent)
	return &Lifetime{
		ctx:     ctx,
		cancel:  cancel,
		stopped: make(chan struct{}),
	}
}

func (l *Lifetime) Context() context.Context {
	return l.ctx
}

func (l *Lifetime) Begin() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing {
		return false
	}
	l.workers.Add(1)
	return true
}

func (l *Lifetime) End() {
	l.workers.Done()
}

func (l *Lifetime) Closing() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closing
}

// Close starts a single finalizer and lets each caller bound its own wait.
// A caller timeout does not abandon workers or release their persistence owner.
// Neither callback is invoked with the lifetime mutex held.
func (l *Lifetime) Close(ctx context.Context, revoke func(), finalize func() error) error {
	l.mu.Lock()
	start := !l.closing
	if start {
		l.closing = true
		l.cancel()
	}
	l.mu.Unlock()
	if start {
		go func() {
			revoke()
			l.workers.Wait()
			l.closeErr = finalize()
			close(l.stopped)
		}()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.stopped:
		return l.closeErr
	}
}
