package execution

import (
	"runtime"
	"sync"
	"testing"
)

func TestKeyedLocksRetireContendedKeys(t *testing.T) {
	var locks KeyedLocks[string]
	release := locks.Lock("busy")
	if locks.TryLock("busy") != nil {
		t.Fatal("second holder entered the same key")
	}
	other := locks.TryLock("independent")
	if other == nil {
		t.Fatal("unrelated key blocked")
	}
	other()

	const workers = 16
	var attempts, done sync.WaitGroup
	var visits int
	attempts.Add(workers)
	for range workers {
		done.Go(func() {
			attempts.Done()
			for range 10 {
				unlock := locks.Lock("busy")
				previous := visits
				runtime.Gosched()
				visits = previous + 1
				unlock()
			}
		})
	}
	attempts.Wait()
	release()
	done.Wait()
	if visits != workers*10 {
		t.Fatalf("serialized updates lost: got %d", visits)
	}
	if len(locks.entries) != 0 {
		t.Fatalf("idle lock entries retained: %d", len(locks.entries))
	}
}
