package execution

import "sync"

// Guard serializes work shared by several services.
type Guard interface {
	// Lock waits for exclusive access.
	Lock()
	// Unlock releases the held lock.
	Unlock()
	// TryLock acquires the lock if immediately available.
	TryLock() bool
}

// KeyedLocks serializes work on equal keys within one domain. The zero value
// is ready to use. Entries exist only while a holder or waiter references them.
type KeyedLocks[K comparable] struct {
	mu      sync.Mutex
	entries map[K]*lockEntry
}

type lockEntry struct {
	mu    sync.Mutex
	users int
}

// Lock waits for key and returns the function that releases it.
func (locks *KeyedLocks[K]) Lock(key K) func() {
	entry := locks.retain(key)
	entry.mu.Lock()
	return func() {
		locks.unlock(key, entry)
	}
}

// TryLock returns a release function on success and nil when key is busy.
func (locks *KeyedLocks[K]) TryLock(key K) func() {
	entry := locks.retain(key)
	if !entry.mu.TryLock() {
		locks.drop(key, entry)
		return nil
	}
	return func() {
		locks.unlock(key, entry)
	}
}

// With holds key through the synchronous callback, including its cleanup.
func (locks *KeyedLocks[K]) With(key K, work func() error) error {
	release := locks.Lock(key)
	defer release()
	return work()
}

func (locks *KeyedLocks[K]) retain(key K) *lockEntry {
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if locks.entries == nil {
		locks.entries = make(map[K]*lockEntry)
	}
	entry := locks.entries[key]
	if entry == nil {
		entry = &lockEntry{}
		locks.entries[key] = entry
	}
	entry.users++
	return entry
}

func (locks *KeyedLocks[K]) unlock(key K, entry *lockEntry) {
	entry.mu.Unlock()
	locks.drop(key, entry)
}

func (locks *KeyedLocks[K]) drop(key K, entry *lockEntry) {
	locks.mu.Lock()
	defer locks.mu.Unlock()
	entry.users--
	if entry.users == 0 {
		delete(locks.entries, key)
	}
}
