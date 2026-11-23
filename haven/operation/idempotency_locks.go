package operation

import "github.com/woxQAQ/haven/execution"

// IdempotencyLocks serialize replay lookup and admission within a request scope.
// Tenant and node admissions each share their own instance.
type IdempotencyLocks struct {
	keys execution.KeyedLocks[requestKey]
}

type requestKey struct {
	owner string
	key   string
}

func (locks *IdempotencyLocks) Lock(ownerID, key string) func() {
	return locks.keys.Lock(requestKey{
		owner: ownerID,
		key:   key,
	})
}
