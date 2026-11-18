package fixtures

import (
	"github.com/woxQAQ/haven/internal/recovery"
	"github.com/woxQAQ/haven/store"
)

func OpenStore(directory string, initialize bool) (*store.State, error) {
	return store.Open(directory, initialize, recovery.NewRecovery().Validation())
}
