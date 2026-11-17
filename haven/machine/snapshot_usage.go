package machine

import (
	"context"

	"github.com/woxQAQ/haven/store"
)

// SnapshotUsageReader reads supply's retained holders in the same snapshot as
// deletion proof. The two reads remain separate to retain failure ordering.
type SnapshotUsageReader interface {
	TemplateSnapshotUsers(context.Context, store.DBTX, string) (int64, error)
	PoolSnapshotUsers(context.Context, store.DBTX, string) (int64, error)
}
