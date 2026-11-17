package supply

import (
	"context"

	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

// ResourceUsage reads retained supply users through the caller's database snapshot.
type ResourceUsage struct{}

func (ResourceUsage) VMUsers(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountWarmVM(ctx, id)
}

func (ResourceUsage) TemplateSnapshotUsers(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountSnapshotUsers(ctx, id)
}

func (ResourceUsage) PoolSnapshotUsers(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountWarmSnapshot(ctx, id)
}
