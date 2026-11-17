package machine

import (
	"context"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func VersionRoots(ctx context.Context, query store.DBTX) ([]string, error) {
	return dbgen.New(query).ListVersionIDs(ctx)
}

func RetainedInstanceCount(ctx context.Context, query store.DBTX, vmID string) (int64, error) {
	return dbgen.New(query).CountInstances(ctx, vmID)
}

func RetainedVersionCount(ctx context.Context, query store.DBTX, vmID string) (int64, error) {
	return dbgen.New(query).CountVersions(ctx, vmID)
}
