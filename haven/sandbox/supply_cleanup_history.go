package sandbox

import (
	"context"

	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// SupplyCleanupHistory holds no connection or running service.
type SupplyCleanupHistory struct {
	OwnershipHistory
}

func (SupplyCleanupHistory) SettingsCount(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountSandboxSettings(ctx, id)
}
