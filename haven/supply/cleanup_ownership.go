package supply

import (
	"context"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/store"
)

// CleanupOwnershipReader checks that a retained raw allocation has not acquired
// sandbox settings. Both reads use the caller's original transaction.
type CleanupOwnershipReader interface {
	machine.SandboxOwnershipReader
	SettingsCount(context.Context, store.DBTX, string) (int64, error)
}
