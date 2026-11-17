package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// AllocationCleanupFence keeps a detached pool member unavailable to every new
// resource workflow while its independent collector retains responsibility.
func AllocationCleanupFence(ctx context.Context, database store.DBTX, vm string, parent ResourceParent) error {
	pending, err := dbgen.New(database).CountPendingCleanupResource(ctx, dbgen.CountPendingCleanupResourceParams{
		ResourceKind: "direct_policy",
		ResourceID:   vm,
	})
	if err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("%w: allocation has unresolved direct policy retirement", errs.ErrConflict)
	}
	owner, err := dbgen.New(database).PendingCleanupResourceOwner(ctx, dbgen.PendingCleanupResourceOwnerParams{
		ResourceKind: "supply_cleanup",
		ResourceID:   vm,
	})

	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	collector, ok := parent.(SupplyCleanupAuthority)
	if !ok {
		return fmt.Errorf("%w: allocation is fenced by independent supply cleanup", errs.ErrConflict)
	}
	collectorID, valid := collector.SupplyCleanupOperation()
	if !valid || collectorID != owner {
		return fmt.Errorf("%w: allocation is fenced by independent supply cleanup", errs.ErrConflict)
	}
	return nil
}
