package haven

import (
	"context"
	"database/sql"
	"errors"

	"github.com/woxQAQ/haven/loom"
)

// allocationCleanupFence keeps a detached pool member unavailable to every new
// resource workflow while its independent collector retains responsibility.
func allocationCleanupFence(ctx context.Context, q queryer, vm string, parent resourceParent) error {
	var pending int
	if err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind='direct_policy' AND resource_id=? AND status='pending'", vm).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return loom.Fail(loom.Conflict, "allocation has unresolved direct policy retirement")
	}
	var owner string
	err := q.QueryRowContext(ctx, "SELECT operation_id FROM cleanup_intents WHERE resource_kind='supply_cleanup' AND resource_id=? AND status='pending'", vm).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	collector, ok := parent.(*supplyCleanupAdmission)
	if !ok || collector == nil || collector.Parent.OperationID != owner {
		return loom.Fail(loom.Conflict, "allocation is fenced by independent supply cleanup")
	}
	return nil
}
