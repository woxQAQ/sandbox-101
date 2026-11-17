package sandbox

import (
	"context"

	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/store"
)

// OwnershipHistory reads canonical sandbox ownership without retaining a store,
// runtime or application. The transaction is always supplied by the workflow.
type OwnershipHistory struct{}

func (OwnershipHistory) ReadSandbox(ctx context.Context, query store.DBTX, id, tenant string) (ownership.Sandbox, error) {
	return LoadSandbox(ctx, query, id, tenant)
}
