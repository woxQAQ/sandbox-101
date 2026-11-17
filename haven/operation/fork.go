package operation

import (
	"context"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func (r *Repository) Finish(ctx context.Context, id, status string, body []byte) error {
	return store.RequireOne(r.queries.FinishLiveOperation(ctx, dbgen.FinishLiveOperationParams{
		ID:     id,
		Status: status,
		Body:   body,
	}))
}
