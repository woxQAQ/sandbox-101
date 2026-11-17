package operation

import (
	"context"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func (r *Repository) Run(ctx context.Context, operationID string) error {
	return store.RequireOne(r.queries.RunLiveOperation(ctx, operationID))
}

func (r *Repository) FailResult(ctx context.Context, operationID string, body []byte) error {
	return store.RequireOne(r.queries.FailResult(ctx, dbgen.FailResultParams{
		ID:   operationID,
		Body: body,
	}))
}
