package operation

import (
	"context"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// RequestState reads the original input and mutable result from the same row.
type RequestState struct {
	RequestIdentity

	Body []byte
}

func (r *Repository) RequestState(ctx context.Context, id string) (RequestState, error) {
	row, err := r.queries.GetRequestState(ctx, id)
	return RequestState{
		RequestIdentity: RequestIdentity{
			Hash:      row.RequestHash,
			Request:   row.Request,
			ScopeKind: row.ScopeKind,
			ScopeID:   row.ScopeID,
			Status:    row.Status,
		},
		Body: row.Body,
	}, err
}

func (r *Repository) RunResult(ctx context.Context, id string, body []byte) error {
	return store.RequireOne(r.queries.RunLiveOperationResult(ctx, dbgen.RunLiveOperationResultParams{
		Body: body,
		ID:   id,
	}))
}
