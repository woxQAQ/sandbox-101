package operation

import (
	"context"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// Scope keeps an operation's durable authorization kind and identity together.
// The caller must still establish authority before querying that scope.
type Scope struct {
	Kind string
	ID   string
}

// RequestIdentity keeps the original request alongside its current status.
// Readers verify the stored bytes; current resource ownership cannot replace it.
type RequestIdentity struct {
	Status    string
	ScopeKind string
	ScopeID   string
	Hash      string
	Request   []byte
}

func (r *Repository) RequestIdentity(ctx context.Context, id string) (RequestIdentity, error) {
	row, err := r.queries.GetRequestIdentity(ctx, id)
	return RequestIdentity{
		Status:    row.Status,
		ScopeKind: row.ScopeKind,
		ScopeID:   row.ScopeID,
		Hash:      row.RequestHash,
		Request:   row.Request,
	}, err
}

func (r *Repository) StepTarget(ctx context.Context, id, key, hash string) (candidate, expected string, err error) {
	row, err := r.queries.GetStepTarget(ctx, dbgen.GetStepTargetParams{
		OperationID: id,
		StepKey:     key,
		InputHash:   hash,
	})
	return row.CandidateResourceID, row.ExpectedInstance, err
}

func (r *Repository) LiveBody(ctx context.Context, id string) ([]byte, error) {
	return r.queries.GetLiveOperationBody(ctx, id)
}

func (r *Repository) ReplaceLiveBody(ctx context.Context, id string, body []byte) error {
	return store.RequireOne(r.queries.ReplaceLiveOperationBody(ctx, dbgen.ReplaceLiveOperationBodyParams{
		Body: body,
		ID:   id,
	}))
}
