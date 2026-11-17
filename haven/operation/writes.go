package operation

import (
	"context"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// Admission fixes an operation's scoped request identity and initial result.
// Request and Body are the original encoded bytes supplied by its workflow.
type Admission struct {
	ID        string
	ScopeKind string
	ScopeID   string
	Key       string
	Hash      string
	Status    string
	Request   []byte
	Body      []byte
}

func (r *Repository) Insert(ctx context.Context, admission Admission) error {
	return store.RequireOne(r.queries.InsertOperation(ctx, dbgen.InsertOperationParams{
		ID:          admission.ID,
		ScopeKind:   admission.ScopeKind,
		ScopeID:     admission.ScopeID,
		IdemKey:     admission.Key,
		RequestHash: admission.Hash,
		Request:     admission.Request,
		Status:      admission.Status,
		Body:        admission.Body,
	}))
}

func (r *Repository) InsertCredential(ctx context.Context, operationID, mac string) error {
	return store.RequireOne(r.queries.InsertCredential(ctx, dbgen.InsertCredentialParams{
		OperationID: operationID,
		RequestMac:  mac,
	}))
}

func (r *Repository) CompleteResult(ctx context.Context, operationID string, body []byte) error {
	return store.RequireOne(r.queries.CompleteResult(ctx, dbgen.CompleteResultParams{
		ID:   operationID,
		Body: body,
	}))
}

func (r *Repository) CompletePending(ctx context.Context, operationID string) error {
	return store.RequireOne(r.queries.CompletePendingOperation(ctx, operationID))
}

func (r *Repository) Complete(ctx context.Context, operationID string) error {
	return store.RequireOne(r.queries.CompleteLiveOperation(ctx, operationID))
}
