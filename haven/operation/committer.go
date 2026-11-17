package operation

import (
	"context"
	"database/sql"
	"errors"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// Projector reads the business view in the caller's transaction. Implementations
// must not write, commit, dispatch effects, or retain application state.
type Projector func(context.Context, store.DBTX, string) (types.Operation, error)

// Committer freezes every newly terminal publication before committing the
// original workflow transaction. Checkpoint custody remains with the caller.
type Committer struct {
	project Projector
}

func NewCommitter(project Projector) *Committer {
	return &Committer{
		project: project,
	}
}

func (committer *Committer) Commit(tx *sql.Tx) error {
	if committer == nil || committer.project == nil {
		return errors.New("publication projector is required")
	}
	ctx := context.Background()
	repository := New(tx)
	ids, err := repository.UnpublishedTerminals(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		view, err := committer.project(ctx, tx, id)
		if err != nil {
			return err
		}
		if err = repository.Publish(ctx, id, Encode(view)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

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
