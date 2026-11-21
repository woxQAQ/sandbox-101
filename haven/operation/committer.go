package operation

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// Projector reads the business view in the caller's transaction. Implementations
// must not write, commit, dispatch effects, or retain application state.
type Projector func(context.Context, store.DBTX, string) (types.Operation, error)

// Completion names one operation the calling workflow verified and explicitly
// finishes inside its own transaction, together with the business result the
// operation must freeze.
type Completion struct {
	ID     string
	Result Result
	Failed bool
	// AdoptResult keeps the live accumulated body as the frozen result instead
	// of replacing it.
	AdoptResult bool
	// Frozen marks an outcome the admission already stored as terminal;
	// Complete only verifies it before freezing the publication.
	Frozen bool
}

// Committer writes terminal state, completion time and the frozen publication
// for the operations one transaction explicitly completes. Checkpoint custody
// remains with the caller, and the transaction owner commits once every step
// succeeded; any failure rolls back the whole round.
type Committer struct {
	project Projector
}

func NewCommitter(project Projector) *Committer {
	return &Committer{
		project: project,
	}
}

// Complete finishes each named operation with its declared outcome and freezes
// publications for the eligible ones in completion order. A parent whose
// business view depends on child publications must be completed after those
// children were frozen by their own transactions. It never commits: the caller
// commits its transaction once this and every other step succeeded.
func (committer *Committer) Complete(tx *sql.Tx, completions ...Completion) error {
	if committer == nil || committer.project == nil {
		return errors.New("publication projector is required")
	}
	ctx := context.Background()
	repository := New(tx)
	for _, completion := range completions {
		if completion.Frozen {
			status, body, err := repository.ResultState(ctx, completion.ID)
			if err != nil {
				return err
			}
			if !Terminal(status) || (status == "failed") != completion.Failed {
				return fmt.Errorf("%w: admission outcome changed before completion %s", errs.ErrManualRecovery, completion.ID)
			}
			if !completion.AdoptResult && !bytes.Equal(body, Encode(completion.Result)) {
				return fmt.Errorf("%w: admission result changed before completion %s", errs.ErrManualRecovery, completion.ID)
			}
			continue
		}
		var err error
		switch {
		case completion.Failed:
			err = repository.FailResult(ctx, completion.ID, Encode(completion.Result))
		case completion.AdoptResult:
			err = repository.Complete(ctx, completion.ID)
		default:
			err = repository.CompleteResult(ctx, completion.ID, Encode(completion.Result))
		}
		if err != nil {
			return err
		}
	}
	for _, completion := range completions {
		eligible, err := repository.PublicationEligible(ctx, completion.ID)
		if err != nil {
			return err
		}
		if !eligible {
			continue
		}
		view, err := committer.project(ctx, tx, completion.ID)
		if err != nil {
			return err
		}
		if err = repository.Publish(ctx, completion.ID, Encode(view)); err != nil {
			return err
		}
	}
	return nil
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
	if err := store.RequireOne(r.queries.InsertOperation(ctx, dbgen.InsertOperationParams{
		ID:          admission.ID,
		ScopeKind:   admission.ScopeKind,
		ScopeID:     admission.ScopeID,
		IdemKey:     admission.Key,
		RequestHash: admission.Hash,
		Request:     admission.Request,
		Status:      admission.Status,
		Body:        admission.Body,
	})); err != nil {
		return err
	}
	now := clockNow()
	finished := ""
	if Terminal(admission.Status) {
		finished = now
	}
	return store.RequireOne(r.queries.InsertOperationClock(ctx, dbgen.InsertOperationClockParams{
		OperationID: admission.ID,
		CreatedAt:   now,
		FinishedAt:  finished,
	}))
}

func (r *Repository) InsertCredential(ctx context.Context, operationID, mac string) error {
	return store.RequireOne(r.queries.InsertCredential(ctx, dbgen.InsertCredentialParams{
		OperationID: operationID,
		RequestMac:  mac,
	}))
}

// PublicationEligible reports whether a terminal operation freezes a business
// view: tenant-admitted operations carry a credential, and activation
// operations carry their activate step. Other operations stay internal.
func (r *Repository) PublicationEligible(ctx context.Context, operationID string) (bool, error) {
	if _, err := r.queries.GetCredentialMAC(ctx, operationID); err == nil {
		return true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	activate, err := r.queries.HasActivateStep(ctx, operationID)
	return activate != 0, err
}

func (r *Repository) CompleteResult(ctx context.Context, operationID string, body []byte) error {
	if err := store.RequireOne(r.queries.CompleteResult(ctx, dbgen.CompleteResultParams{
		ID:   operationID,
		Body: body,
	})); err != nil {
		return err
	}
	return r.finishClock(ctx, operationID)
}

func (r *Repository) CompletePending(ctx context.Context, operationID string) error {
	if err := store.RequireOne(r.queries.CompletePendingOperation(ctx, operationID)); err != nil {
		return err
	}
	return r.finishClock(ctx, operationID)
}

func (r *Repository) Complete(ctx context.Context, operationID string) error {
	if err := store.RequireOne(r.queries.CompleteLiveOperation(ctx, operationID)); err != nil {
		return err
	}
	return r.finishClock(ctx, operationID)
}

// clockNow stamps the operation clock as UTC RFC3339 with millisecond
// precision; readers parse it with RFC3339Nano.
func clockNow() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

func (r *Repository) finishClock(ctx context.Context, operationID string) error {
	return store.RequireOne(r.queries.FinishOperationClock(ctx, dbgen.FinishOperationClockParams{
		FinishedAt:  clockNow(),
		OperationID: operationID,
	}))
}
