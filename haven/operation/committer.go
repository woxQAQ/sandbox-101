package operation

import (
	"context"
	"database/sql"
	"errors"

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
