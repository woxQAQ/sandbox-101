package operation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// resultTransaction keeps ordered fork targets and scalar results in the same commit.
func (r *Repository) resultTransaction(ctx context.Context, write func(*dbgen.Queries) error) error {
	if _, ok := r.database.(*sql.Tx); ok {
		return write(r.queries)
	}
	db, ok := r.database.(store.WorkflowDatabase)
	if !ok {
		return fmt.Errorf("operation result requires a transaction")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = write(dbgen.New(tx)); err != nil {
		return err
	}
	return tx.Commit()
}

func insertResultSandboxes(ctx context.Context, q *dbgen.Queries, id string, targets []string) error {
	for index, target := range targets {
		if err := store.RequireOne(q.InsertResultSandbox(ctx, dbgen.InsertResultSandboxParams{
			OperationID: id,
			Position:    int64(index),
			SandboxID:   target,
		})); err != nil {
			return err
		}
	}
	return nil
}

func scalarResult(row dbgen.Operation) Result {
	return Result{
		SandboxID:     row.ResultSandboxID,
		VMID:          row.ResultVmID,
		ImageDigest:   row.ResultImageDigest,
		TemplateID:    row.ResultTemplateID,
		CheckpointID:  row.ResultCheckpointID,
		SnapshotID:    row.ResultSnapshotID,
		ResourceReady: row.ResourceReady,
		Error:         row.ResultError,
	}
}

func (r *Repository) readOperation(ctx context.Context, lookup func(*dbgen.Queries) (dbgen.Operation, error)) (ProjectionSource, error) {
	var source ProjectionSource
	err := store.ReadSnapshot(ctx, r.database, func(tx *sql.Tx) error {
		q := dbgen.New(tx)
		row, err := lookup(q)
		if err != nil {
			return err
		}
		result := scalarResult(row)
		result.SandboxIDs, err = q.ListResultSandboxes(ctx, row.ID)
		if err != nil {
			return err
		}
		request, err := readRequest(ctx, q, row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: operation %s has incomplete request", errs.ErrManualRecovery, row.ID)
			}
			return fmt.Errorf("read operation %s request: %w", row.ID, err)
		}
		source = ProjectionSource{
			ID:        row.ID,
			Key:       row.IdemKey,
			ScopeKind: row.ScopeKind,
			ScopeID:   row.ScopeID,
			Status:    row.Status,
			Request:   request,
			Result:    result,
		}
		return nil
	})
	return source, err
}

func (r *Repository) updateResult(ctx context.Context, id, status string, result Result) error {
	finished := ""
	if Terminal(status) {
		finished = clockNow()
	}
	return r.resultTransaction(ctx, func(q *dbgen.Queries) error {
		if err := store.RequireOne(q.UpdateLiveResult(ctx, dbgen.UpdateLiveResultParams{
			ID: id,
			NextStatus: sql.NullString{
				String: status,
				Valid:  status != "",
			},
			FinishedAt:         finished,
			ResultSandboxID:    result.SandboxID,
			ResultVmID:         result.VMID,
			ResultImageDigest:  result.ImageDigest,
			ResultTemplateID:   result.TemplateID,
			ResultCheckpointID: result.CheckpointID,
			ResultSnapshotID:   result.SnapshotID,
			ResourceReady:      result.ResourceReady,
			ResultError:        result.Error,
		})); err != nil {
			return err
		}
		if err := q.ClearResultSandboxes(ctx, id); err != nil {
			return err
		}
		return insertResultSandboxes(ctx, q, id, result.SandboxIDs)
	})
}
