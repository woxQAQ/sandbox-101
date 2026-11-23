package operation

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// ProjectionHeader reads the accepted command and result in the caller's
// snapshot. Completed domain views are added by the publication coordinator.
func ProjectionHeader(ctx context.Context, query store.DBTX, id string) (types.Operation, Result, error) {
	var out types.Operation
	var result Result
	repository := New(query)
	source, err := repository.LoadProjectionSource(ctx, id)
	if err != nil {
		return out, result, err
	}
	out.ID, out.Key, out.TenantID, out.Status = source.ID, source.Key, source.ScopeID, source.Status
	if source.ScopeKind != ScopeTenant {
		return out, result, fmt.Errorf("%w: operation not found", errs.ErrNotFound)
	}
	command, err := AcceptedCommand(source.Request)
	if err != nil {
		return out, result, err
	}
	out.Command = command
	result = source.Result
	out.CreatedAt, out.FinishedAt, err = repository.Times(ctx, id)
	if err != nil {
		return out, result, err
	}
	out.SnapshotID = result.SnapshotID
	out.IDs = append([]string(nil), result.SandboxIDs...)
	if len(out.IDs) == 0 && result.SandboxID != "" {
		out.IDs = []string{
			result.SandboxID,
		}
	}
	out.Result.OperationID = id
	out.Step = "pending"
	switch out.Status {
	case "succeeded":
		out.Step = "done"
	case "failed":
		out.Step = "failed"
	}
	return out, result, nil
}

// ValidateDelivery checks the object promised by a successful business action
// using only the frozen view. Failed operations may carry partial results.
func ValidateDelivery(view types.Operation) error {
	if view.Status != "succeeded" {
		return nil
	}
	switch view.Command.Action {
	case "claim", "fork_branch", "renew", "configure", "pause", "resume", "rollback", "release":
		if len(view.IDs) != 1 || view.Result.Sandbox == nil || view.Result.Sandbox.ID != view.IDs[0] {
			return fmt.Errorf("%w: missing sandbox delivery for %s", errs.ErrManualRecovery, view.ID)
		}
	case "fork":
		if len(view.IDs) == 0 || len(view.Result.Sandboxes) != len(view.IDs) {
			return fmt.Errorf("%w: incomplete fork delivery for %s", errs.ErrManualRecovery, view.ID)
		}
		for index, id := range view.IDs {
			if view.Result.Sandboxes[index].ID != id {
				return fmt.Errorf("%w: fork delivery order changed for %s", errs.ErrManualRecovery, view.ID)
			}
		}
	case "checkpoint":
		if view.Result.Checkpoint == nil {
			return fmt.Errorf("%w: missing checkpoint delivery for %s", errs.ErrManualRecovery, view.ID)
		}
	case "template_put", "template_build", "refill":
		if view.Result.Template == nil {
			return fmt.Errorf("%w: missing template delivery for %s", errs.ErrManualRecovery, view.ID)
		}
	}
	return nil
}

// LoadPublication reads the original delivery. It never rebuilds a terminal
// view from current resources and rejects persisted transport fields or tokens.
func LoadPublication(ctx context.Context, query store.DBTX, id string) (types.Operation, error) {
	var view types.Operation
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		var err error
		view, err = readPublication(ctx, tx, id)
		return err
	})
	return view, err
}

func readPublication(ctx context.Context, tx *sql.Tx, id string) (types.Operation, error) {
	q := dbgen.New(tx)
	record, err := q.GetPublication(ctx, id)
	if err != nil {
		return types.Operation{}, err
	}
	view, result, err := ProjectionHeader(ctx, tx, id)
	if err != nil {
		return view, err
	}
	view.ChargeTenantID = record.ChargeTenantID
	if err = readPublishedObjects(ctx, q, record, &view); err != nil {
		return view, err
	}
	if !Terminal(view.Status) || Digest(Encode(view)) != record.ResponseHash {
		return view, fmt.Errorf("%w: frozen delivery differs from accepted operation %s", errs.ErrManualRecovery, id)
	}
	if err = ValidateDelivery(view); err != nil {
		return view, err
	}
	if c := view.Result.Checkpoint; c != nil && c.ID != result.CheckpointID {
		return view, fmt.Errorf("frozen checkpoint identity changed")
	}
	if t := view.Result.Template; t != nil && t.ID != result.TemplateID {
		return view, fmt.Errorf("frozen template identity changed")
	}
	return view, nil
}

// ValidateHistory checks accepted requests, including private work that can resume without a publication.
func (r *Repository) ValidateHistory(ctx context.Context) error {
	return store.ReadSnapshot(ctx, r.database, func(tx *sql.Tx) error {
		q := dbgen.New(tx)
		missing, err := q.CountMissingPublications(ctx)
		if err != nil {
			return err
		}
		if missing != 0 {
			return fmt.Errorf("terminal public operation lacks frozen result")
		}
		unpublished, err := q.ListUnpublishedOperations(ctx)
		if err != nil {
			return err
		}
		for _, row := range unpublished {
			if _, err = readRequest(ctx, q, row); err != nil {
				return fmt.Errorf("invalid request for %s: %w", row.ID, err)
			}
		}
		ids, err := q.ListPublicationIDs(ctx)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if _, err = readPublication(ctx, tx, id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *Repository) RecoverySteps(ctx context.Context) ([]Step, error) {
	rows, err := r.queries.ListStepsForRecovery(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Step, 0, len(rows))
	for _, row := range rows {
		out = append(out, Step{
			OperationID:      row.OperationID,
			Key:              row.StepKey,
			Action:           row.Action,
			Status:           row.Status,
			Hash:             row.InputHash,
			Candidate:        row.CandidateResourceID,
			ExpectedInstance: row.ExpectedInstance,
			Input:            row.Input,
		})
	}
	return out, nil
}

func (r *Repository) StatusAndResult(ctx context.Context, id string) (string, Result, error) {
	row, err := r.LoadProjectionSource(ctx, id)
	return row.Status, row.Result, err
}
