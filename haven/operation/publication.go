package operation

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// ProjectionHeader reads the accepted command and result in the caller's
// snapshot. Completed domain views are added by the publication coordinator.
func ProjectionHeader(ctx context.Context, query store.DBTX, id string) (types.Operation, Result, error) {
	var out types.Operation
	var result Result
	repository := New(query)
	source, err := repository.Projection(ctx, id)
	if err != nil {
		return out, result, err
	}
	out.ID, out.Key, out.TenantID, out.Status = source.ID, source.Key, source.ScopeID, source.Status
	if source.ScopeKind != "tenant" {
		return out, result, fmt.Errorf("%w: operation not found", errs.ErrNotFound)
	}
	command, err := AcceptedCommand(source.Request)
	if err != nil {
		return out, result, err
	}
	out.Command = command
	if err = json.Unmarshal(source.Body, &result); err != nil {
		return out, result, err
	}
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
	body, err := New(query).Publication(ctx, id)
	if err != nil {
		return view, err
	}
	if err := json.Unmarshal(body, &view); err != nil {
		return view, err
	}
	if view.ID != id || view.Result.OperationID != id || (view.Status != "succeeded" && view.Status != "failed") || view.Result.Tokens != nil || view.Error != nil {
		return view, fmt.Errorf("%w: frozen delivery identity or transport fields changed", errs.ErrManualRecovery)
	}
	return view, ValidateDelivery(view)
}

// PublicOperation authorizes delivery of the original business view. Status and
// publication share one read snapshot; bearer tokens are derived only on delivery.

// PublicationProof contains only frozen operation identity and delivery. Recovery
// validates this history without reconstructing results from current resources.
type PublicationProof struct {
	ID          string
	Key         string
	ScopeKind   string
	ScopeID     string
	Status      string
	CreatedAt   string
	FinishedAt  string
	Publication []byte
	Request     []byte
	Result      []byte
}

func (r *Repository) PublicationProofs(ctx context.Context) ([]PublicationProof, error) {
	rows, err := r.queries.ListPublicationProofs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]PublicationProof, 0, len(rows))
	for _, row := range rows {
		out = append(out, PublicationProof{
			ID:          row.ID,
			Key:         row.IdemKey,
			ScopeKind:   row.ScopeKind,
			ScopeID:     row.ScopeID,
			Status:      row.Status,
			CreatedAt:   row.CreatedAt,
			FinishedAt:  row.FinishedAt,
			Publication: row.Publication,
			Request:     row.Request,
			Result:      row.Result,
		})
	}
	return out, nil
}

func (r *Repository) MissingPublications(ctx context.Context) (int64, error) {
	return r.queries.CountMissingPublications(ctx)
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
			Body:             row.Body,
		})
	}
	return out, nil
}

func (r *Repository) ResultState(ctx context.Context, id string) (status string, body []byte, err error) {
	row, err := r.queries.GetResultState(ctx, id)
	return row.Status, row.Body, err
}
