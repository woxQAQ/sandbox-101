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
	return view, nil
}

// PublicOperation authorizes delivery of the original business view. Status and
// publication share one read snapshot; bearer tokens are derived only on delivery.
