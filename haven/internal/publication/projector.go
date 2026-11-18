package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

// projectOperation builds a business view in the caller's SQLite snapshot.
// Terminal views are frozen before commit; later queries read that publication.
// Diagnostics, HTTP errors and bearer tokens are outside the persisted view.
func Project(ctx context.Context, q store.DBTX, id string) (types.Operation, error) {
	out, result, err := operation.ProjectionHeader(ctx, q, id)
	if err != nil {
		return out, err
	}
	if !operation.Terminal(out.Status) {
		return out, nil
	}
	if out.Command.Action == "fork" && out.Status == "succeeded" {
		group, err := sandbox.LoadFork(ctx, q, id)
		if err != nil {
			return out, err
		}
		for index, childID := range group.State.Children {
			child, err := operation.LoadPublication(ctx, q, childID)
			if err != nil {
				return out, err
			}
			if child.Status != "succeeded" || child.Result.Sandbox == nil || child.Result.Sandbox.ID != group.Input.Targets[index].ID || child.Result.Sandbox.TenantID != group.Input.OwnerTenantID {
				return out, fmt.Errorf("%w: fork historical branch delivery changed", errs.ErrManualRecovery)
			}
			out.Result.Sandboxes = append(out.Result.Sandboxes, *child.Result.Sandbox)
			out.ChargeTenantID = child.Result.Sandbox.TenantID
		}
	} else if out.Command.Action != "checkpoint" && out.Command.Action != operation.ActionCheckpointDeletePublic {
		for _, sandboxID := range out.IDs {
			value, err := sandbox.NewCatalog(q).Get(ctx, authorization.Principal{
				Root: true,
			}, sandboxID)
			if err == nil {
				view := value.Public()
				if out.Command.Action == "fork" {
					out.Result.Sandboxes = append(out.Result.Sandboxes, view)
				} else {
					out.Result.Sandbox = &view
				}
				out.ChargeTenantID = view.TenantID
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, errs.ErrNotFound) {
				return out, err
			}
		}
	}
	if result.CheckpointID != "" {
		value, lookup := sandbox.LoadCheckpoint(ctx, q, result.CheckpointID)
		err = lookup
		if err == nil {
			view := value.Public()
			out.Result.Checkpoint = &view
			out.ChargeTenantID = view.TenantID
		} else if !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
	}
	if result.TemplateID != "" {
		value, lookup := supply.LoadTemplate(ctx, q, result.TemplateID)
		err = lookup
		if err == nil {
			view := value.Public()
			out.Result.Template = &view
			out.ChargeTenantID = view.TenantID
		} else if !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
	}
	return out, nil
}
