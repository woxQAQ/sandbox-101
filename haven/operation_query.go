package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/types"
)

// PublicOperation authorizes delivery of an operation's business view. HTTP
// adapters format failures; bearer tokens are derived only for this delivery.
func (service *Service) PublicOperation(ctx context.Context, principal Principal, id string) (types.Operation, error) {
	if _, err := service.Operation(ctx, principal, id); err != nil {
		return types.Operation{}, err
	}
	// Status and publication share one read snapshot so concurrent completion
	// cannot look like missing history.
	tx, err := service.store.db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return types.Operation{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var status string
	if err = tx.QueryRowContext(ctx, "SELECT o.status FROM operations o JOIN operation_credentials c ON c.operation_id=o.id WHERE o.id=? AND o.scope_kind='tenant'", id).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return types.Operation{}, fmt.Errorf("%w: operation not found", ErrNotFound)
		}
		return types.Operation{}, err
	}
	var out types.Operation
	if !terminal(status) {
		out, err = projectOperation(ctx, tx, id)
	} else {
		out, err = loadPublication(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return out, fmt.Errorf("%w: public terminal result is missing", ErrManualRecovery)
		}
	}
	if err != nil {
		return out, err
	}
	if principal.SandboxID != "" && (out.Command.SandboxID != principal.SandboxID || (out.Command.Action != "renew" && out.Command.Action != "release")) {
		return types.Operation{}, fmt.Errorf("%w: operation not found", ErrNotFound)
	}
	if out.ID != id || out.Status != status {
		return out, fmt.Errorf("%w: public result identity changed", ErrManualRecovery)
	}
	if out.Status == "succeeded" {
		sandboxes := out.Result.Sandboxes
		if out.Result.Sandbox != nil {
			sandboxes = append(append([]types.Sandbox(nil), sandboxes...), *out.Result.Sandbox)
		}
		for _, sandbox := range sandboxes {
			if sandbox.State == "terminated" {
				continue
			}
			if out.Result.Tokens == nil {
				out.Result.Tokens = make(map[string]string)
			}
			out.Result.Tokens[sandbox.ID] = service.sandboxToken(sandboxRecord{
				TenantID: sandbox.TenantID,
				ID:       sandbox.ID,
			})
		}
	}
	return out, nil
}
