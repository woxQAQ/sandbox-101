package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// OperationDelivery confines sandbox capabilities and derives bearer tokens
// while reading the original frozen operation view.
type OperationDelivery struct {
	database       *store.Database
	operations     *operation.Operations
	authentication *authorization.Authentication
}

func NewOperationDelivery(database *store.Database, operations *operation.Operations, authentication *authorization.Authentication) *OperationDelivery {
	return &OperationDelivery{
		database:       database,
		operations:     operations,
		authentication: authentication,
	}
}

// PublicOperation authorizes delivery of the original business view. Status and
// publication share one read snapshot; bearer tokens are derived only on delivery.
func (s *OperationDelivery) PublicOperation(ctx context.Context, principal authorization.Principal, id string) (types.Operation, error) {
	if _, err := s.operations.Operation(ctx, principal, id); err != nil {
		return types.Operation{}, err
	}
	tx, err := s.database.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return types.Operation{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	status, err := operation.New(tx).PublicStatus(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return types.Operation{}, fmt.Errorf("%w: operation not found", errs.ErrNotFound)
		}
		return types.Operation{}, err
	}
	var out types.Operation
	if status != "succeeded" && status != "failed" {
		out, _, err = operation.ProjectionHeader(ctx, tx, id)
	} else {
		out, err = operation.LoadPublication(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return out, fmt.Errorf("%w: public terminal result is missing", errs.ErrManualRecovery)
		}
	}
	if err != nil {
		return out, err
	}
	if principal.SandboxID != "" && (out.Command.SandboxID != principal.SandboxID || (out.Command.Action != "renew" && out.Command.Action != "release")) {
		return types.Operation{}, fmt.Errorf("%w: operation not found", errs.ErrNotFound)
	}
	if out.ID != id || out.Status != status {
		return out, fmt.Errorf("%w: public result identity changed", errs.ErrManualRecovery)
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
			out.Result.Tokens[sandbox.ID] = s.authentication.SandboxToken(sandbox.TenantID, sandbox.ID)
		}
	}
	return out, nil
}
