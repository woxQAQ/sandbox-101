package recovery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// Recovery validates frozen workflows and publications for all store entrypoints.
// It owns no running service, backend, worker, or mutable store connection.
type Recovery struct{}

func NewRecovery() *Recovery {
	return &Recovery{}
}

func (r *Recovery) Validation() store.Validation {
	if r == nil {
		return store.Validation{}
	}
	return store.Validation{
		Publications: r.publications,
		Workflows:    r.workflows,
	}
}

func (r *Recovery) workflows(ctx context.Context, database *sql.DB, origin store.Origin) error {
	entries, err := operation.New(database).RecoverySteps(ctx)
	if err != nil {
		return err
	}
	for _, item := range entries {
		hash := sha256.Sum256(item.Input)
		if item.Action != "resource_link" && item.Key != "warm_claim" && (hex.EncodeToString(hash[:]) != item.Hash || !json.Valid(item.Input)) {
			return fmt.Errorf("invalid durable step encoding: %s/%s", item.OperationID, item.Key)
		}
		if err := validateWorkflowProof(ctx, database, origin, item); err != nil {
			return fmt.Errorf("durable step %s/%s requires recovery: %w", item.OperationID, item.Key, err)
		}
	}
	return nil
}

func (r *Recovery) publications(ctx context.Context, database *sql.DB) error {
	return operation.New(database).ValidateHistory(ctx)
}
