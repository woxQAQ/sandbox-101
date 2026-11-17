package machine

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// admitCapturePublication joins the catalog and permanent owner in the caller's transaction.
func admitCapturePublication(ctx context.Context, tx *sql.Tx, record SnapshotRecord) error {
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	queries := dbgen.New(tx)
	if err = store.RequireOne(queries.InsertSnapshot(ctx, dbgen.InsertSnapshotParams{
		ID:         record.ID,
		SourceVmID: record.SourceVM,
		Body:       body,
	})); err != nil {
		return err
	}
	return store.RequireOne(queries.InsertCaptureOwner(ctx, dbgen.InsertCaptureOwnerParams{
		SnapshotID:        record.ID,
		TenantID:          record.TenantID,
		SourceOperationID: record.OperationID,
	}))
}
