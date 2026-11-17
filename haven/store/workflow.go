package store

import (
	"context"
	"database/sql"
)

type workflowDatabase interface {
	DBTX

	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

// workflowPersistence borrows query, transaction and checkpoint capabilities
// without exposing database close, key ownership or writer-lock release.
type Workflow struct {
	Database   workflowDatabase
	Directory  string
	Reviewed   bool
	Checkpoint func() error
}
