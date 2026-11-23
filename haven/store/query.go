package store

import (
	"context"
	"database/sql"
	"fmt"
)

// DBTX is the connection used by domain queries. A workflow passes its existing
// transaction to every participant; repositories never open another transaction.
type DBTX interface {
	// PrepareContext prepares a SQL statement on the current connection or transaction.
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	// ExecContext executes a statement and returns its affected-row result.
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	// QueryContext opens a result set in the caller's database scope.
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	// QueryRowContext returns a single-row result whose error is reported by Scan.
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// RequireOne preserves conditional responsibility writes, including triggers
// that ignore a write without returning an SQL error.
func RequireOne(count int64, err error) error {
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("required responsibility write affected %d rows", count)
	}
	return nil
}

type WorkflowDatabase interface {
	DBTX

	// BeginTx opens a transaction with the requested isolation and access mode.
	BeginTx(ctx context.Context, options *sql.TxOptions) (*sql.Tx, error)
}

// ReadSnapshot shares an existing transaction or opens a consistent read for related rows.
func ReadSnapshot(ctx context.Context, query DBTX, read func(*sql.Tx) error) error {
	if tx, ok := query.(*sql.Tx); ok {
		return read(tx)
	}
	db, ok := query.(WorkflowDatabase)
	if !ok {
		return fmt.Errorf("related records require a transaction-capable connection")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	return read(tx)
}

// workflowPersistence borrows query, transaction and checkpoint capabilities
// without exposing database close, key ownership or writer-lock release.
type Workflow struct {
	Database   WorkflowDatabase
	Directory  string
	Reviewed   bool
	Checkpoint func() error
}
