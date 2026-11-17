package store

import (
	"context"
	"database/sql"
	"fmt"
)

// DBTX is the connection used by domain queries. A workflow passes its existing
// transaction to every participant; repositories never open another transaction.
type DBTX interface {
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
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
