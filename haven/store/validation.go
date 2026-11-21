package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const Format = storeFormat

var (
	ErrRecovery = errStoreRecovery
	ErrConflict = errStoreConflict
)

// Origin supplies immutable startup context without granting access to the
// writer, node key, runtime effects, or application services.
type Origin struct {
	Directory string
	Reviewed  bool
}

// Validation supplies application-owned proof checks. Both callbacks are
// required even for offline entrypoints; a missing validator cannot silently
// turn an ordinary open into metadata-only acceptance.
type Validation struct {
	Publications func(context.Context, *sql.DB) error
	Workflows    func(context.Context, *sql.DB, Origin) error
}

func (v Validation) check() error {
	if v.Publications == nil || v.Workflows == nil {
		return fmt.Errorf("%w: publication and workflow validators are required", ErrRecovery)
	}
	return nil
}

// HasForeignKeyViolations checks normalized references without changing store
// state. A query or row error remains unknown and must block collection.
func HasForeignKeyViolations(ctx context.Context, database DBTX) (bool, error) {
	rows, err := database.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return false, err
	}
	invalid := rows.Next()
	return invalid, errors.Join(rows.Err(), rows.Close())
}

func (state *State) Database() *Database {
	return state.db
}

func (state *State) Directory() string {
	return state.directory
}

func (state *State) Identity() Identity {
	return state.identity
}

func (state *State) Reviewed() bool {
	return state.reviewed
}
