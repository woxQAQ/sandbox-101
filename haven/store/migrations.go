// Package store owns the SQLite schema and persistence infrastructure.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"

	"github.com/pressly/goose/v3/database"
)

const (
	// Version stays zero while Haven is unpublished. Schema changes edit the baseline.
	Version        = 0
	ApplicationID  = 0x4841564e
	migrationTable = "goose_db_version"
)

//go:embed migrations/000000_baseline.up.sql
var baselineSQL string

// Migrate installs the development baseline under the caller's writer lock.
// The application ID distinguishes initialized stores from empty SQLite files,
// both of which have user_version zero.
func Migrate(ctx context.Context, db *sql.DB, initialize func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var current, application int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA application_id").Scan(&application); err != nil {
		return err
	}
	if current != Version || (application != 0 && application != ApplicationID) {
		return fmt.Errorf("unsupported store migration origin: application=%d version=%d", application, current)
	}
	if application == ApplicationID {
		return validateMigrationVersion(ctx, tx, Version)
	}
	var objects int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").Scan(&objects); err != nil {
		return err
	}
	if objects != 0 {
		return errors.New("store baseline requires an empty database")
	}
	if _, err := tx.ExecContext(ctx, baselineSQL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id=%d", ApplicationID)); err != nil {
		return err
	}
	journal, err := database.NewStore(database.DialectSQLite3, migrationTable)
	if err != nil {
		return err
	}
	if err := journal.CreateVersionTable(ctx, tx); err != nil {
		return err
	}
	if err := journal.Insert(ctx, tx, database.InsertRequest{
		Version: Version,
	}); err != nil {
		return err
	}
	if initialize != nil {
		if err := initialize(tx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func validateMigrationVersion(ctx context.Context, query DBTX, expected int64) error {
	journal, err := database.NewStore(database.DialectSQLite3, migrationTable)
	if err != nil {
		return err
	}
	version, err := journal.GetLatestVersion(ctx, query)
	if err != nil {
		return fmt.Errorf("read goose migration history: %w", err)
	}
	if version != expected {
		return fmt.Errorf("goose migration version %d differs from store version %d", version, expected)
	}
	return nil
}
