// Package store owns SQLite migrations and persistence infrastructure.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

const (
	// Earlier control stores cannot be upgraded by normal startup.
	Version        = 9
	ApplicationID  = 0x4841564e
	migrationTable = "goose_db_version"
)

// The Go migration combines this schema with the new store's identity in one
// goose transaction. sqlc reads the same SQL, without a second schema copy.
//
//go:embed migrations/000009_baseline.up.sql
var baselineSQL string

// Migrate runs goose while the caller holds the store writer lock. Initialize
// participates in the baseline transaction, before goose records version 9.
// Existing stores require matching identity/version markers and goose history;
// normal startup never manufactures migration history for an old store.
func Migrate(ctx context.Context, db *sql.DB, initialize func(*sql.Tx) error) error {
	var current, application int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&application); err != nil {
		return err
	}
	if current != 0 || application != 0 {
		if application != ApplicationID || current != Version {
			return fmt.Errorf("unsupported store migration origin: application=%d version=%d", application, current)
		}
		return validateMigrationVersion(ctx, db, Version)
	}
	var objects, journals int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' AND name <> 'goose_db_version'").Scan(&objects); err != nil {
		return err
	}
	if objects != 0 {
		return errors.New("migration baseline requires an empty database")
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='goose_db_version'").Scan(&journals); err != nil {
		return err
	}
	// Goose may retain its version-zero journal after a rolled-back baseline.
	if journals != 0 {
		if err := validateMigrationVersion(ctx, db, 0); err != nil {
			return err
		}
	}
	baseline := goose.NewGoMigration(Version, &goose.GoFunc{
		RunTx: func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, baselineSQL); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id=%d; PRAGMA user_version=%d", ApplicationID, Version)); err != nil {
				return err
			}
			if initialize != nil {
				return initialize(tx)
			}
			return nil
		},
	}, &goose.GoFunc{RunTx: func(context.Context, *sql.Tx) error {
		return errors.New("store history cannot be downgraded")
	}})
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, nil,
		goose.WithDisableGlobalRegistry(true), goose.WithGoMigrations(baseline),
		goose.WithLogger(goose.NopLogger()))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
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
