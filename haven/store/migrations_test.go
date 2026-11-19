package store_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/woxQAQ/haven/store"
)

// The empty-store transaction includes both the baseline and its identity.
func TestSQLiteMigrationTransaction(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	denied := errors.New("initialization failed after identity write")
	for _, commit := range []bool{
		false,
		true,
	} {
		err = store.Migrate(ctx, db, func(tx *sql.Tx) error {
			if _, err := tx.Exec("INSERT INTO store_metadata VALUES(1,'fixture',?,'identity','key-hash',0)", store.Version); err != nil {
				return err
			}
			if !commit {
				return denied
			}
			return nil
		})
		if commit && err != nil || !commit && !errors.Is(err, denied) {
			t.Fatalf("initialization result: %v", err)
		}
		var version, application, tables int
		if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
			t.Fatal(err)
		}
		if err = db.QueryRow("PRAGMA application_id").Scan(&application); err != nil {
			t.Fatal(err)
		}
		if err = db.QueryRow("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name IN ('operations','store_metadata','goose_db_version')").Scan(&tables); err != nil {
			t.Fatal(err)
		}
		if !commit && (version != 0 || application != 0 || tables != 0) {
			t.Fatalf("rolled-back initialization survived: version=%d application=%d tables=%d", version, application, tables)
		}
		if commit && (version != 0 || application != store.ApplicationID || tables != 3) {
			t.Fatalf("committed initialization incomplete: version=%d application=%d tables=%d", version, application, tables)
		}
	}
	if _, err = db.Exec("INSERT INTO tenants VALUES('owner','token-hash',0,0)"); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []any{nil, -1, 0.5, "invalid"} {
		for _, column := range []string{"max_sandboxes", "max_storage_mib"} {
			if _, err := db.Exec("UPDATE tenants SET "+column+"=? WHERE id='owner'", invalid); err == nil {
				t.Fatalf("%s accepted invalid budget %v", column, invalid)
			}
		}
	}
	if err = store.Migrate(ctx, db, func(*sql.Tx) error {
		t.Fatal("repeated migration ran initialization")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM tenants WHERE id='owner'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("repeated migration changed existing facts: count=%d err=%v", count, err)
	}
	if err = db.QueryRow("SELECT COUNT(*) FROM goose_db_version WHERE version_id=? AND is_applied", store.Version).Scan(&count); err != nil || count != 1 {
		t.Fatalf("baseline was not recorded exactly once: count=%d err=%v", count, err)
	}
}

func TestSQLiteMigrationRejectsUnsupportedOrigins(t *testing.T) {
	for name, seed := range map[string]string{
		"unversioned foreign":     "CREATE TABLE foreign_state(id TEXT)",
		"legacy":                  "PRAGMA application_id=1212241486; PRAGMA user_version=8",
		"future":                  "PRAGMA application_id=1212241486; PRAGMA user_version=999",
		"previous v9":             "PRAGMA application_id=1212241486; PRAGMA user_version=9",
		"foreign application":     "PRAGMA application_id=42",
		"missing goose history":   "PRAGMA application_id=1212241486",
		"malformed goose history": "CREATE TABLE goose_db_version(unrelated TEXT)",
	} {
		t.Run(name, func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			if _, err = db.Exec(seed); err != nil {
				t.Fatal(err)
			}
			if err = store.Migrate(context.Background(), db, nil); err == nil {
				t.Fatal("unsupported migration origin accepted")
			}
			var tables int
			if err = db.QueryRow("SELECT COUNT(*) FROM sqlite_schema WHERE name='operations'").Scan(&tables); err != nil || tables != 0 {
				t.Fatalf("rejected origin received control schema: tables=%d err=%v", tables, err)
			}
		})
	}
}
