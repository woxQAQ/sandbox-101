package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

// A fork temporarily shares the writer's open file description before CLOEXEC.
// Store.Close must release ownership even while that duplicate is still alive;
// application shutdown tests separately enforce that workers exit before Close.
func TestHavenWriterHandoffWithRetainedLockDescriptor(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	first, err := Open(directory, true, fixtureValidation())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	duplicate, err := unix.Dup(int(first.lock.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(duplicate)
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(directory, false, fixtureValidation())
	if err != nil {
		t.Fatalf("completed close retained writer ownership through another descriptor: %v", err)
	}
	defer next.Close()
	if other, err := Open(directory, false, fixtureValidation()); err == nil {
		_ = other.Close()
		t.Fatal("new writer failed to exclude another store opener")
	}
}

func TestStoreRequiresProofValidatorsBeforeTouchingState(t *testing.T) {
	for _, missing := range []string{
		"publications",
		"workflows",
		"both",
	} {
		t.Run(missing, func(t *testing.T) {
			validation := fixtureValidation()
			if missing != "workflows" {
				validation.Publications = nil
			}
			if missing != "publications" {
				validation.Workflows = nil
			}
			directory := filepath.Join(t.TempDir(), "state")
			if state, err := Open(directory, true, validation); !errors.Is(err, ErrRecovery) {
				if state != nil {
					_ = state.Close()
				}
				t.Fatalf("missing proof validator accepted: %v", err)
			}
			if _, err := os.Stat(directory); !os.IsNotExist(err) {
				t.Fatalf("invalid validation configuration touched state: %v", err)
			}
		})
	}
}

func TestStoreProofRejectionPrecedesWritableConnection(t *testing.T) {
	for _, phase := range []string{
		"publications",
		"workflows",
	} {
		t.Run(phase, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "state")
			state, err := Open(directory, true, fixtureValidation())
			if err != nil {
				t.Fatal(err)
			}
			if err = state.Close(); err != nil {
				t.Fatal(err)
			}
			before := stateFiles(t, directory)
			denied := errors.New("fixture rejected frozen proof")
			called := false
			reject := func(ctx context.Context, db *sql.DB) error {
				called = true
				if _, err := db.ExecContext(ctx, "UPDATE store_metadata SET epoch=epoch+1 WHERE singleton=1"); err == nil {
					t.Fatal("startup proof validator received a writable database")
				}
				return denied
			}
			validation := fixtureValidation()
			if phase == "publications" {
				validation.Publications = reject
			} else {
				validation.Workflows = func(ctx context.Context, db *sql.DB, origin Origin) error {
					if origin.Directory != directory || origin.Reviewed {
						t.Fatal("wrong immutable startup origin", origin)
					}
					return reject(ctx, db)
				}
			}
			if unexpected, err := Open(directory, false, validation); err == nil {
				_ = unexpected.Close()
				t.Fatal("rejected proof opened the writer")
			}
			if !called {
				t.Fatal("startup skipped the requested proof validator")
			}
			if after := stateFiles(t, directory); !reflect.DeepEqual(before, after) {
				t.Fatal("proof rejection changed the original state or SQLite sidecars")
			}
			next, err := Open(directory, false, fixtureValidation())
			if err != nil {
				t.Fatal("rejected open retained writer ownership", err)
			}
			if err = next.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStoreMigrationHistoryRejectionIsReadOnly(t *testing.T) {
	for name, damage := range map[string]string{
		"missing":       "DROP TABLE goose_db_version",
		"wrong version": "UPDATE goose_db_version SET version_id=99 WHERE version_id=9",
	} {
		t.Run(name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "state")
			state, err := Open(directory, true, fixtureValidation())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = state.db.Exec(damage); err != nil {
				t.Fatal(err)
			}
			if err = state.Close(); err != nil {
				t.Fatal(err)
			}
			before := stateFiles(t, directory)
			if opened, err := Open(directory, false, fixtureValidation()); err == nil {
				_ = opened.Close()
				t.Fatal("invalid migration history accepted")
			}
			if after := stateFiles(t, directory); !reflect.DeepEqual(before, after) {
				t.Fatal("rejected migration history changed original state")
			}
		})
	}
}

// These fixtures inject structural-only proof checks explicitly. Production
// composition supplies all application-owned workflow and publication checks.
func fixtureValidation() Validation {
	return Validation{
		Publications: func(context.Context, *sql.DB) error {
			return nil
		},
		Workflows: func(context.Context, *sql.DB, Origin) error {
			return nil
		},
	}
}

type stateFile struct {
	Mode os.FileMode
	Hash [32]byte
}

func stateFiles(t *testing.T, directory string) map[string]stateFile {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]stateFile)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		body, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[entry.Name()] = stateFile{
			Mode: info.Mode(),
			Hash: sha256.Sum256(body),
		}
	}
	return out
}
