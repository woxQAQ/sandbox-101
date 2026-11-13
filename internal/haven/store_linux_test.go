package haven

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

func TestHavenStoreExclusiveOwner(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	first, err := openStore(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.close() })
	if second, err := openStore(directory, false); !errors.Is(err, errStoreConflict) {
		if second != nil {
			_ = second.close()
		}
		t.Fatalf("second writer accepted: %v", err)
	}
	if first.db.Stats().MaxOpenConnections != 1 {
		t.Fatal("store does not have one writer connection")
	}
	var journal string
	var synchronous, foreignKeys int
	if err := first.db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("journal mode=%s: %v", journal, err)
	}
	if err := first.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil || synchronous != 2 {
		t.Fatalf("synchronous=%d: %v", synchronous, err)
	}
	if err := first.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("foreign_keys=%d: %v", foreignKeys, err)
	}
	transaction, err := first.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = transaction.Exec("INSERT INTO tenants VALUES('tenant','hash',x'7b7d'); INSERT INTO vms VALUES('vm',1,'','created',NULL,x'7b7d')"); err != nil {
		_ = transaction.Rollback()
		t.Fatal(err)
	}
	if err = transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	identity := first.identity
	key := bytes.Clone(first.key)
	if err := first.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStore(directory, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if reopened.identity != identity || !bytes.Equal(reopened.key, key) {
		t.Fatal("store identity or stable node key changed across reopen")
	}
	var tenants, vms int
	if err := reopened.db.QueryRow("SELECT (SELECT COUNT(*) FROM tenants),(SELECT COUNT(*) FROM vms)").Scan(&tenants, &vms); err != nil || tenants != 1 || vms != 1 {
		t.Fatalf("one DB did not preserve tenant and resource facts: tenants=%d vms=%d error=%v", tenants, vms, err)
	}
}

func TestHavenStoreRejectsForeignStateBeforeDDL(t *testing.T) {
	for _, initialize := range []bool{false, true} {
		for _, legacy := range []struct{ name, schema string }{
			{"metadata.db", "CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT); INSERT INTO meta VALUES('schema','1');"},
			{"sandboxd.db", "CREATE TABLE version(version INTEGER); INSERT INTO version VALUES(6);"},
		} {
			t.Run(fmt.Sprintf("legacy-%s/init-%t", legacy.name, initialize), func(t *testing.T) {
				directory := t.TempDir()
				if err := os.Chmod(directory, 0o700); err != nil {
					t.Fatal(err)
				}
				storeTestSQL(t, filepath.Join(directory, legacy.name), legacy.schema)
				before := storeSnapshot(t, directory)
				if state, err := openStore(directory, initialize); !errors.Is(err, errStoreRecovery) {
					if state != nil {
						_ = state.close()
					}
					t.Fatalf("legacy state accepted: %v", err)
				}
				if after := storeSnapshot(t, directory); !reflect.DeepEqual(before, after) {
					t.Fatal("legacy state changed during rejection")
				}
			})
		}
	}
	for _, scenario := range []string{"nonempty-init", "missing-db", "corrupt-db", "foreign-application", "future-version", "missing-table", "missing-guard", "dangling-reference", "recovery-gate", "missing-key", "public-directory"} {
		t.Run(scenario, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "state")
			state, err := openStore(directory, true)
			if err != nil {
				t.Fatal(err)
			}
			if err = state.close(); err != nil {
				t.Fatal(err)
			}
			initialize := false
			switch scenario {
			case "nonempty-init":
				initialize = true
			case "missing-db":
				err = os.Remove(filepath.Join(directory, "haven.db"))
			case "corrupt-db":
				err = os.WriteFile(filepath.Join(directory, "haven.db"), []byte("corrupt fixture"), 0o600)
			case "foreign-application":
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA application_id=0; CREATE TABLE engine_marker(value TEXT)")
			case "future-version":
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA user_version=99")
			case "missing-table":
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "DROP TABLE backing")
			case "missing-guard":
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "DROP TRIGGER step_terminal_immutable")
			case "dangling-reference":
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA foreign_keys=OFF; INSERT INTO vms VALUES('dangling',1,'','created','missing',x'7b7d')")
			case "recovery-gate":
				err = os.WriteFile(filepath.Join(directory, "RECOVERY_REQUIRED"), []byte("review required"), 0o600)
			case "missing-key":
				err = os.Remove(filepath.Join(directory, "node.key"))
			case "public-directory":
				err = os.Chmod(directory, 0o755)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := storeSnapshot(t, directory)
			if state, err := openStore(directory, initialize); !errors.Is(err, errStoreRecovery) {
				if state != nil {
					_ = state.close()
				}
				t.Fatalf("unsupported state accepted: %v", err)
			}
			if after := storeSnapshot(t, directory); !reflect.DeepEqual(before, after) {
				t.Fatal("rejection modified files, identities, modes or directory entries")
			}
		})
	}
	t.Run("symlink-state", func(t *testing.T) {
		parent := t.TempDir()
		directory := filepath.Join(parent, "state")
		state, err := openStore(directory, true)
		if err != nil {
			t.Fatal(err)
		}
		if err = state.close(); err != nil {
			t.Fatal(err)
		}
		alias := filepath.Join(parent, "alias")
		if err = os.Symlink(directory, alias); err != nil {
			t.Fatal(err)
		}
		before := storeSnapshot(t, parent)
		if state, err := openStore(alias, false); !errors.Is(err, errStoreRecovery) {
			if state != nil {
				_ = state.close()
			}
			t.Fatalf("symlink state accepted: %v", err)
		}
		if after := storeSnapshot(t, parent); !reflect.DeepEqual(before, after) {
			t.Fatal("symlink rejection modified the original state")
		}
	})
}

func TestHavenStoreRejectsRollbackBehindHead(t *testing.T) {
	parent := t.TempDir()
	directory := filepath.Join(parent, "state")
	state, err := openStore(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(parent, "older.db")
	if _, err := state.db.Exec("VACUUM INTO ?", backup); err != nil {
		t.Fatal(err)
	}
	if _, err := state.db.Exec("INSERT INTO tenants VALUES('later','hash',x'7b7d')"); err != nil {
		t.Fatal(err)
	}
	if err = state.close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(backup)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "haven.db"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	before := storeSnapshot(t, directory)
	if reopened, err := openStore(directory, false); !errors.Is(err, errStoreRecovery) {
		if reopened != nil {
			_ = reopened.close()
		}
		t.Fatalf("metadata behind durable HEAD accepted: %v", err)
	}
	if after := storeSnapshot(t, directory); !reflect.DeepEqual(before, after) {
		t.Fatal("rollback rejection modified the retained point")
	}
}

func storeTestSQL(t *testing.T, path, statement string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.Exec(statement); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

type storeFileSnapshot struct {
	Mode  fs.FileMode
	Inode uint64
	Hash  [32]byte
	Link  string
}

func storeSnapshot(t *testing.T, directory string) map[string]storeFileSnapshot {
	t.Helper()
	files := map[string]storeFileSnapshot{}
	if err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		value := storeFileSnapshot{Mode: info.Mode(), Inode: info.Sys().(*syscall.Stat_t).Ino}
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			value.Hash = sha256.Sum256(data)
		} else if info.Mode()&os.ModeSymlink != 0 {
			value.Link, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		relative, _ := filepath.Rel(directory, path)
		files[relative] = value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files
}
