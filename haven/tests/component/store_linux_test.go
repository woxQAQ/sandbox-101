package component

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenStoreExclusiveOwner(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	directory := filepath.Join(t.TempDir(), "state")
	first, err := fixtures.OpenStore(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = first.Close()
	})
	if second, err := fixtures.OpenStore(directory, false); !errors.Is(err, store.ErrConflict) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second writer accepted: %v", err)
	}
	if first.Database().Stats().MaxOpenConnections != 1 {
		t.Fatal("store does not have one writer connection")
	}
	var journal string
	var synchronous, foreignKeys int
	if err := first.Database().QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("journal mode=%s: %v", journal, err)
	}
	if err := first.Database().QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil || synchronous != 2 {
		t.Fatalf("synchronous=%d: %v", synchronous, err)
	}
	if err := first.Database().QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("foreign_keys=%d: %v", foreignKeys, err)
	}
	transaction, err := first.Database().Begin()
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
	identity := first.Identity()
	key, err := os.ReadFile(filepath.Join(directory, "node.key"))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := fixtures.OpenStore(directory, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedKey, err := os.ReadFile(filepath.Join(directory, "node.key"))
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Identity() != identity || !bytes.Equal(reopenedKey, key) {
		t.Fatal("store identity or stable node key changed across reopen")
	}
	var tenants, vms int
	if err := reopened.Database().QueryRow("SELECT (SELECT COUNT(*) FROM tenants),(SELECT COUNT(*) FROM vms)").Scan(&tenants, &vms); err != nil || tenants != 1 || vms != 1 {
		t.Fatalf("one DB did not preserve tenant and resource facts: tenants=%d vms=%d error=%v", tenants, vms, err)
	}
}

func TestHavenStoreRejectsForeignStateBeforeDDL(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, initialize := range []bool{
		false,
		true,
	} {
		for _, legacy := range []struct {
			name   string
			schema string
		}{
			{"metadata.db", "CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT); INSERT INTO meta VALUES('schema','1');"},
			{"sandboxd.db", "CREATE TABLE version(version INTEGER); INSERT INTO version VALUES(6);"},
		} {
			t.Run(fmt.Sprintf("legacy-%s/init-%t", legacy.name, initialize), func(t *testing.T) {
				directory := t.TempDir()
				if err := os.Chmod(directory, 0o700); err != nil {
					t.Fatal(err)
				}
				fixtures.StoreTestSQL(t, filepath.Join(directory, legacy.name), legacy.schema)
				before := fixtures.StoreSnapshot(t, directory)
				if state, err := fixtures.OpenStore(directory, initialize); !errors.Is(err, store.ErrRecovery) {
					if state != nil {
						_ = state.Close()
					}
					t.Fatalf("legacy state accepted: %v", err)
				}
				if after := fixtures.StoreSnapshot(t, directory); !reflect.DeepEqual(before, after) {
					t.Fatal("legacy state changed during rejection")
				}
			})
		}
	}
	for _, scenario := range []string{
		"nonempty-init",
		"missing-db",
		"corrupt-db",
		"foreign-application",
		"future-version",
		"missing-table",
		"foreign-trigger",
		"dangling-reference",
		"recovery-gate",
		"missing-key",
		"public-directory",
	} {
		t.Run(scenario, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "state")
			state, err := fixtures.OpenStore(directory, true)
			if err != nil {
				t.Fatal(err)
			}
			if err = state.Close(); err != nil {
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
				fixtures.StoreTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA application_id=0; CREATE TABLE engine_marker(value TEXT)")
			case "future-version":
				fixtures.StoreTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA user_version=99")
			case "missing-table":
				fixtures.StoreTestSQL(t, filepath.Join(directory, "haven.db"), "DROP TABLE backing")
			case "foreign-trigger":
				fixtures.StoreTestSQL(t, filepath.Join(directory, "haven.db"), "CREATE TRIGGER foreign_guard BEFORE UPDATE ON operations BEGIN SELECT 1; END")
			case "dangling-reference":
				fixtures.StoreTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA foreign_keys=OFF; INSERT INTO vms VALUES('dangling',1,'','created','missing',x'7b7d')")
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
			before := fixtures.StoreSnapshot(t, directory)
			if state, err := fixtures.OpenStore(directory, initialize); !errors.Is(err, store.ErrRecovery) {
				if state != nil {
					_ = state.Close()
				}
				t.Fatalf("unsupported state accepted: %v", err)
			}
			if after := fixtures.StoreSnapshot(t, directory); !reflect.DeepEqual(before, after) {
				t.Fatal("rejection modified files, identities, modes or directory entries")
			}
		})
	}
	t.Run("symlink-state", func(t *testing.T) {
		parent := t.TempDir()
		directory := filepath.Join(parent, "state")
		state, err := fixtures.OpenStore(directory, true)
		if err != nil {
			t.Fatal(err)
		}
		if err = state.Close(); err != nil {
			t.Fatal(err)
		}
		alias := filepath.Join(parent, "alias")
		if err = os.Symlink(directory, alias); err != nil {
			t.Fatal(err)
		}
		before := fixtures.StoreSnapshot(t, parent)
		if state, err := fixtures.OpenStore(alias, false); !errors.Is(err, store.ErrRecovery) {
			if state != nil {
				_ = state.Close()
			}
			t.Fatalf("symlink state accepted: %v", err)
		}
		if after := fixtures.StoreSnapshot(t, parent); !reflect.DeepEqual(before, after) {
			t.Fatal("symlink rejection modified the original state")
		}
	})
}

func TestHavenStoreRejectsRollbackBehindHead(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	parent := t.TempDir()
	directory := filepath.Join(parent, "state")
	state, err := fixtures.OpenStore(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(parent, "older.db")
	if _, err := state.Database().Exec("VACUUM INTO ?", backup); err != nil {
		t.Fatal(err)
	}
	if _, err := state.Database().Exec("INSERT INTO tenants VALUES('later','hash',x'7b7d')"); err != nil {
		t.Fatal(err)
	}
	if err = state.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(backup)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "haven.db"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	before := fixtures.StoreSnapshot(t, directory)
	if reopened, err := fixtures.OpenStore(directory, false); !errors.Is(err, store.ErrRecovery) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("metadata behind durable HEAD accepted: %v", err)
	}
	if after := fixtures.StoreSnapshot(t, directory); !reflect.DeepEqual(before, after) {
		t.Fatal("rollback rejection modified the retained point")
	}
}
