package store_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/store"
)

func TestHavenStoreExclusiveOwner(t *testing.T) {
	testutil.ParallelLifecycle(t)
	directory := filepath.Join(t.TempDir(), "state")
	first, err := testapp.OpenStore(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = first.Close()
	})
	if second, err := testapp.OpenStore(directory, false); !errors.Is(err, store.ErrConflict) {
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
	if _, err = transaction.Exec("INSERT INTO tenants VALUES('tenant','hash',0,0); INSERT INTO operations(id,scope_kind,scope_id,idem_key,action,request_kind,status,created_at,finished_at) VALUES('op','node','node','key','gc','gc','running','created',''); INSERT INTO operation_gc_requests VALUES('op',0,1,100); INSERT INTO vms(id,slot,current_instance,state,generation_operation_id,directory,resource_hash) VALUES('vm',1,'','created','op','dir','hash')"); err != nil {
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
	reopened, err := testapp.OpenStore(directory, false)
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
	testutil.ParallelLifecycle(t)
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
				storeTestSQL(t, filepath.Join(directory, legacy.name), legacy.schema)
				before := testutil.StoreSnapshot(t, directory)
				if state, err := testapp.OpenStore(directory, initialize); !errors.Is(err, store.ErrRecovery) {
					if state != nil {
						_ = state.Close()
					}
					t.Fatalf("legacy state accepted: %v", err)
				}
				if after := testutil.StoreSnapshot(t, directory); !reflect.DeepEqual(before, after) {
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
		"previous-format",
		"previous-version",
		"missing-identity-version",
		"future-version",
		"foreign-trigger",
		"dangling-reference",
		"recovery-gate",
		"missing-key",
		"public-directory",
	} {
		t.Run(scenario, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "state")
			state, err := testapp.OpenStore(directory, true)
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
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA application_id=0; CREATE TABLE engine_marker(value TEXT)")
			case "previous-format":
				identity := state.Identity()
				identity.Format = "haven-single-control"
				body, err := json.Marshal(identity)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, "store.json"), body, 0o600); err != nil {
					t.Fatal(err)
				}
			case "missing-identity-version":
				path := filepath.Join(directory, "store.json")
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var identity map[string]json.RawMessage
				if err := json.Unmarshal(body, &identity); err != nil {
					t.Fatal(err)
				}
				delete(identity, "version")
				body, err = json.Marshal(identity)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, body, 0o600); err != nil {
					t.Fatal(err)
				}
			case "previous-version":
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA user_version=9")
			case "future-version":
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA user_version=99")
			case "foreign-trigger":
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "CREATE TRIGGER foreign_guard BEFORE UPDATE ON operations BEGIN SELECT 1; END")
			case "dangling-reference":
				storeTestSQL(t, filepath.Join(directory, "haven.db"), "PRAGMA foreign_keys=OFF; INSERT INTO vms(id,slot,current_instance,state,generation_operation_id,directory,resource_hash) VALUES('dangling',1,'','created','missing','dir','hash')")
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
			before := testutil.StoreSnapshot(t, directory)
			if state, err := testapp.OpenStore(directory, initialize); !errors.Is(err, store.ErrRecovery) {
				if state != nil {
					_ = state.Close()
				}
				t.Fatalf("unsupported state accepted: %v", err)
			}
			if after := testutil.StoreSnapshot(t, directory); !reflect.DeepEqual(before, after) {
				t.Fatal("rejection modified files, identities, modes or directory entries")
			}
		})
	}
	t.Run("symlink-state", func(t *testing.T) {
		parent := t.TempDir()
		directory := filepath.Join(parent, "state")
		state, err := testapp.OpenStore(directory, true)
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
		before := testutil.StoreSnapshot(t, parent)
		if state, err := testapp.OpenStore(alias, false); !errors.Is(err, store.ErrRecovery) {
			if state != nil {
				_ = state.Close()
			}
			t.Fatalf("symlink state accepted: %v", err)
		}
		if after := testutil.StoreSnapshot(t, parent); !reflect.DeepEqual(before, after) {
			t.Fatal("symlink rejection modified the original state")
		}
	})
}

func TestHavenStoreRejectsRollbackBehindHead(t *testing.T) {
	testutil.ParallelLifecycle(t)
	parent := t.TempDir()
	directory := filepath.Join(parent, "state")
	state, err := testapp.OpenStore(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(parent, "older.db")
	if _, err := state.Database().Exec("VACUUM INTO ?", backup); err != nil {
		t.Fatal(err)
	}
	if _, err := state.Database().Exec("INSERT INTO tenants VALUES('later','hash',0,0)"); err != nil {
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
	before := testutil.StoreSnapshot(t, directory)
	if reopened, err := testapp.OpenStore(directory, false); !errors.Is(err, store.ErrRecovery) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("metadata behind durable HEAD accepted: %v", err)
	}
	if after := testutil.StoreSnapshot(t, directory); !reflect.DeepEqual(before, after) {
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
