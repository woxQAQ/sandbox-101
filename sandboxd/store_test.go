package sandboxd

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestFutureStoreSchemaIsRejectedBeforeDDL(t *testing.T) {
	dir := t.TempDir()
	store, err := openStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatal("state directory was not made private", err)
	}
	if _, err = store.db.Exec(
		"UPDATE version SET version=?; DROP TABLE checkpoints",
		storeSchemaVersion+1,
	); err != nil {
		t.Fatal(err)
	}
	store.close()
	if _, err = openStore(dir, false); !isCode(err, api.Incompatible) {
		t.Fatalf("future schema accepted: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "sandboxd.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var tables, version int
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='checkpoints'").
		Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT version FROM version").
		Scan(&version); err != nil {
		t.Fatal(err)
	}
	if tables != 0 || version != storeSchemaVersion+1 {
		t.Fatal("opening an incompatible database changed its schema")
	}
}

// Schema 3 introduced configuration fences but has no template_intents.
// It must be rejected before any DDL touches the file, leaving both the raw
// bytes and the table set exactly as they were.
func TestSchemaThreeStateIsRejectedBeforeDDL(t *testing.T) {
	dir := t.TempDir()
	store, err := openStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(
		"UPDATE version SET version=3; DROP TABLE template_intents",
	); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	if err = store.close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "sandboxd.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Open(
		Options{
			StateDir:  dir,
			Engine:    newFakeEngine(),
			Agent:     &fakeAgent{},
			RootToken: "root-secret",
		},
	); !isCode(err, api.Incompatible) {
		t.Fatalf("schema 3 state accepted: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("rejected open modified the database bytes")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, intents int
	if err = db.QueryRow("SELECT version FROM version").
		Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE name='template_intents'",
	).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if version != 3 || intents != 0 {
		t.Fatal("rejected open changed the schema", version, intents)
	}
}

func TestStoreWriterGuardAndMissingDBRecovery(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = openStore(dir, false); !isCode(err, api.Conflict) {
		t.Fatal("second writer allowed")
	}
	s.close()
	if err = os.Remove(filepath.Join(dir, "sandboxd.db")); err != nil {
		t.Fatal(err)
	}
	if _, err = openStore(dir, true); !isCode(err, api.ManualRecovery) {
		t.Fatal("lost DB silently reinitialized")
	}
}
