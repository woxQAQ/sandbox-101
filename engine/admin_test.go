package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestRestoreBackupRefusesSQLiteSidecars(t *testing.T) {
	for _, name := range []string{"metadata.db-wal", "metadata.db-shm"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, name)
			if err := os.WriteFile(
				path,
				[]byte("retained evidence"),
				0o600,
			); err != nil {
				t.Fatal(err)
			}
			err := RestoreBackup(dir, filepath.Join(dir, "absent-backup"))
			a, ok := err.(*api.Error)
			if !ok || a.Code != api.Conflict {
				t.Fatalf(
					"expected sidecar conflict before inspecting backup, got %v",
					err,
				)
			}
			if data, err := os.ReadFile(
				path,
			); err != nil ||
				string(data) != "retained evidence" {
				t.Fatalf("sidecar changed: %q %v", data, err)
			}
		})
	}
}

func TestBackupRecoveryRequiresExplicitReview(t *testing.T) {
	dir := t.TempDir()
	b := newTestBackend()
	e := openTest(t, dir, b, true, nil)
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	backup := filepath.Join(t.TempDir(), "backup.db")
	point, err := e.Backup(context.Background(), backup)
	if err != nil {
		t.Fatal(err)
	}
	if point.ID == "" || point.DatabaseSHA256 == "" {
		t.Fatal("missing recovery point")
	}
	e.Close()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err = os.Remove(
			filepath.Join(dir, "metadata.db") + suffix,
		); err != nil &&
			!os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	if err = RestoreBackup(dir, backup); err != nil {
		t.Fatal(err)
	}
	if other, x := Open(Options{StateDir: dir, Backend: b}); x == nil {
		other.Close()
		t.Fatal("restored backup automatically trusted")
	}
	if err = AcknowledgeRecovery(
		dir,
		"reviewed running scopes and retained files",
	); err != nil {
		t.Fatal(err)
	}
	e = openTest(t, dir, b, false, nil)
	defer e.Close()
	v, err := e.GetVM(context.Background(), a.VMID)
	if err != nil || v.ID != a.VMID {
		t.Fatal("recovery lost known VM", err)
	}
}
