package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

type catalogBackend struct{ *testBackend }

func (*catalogBackend) RequiresImageCatalog() bool              { return true }
func (*catalogBackend) Prepare(context.Context, Resource) error { return nil }

func TestImagePublicationRejectsOrphansAndChangedMaterialization(t *testing.T) {
	e, err := Open(
		Options{
			StateDir:          filepath.Join(t.TempDir(), "state"),
			Init:              true,
			Backend:           &catalogBackend{newTestBackend()},
			SafetyBytes:       1,
			MaxMemoryMiB:      8192,
			MaxVCPUs:          64,
			ReconcileInterval: time.Hour,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	cfg := config()
	dir := filepath.Join(e.opts.StateDir, "images", cfg.Image[7:])
	if err = os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"kernel", "initramfs", "root.erofs"} {
		if err = os.WriteFile(
			filepath.Join(dir, name),
			[]byte(name),
			0o444,
		); err != nil {
			t.Fatal(err)
		}
	}
	files, err := hashTree(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := ImageManifest{
		Format:       1,
		Layout:       "merged-erofs-v1",
		Digest:       cfg.Image,
		Architecture: runtime.GOARCH,
		Files:        files,
	}
	if err = writeJSON(filepath.Join(dir, "image.json"), m); err != nil {
		t.Fatal(err)
	}
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "orphan", Action: "create", Config: cfg},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := awaitStatus(
		t,
		e,
		op.ID,
	); got.Status != "failed" || got.Error == nil ||
		got.Error.Code != api.Incompatible {
		t.Fatalf("unpublished filesystem image accepted: %+v", got)
	}
	if _, err = e.db.Exec(
		"INSERT INTO meta VALUES(?,?)",
		"image:"+cfg.Image,
		string(encode(m)),
	); err != nil {
		t.Fatal(err)
	}
	wait(t, e, api.Request{Key: "published", Action: "create", Config: cfg})
	changed := m
	changed.Layout = "different-layout"
	if err = writeJSON(filepath.Join(dir, "image.json"), changed); err != nil {
		t.Fatal(err)
	}
	op, err = e.Submit(
		context.Background(),
		api.Request{Key: "changed", Action: "create", Config: cfg},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := awaitStatus(
		t,
		e,
		op.ID,
	); got.Status != "failed" || got.Error == nil ||
		got.Error.Code != api.Incompatible {
		t.Fatalf("changed image metadata accepted: %+v", got)
	}
	if err = writeJSON(filepath.Join(dir, "image.json"), m); err != nil {
		t.Fatal(err)
	}
	captured, _ := json.Marshal(changed)
	if err = e.checkImagePublication(
		Resource{Config: *cfg, ImageDir: dir},
		captured,
	); err == nil {
		t.Fatal("different captured image materialization accepted")
	}
}

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
