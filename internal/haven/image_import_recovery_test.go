package haven

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func TestHavenActualOCIImportRecoveryUsesFrozenOutput(t *testing.T) {
	for _, boundary := range []string{"image_import_materialized", "image_import_renamed", "image_import_publication_before_commit"} {
		t.Run(boundary, func(t *testing.T) {
			options := privateOCIImport(t)
			service, runtime, _, _ := sandboxFixture(t)
			service.opts.Fault = func(point string) error {
				if point == boundary {
					return ErrInterrupted
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			operation, err := service.AdmitImageImport(ctx, Principal{Root: true, TenantID: "root"}, "import", options)
			if err != nil {
				t.Fatal(err)
			}
			waitImportInterruption(t, service, operation.ID)
			before, err := loadImageImport(ctx, service.store.db, operation.ID)
			if err != nil || before.State.Proof == nil {
				t.Fatal(before, err)
			}
			if err = os.RemoveAll(options.Layout); err != nil {
				t.Fatal(err)
			}
			service = reopenCloneService(t, service, runtime)
			result, err := service.WaitManagement(ctx, Principal{Root: true, TenantID: "root"}, operation.ID)
			if err != nil || result.Result.ImageDigest != options.Digest {
				t.Fatal(result, err)
			}
			after, err := loadImageImport(ctx, service.store.db, operation.ID)
			if err != nil || !reflect.DeepEqual(before.State.Proof, after.State.Proof) || !reflect.DeepEqual(before.State.Boundary, after.State.Boundary) {
				t.Fatal("recovery replaced original output proof", before, after, err)
			}
		})
	}
}

func TestHavenActualOCIImportRejectsReplacedOutput(t *testing.T) {
	options := privateOCIImport(t)
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Fault = func(point string) error {
		if point == "image_import_materialized" {
			return ErrInterrupted
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	operation, err := service.AdmitImageImport(ctx, Principal{Root: true, TenantID: "root"}, "import", options)
	if err != nil {
		t.Fatal(err)
	}
	waitImportInterruption(t, service, operation.ID)
	step, err := loadImageImport(ctx, service.store.db, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(step.Input.Work, "output")
	original := filepath.Join(step.Input.Work, "original-output")
	if err = os.Rename(output, original); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatal("unexpected output directory")
		}
		if err = loom.CopyFile(filepath.Join(original, entry.Name()), filepath.Join(output, entry.Name()), 0o400); err != nil {
			t.Fatal(err)
		}
	}
	service = reopenCloneService(t, service, runtime)
	result, err := service.WaitManagement(ctx, Principal{Root: true, TenantID: "root"}, operation.ID)
	if err == nil || result.Status != "failed" {
		t.Fatal(result, err)
	}
	var count int
	if err = service.store.db.QueryRow("SELECT COUNT(*) FROM images WHERE digest=?", options.Digest).Scan(&count); err != nil || count != 0 {
		t.Fatal("replacement was published", count, err)
	}
	if _, err = os.Stat(original); err != nil {
		t.Fatal("original output was removed", err)
	}
}

func waitImportInterruption(t *testing.T, service *Service, id string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		service.mu.Lock()
		active, last := service.active[id], service.lastErrors[id]
		service.mu.Unlock()
		if !active && last != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("image effect interruption missing")
		}
		time.Sleep(time.Millisecond)
	}
}
