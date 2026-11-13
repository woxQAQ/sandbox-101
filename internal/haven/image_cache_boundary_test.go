package haven

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func TestHavenActualOCICacheReaderAndReplacementFence(t *testing.T) {
	for _, condition := range []string{"reader", "replacement"} {
		t.Run(condition, func(t *testing.T) {
			service, _ := failedCachedImport(t)
			root := Principal{Root: true, TenantID: "root"}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			work, err := service.CollectGarbage(ctx, root, "work", GCOptions{Budget: 15 * time.Second})
			if err != nil || len(work.Collected) != 1 {
				t.Fatal(work, err)
			}
			// The catalog and receipt, rather than a directory scan, choose the candidate.
			var builder, diff string
			if err = service.store.db.QueryRow("SELECT builder,diff_id FROM image_caches").Scan(&builder, &diff); err != nil {
				t.Fatal(err)
			}
			cache, err := loadImageCache(ctx, service.store.db, builder, diff)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(cache.Witness.Path, "layer.erofs")
			var reader *os.File
			if condition == "reader" {
				reader, err = os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = reader.Close() }()
			} else {
				service.opts.Fault = func(point string) error {
					if point == "image_cleanup_prepared" {
						return ErrInterrupted
					}
					return nil
				}
			}
			report, err := service.CollectGarbage(ctx, root, "cache", GCOptions{Budget: 15 * time.Second})
			if err != nil || len(report.Collected) != 0 {
				t.Fatal(report, err)
			}
			var collector string
			for _, item := range report.Deferred {
				if item.Kind == "image_cache" {
					collector = item.OperationID
				}
			}
			if collector == "" {
				t.Fatal("unconfirmed cache lacks independent cleanup", report)
			}
			var charge int
			if err = service.store.db.QueryRow("SELECT COUNT(*) FROM reservations WHERE owner LIKE 'cache:%'").Scan(&charge); err != nil || charge != 1 {
				t.Fatal("uncertain cache retired physical charge", charge, err)
			}
			if condition == "reader" {
				if err = reader.Close(); err != nil {
					t.Fatal(err)
				}
				if err = service.Reconcile(); err != nil {
					t.Fatal(err)
				}
				if _, err = service.WaitManagement(ctx, root, collector); err != nil {
					t.Fatal(err)
				}
			} else {
				original := filepath.Join(t.TempDir(), "original-layer.erofs")
				if err = os.Chmod(cache.Witness.Path, 0o700); err != nil {
					t.Fatal(err)
				}
				if err = os.Rename(path, original); err != nil {
					t.Fatal(err)
				}
				if err = loom.CopyFile(original, path, 0o444); err != nil {
					t.Fatal(err)
				}
				if err = os.Chmod(cache.Witness.Path, 0o555); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(cache.Witness.Path, 0o700) })
				service.opts.Fault = nil
				if err = service.Reconcile(); err != nil {
					t.Fatal(err)
				}
				if _, err = service.WaitManagement(ctx, root, collector); err == nil {
					t.Fatal("cache replacement passed original deletion receipt")
				}
				if _, err = os.Stat(path); err != nil {
					t.Fatal("replaced cache file was deleted", err)
				}
				if err = service.store.db.QueryRow("SELECT COUNT(*) FROM reservations WHERE owner LIKE 'cache:%'").Scan(&charge); err != nil || charge != 1 {
					t.Fatal("replacement retired cache charge", charge, err)
				}
			}
		})
	}
}
