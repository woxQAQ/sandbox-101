package native

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/types"
)

// Exercise real cache creation, durable cleanup and concurrent cache hits in one
// Haven store. Unpublished cache authority comes from the failed import step;
// an unregistered directory remains unknown regardless of its age or contents.
func TestGCActualLayerCacheAndCompletedImport(t *testing.T) {
	t.Parallel()
	fixture, layout, layers, published := importFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	orphan := putOCILayer(t, layout, append(bootLayerEntries(), fileEntry("orphan", "data")), false)
	orphanImage := putOCIManifest(t, layout, "failed-cache-owner", []ociLayerFixture{
		orphan,
	})
	fixture.faults.Set(func(point string) error {
		if point == "image_import_layer_saved" {
			return errors.New("known native import abort")
		}
		return nil
	})
	if _, err := fixture.ImportOCI(ctx, ImportOptions{
		Layout:   layout,
		Digest:   orphanImage,
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	}); err == nil {
		t.Fatal("missing failed import")
	}
	fixture.faults.Set(nil)
	orphanPath := filepath.Join(fixture.opts.StateDir, "layer-cache", published.Builder[7:], orphan.diff.Encoded())
	unknown := filepath.Join(fixture.opts.StateDir, "layer-cache", published.Builder[7:], ".pending-unknown")
	if err := os.Mkdir(unknown, 0o700); err != nil {
		t.Fatal(err)
	}
	work := fixture.importWork(t, published.Digest)
	if _, err := fixture.service.GarbageCollection.CollectGarbage(ctx, root, "native-dry", types.GCOptions{
		DryRun: true,
		Budget: 20 * time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		orphanPath,
		unknown,
		work,
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("dry run removed resource", path, err)
		}
	}
	var deferred []types.GCItem
	for index := range 3 {
		report, err := fixture.service.GarbageCollection.CollectGarbage(ctx, root, string(rune('a'+index)), types.GCOptions{
			Budget: 20 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		deferred = append(deferred, report.Deferred...)
	}
	for _, path := range []string{
		orphanPath,
		work,
	} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatal("owned resource not reclaimed", path, err)
		}
	}
	if _, err := os.Stat(unknown); err != nil {
		t.Fatal("unknown resource removed", err)
	}
	unknownReported := false
	for _, item := range deferred {
		if item.Kind == "unknown_artifact" && item.ID == unknown {
			unknownReported = true
		}
	}
	if !unknownReported {
		t.Fatal("unknown stage not reported", deferred)
	}
	if err := published.Verify(filepath.Join(fixture.opts.StateDir, "images", published.Digest[7:]), published.Digest); err != nil {
		t.Fatal("GC broke published layer cache", err)
	}
	second := putOCIManifest(t, layout, "gc-concurrent", layers)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			_, err := fixture.ImportOCI(ctx, ImportOptions{
				Layout:   layout,
				Digest:   second,
				MaxBytes: 4 << 20,
				MaxFiles: 1000,
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	for index := range 10 {
		_, err := fixture.service.GarbageCollection.CollectGarbage(ctx, root, "concurrent-"+string(rune('a'+index)), types.GCOptions{
			Budget: 20 * time.Second,
		})
		if err != nil && !errors.Is(err, errs.ErrConflict) {
			t.Error(err)
		}
	}
	workers.Wait()
}
