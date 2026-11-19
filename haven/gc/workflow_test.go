package gc_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/gc"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestHavenActualOCIFailedImportAndIndependentCacheGC(t *testing.T) {
	service, original := testapp.FailedCachedImport(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	report, err := service.GC.Collect(ctx, root, "work", types.GCOptions{
		Budget: 20 * time.Second,
	})
	if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "image_work" {
		t.Fatal(report, err)
	}
	var caches, imports, cacheCharges int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM image_caches),(SELECT COUNT(*) FROM reservations WHERE owner LIKE 'import:%'),(SELECT COUNT(*) FROM reservations WHERE owner LIKE 'cache:%')").Scan(&caches, &imports, &cacheCharges); err != nil || caches != 1 || imports != 0 || cacheCharges != 1 {
		t.Fatal(caches, imports, cacheCharges, err)
	}
	report, err = service.GC.Collect(ctx, root, "cache", types.GCOptions{
		Budget: 20 * time.Second,
	})
	if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "image_cache" {
		t.Fatal(report, err)
	}
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM image_caches),(SELECT COUNT(*) FROM reservations WHERE owner LIKE 'cache:%')").Scan(&caches, &cacheCharges); err != nil || caches != 0 || cacheCharges != 0 {
		t.Fatal(caches, cacheCharges, err)
	}
	unchanged, err := service.Operations.GetManagementOperation(ctx, authorization.Principal{
		Root: true,
	}, original.ID)
	if err != nil || !reflect.DeepEqual(unchanged, original) {
		t.Fatal("cache GC changed original failed import", unchanged, original, err)
	}
}

// Exercise real cache creation, durable cleanup and concurrent cache hits in one
// Haven store. Unpublished cache authority comes from the failed import step;
// an unregistered directory remains unknown regardless of its age or contents.
func TestGCActualLayerCacheAndCompletedImport(t *testing.T) {
	t.Parallel()
	fixture, layout, layers, published := testapp.ImportFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	orphan := testfixture.PutOCILayer(t, layout, append(testfixture.BootLayerEntries(), testfixture.FileEntry("orphan", "data")), false)
	orphanImage := testfixture.PutOCIManifest(t, layout, "failed-cache-owner", []testfixture.OCILayerFixture{
		orphan,
	})
	fixture.Faults.Set(func(point string) error {
		if point == "image_import_layer_saved" {
			return errors.New("known native import abort")
		}
		return nil
	})
	if _, err := fixture.ImportOCI(ctx, imageimport.ImageImportOptions{
		Layout:   layout,
		Digest:   orphanImage,
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	}); err == nil {
		t.Fatal("missing failed import")
	}
	fixture.Faults.Set(nil)
	orphanPath := filepath.Join(fixture.StateDir, "layer-cache", published.Builder[7:], orphan.Diff.Encoded())
	unknown := filepath.Join(fixture.StateDir, "layer-cache", published.Builder[7:], ".pending-unknown")
	if err := os.Mkdir(unknown, 0o700); err != nil {
		t.Fatal(err)
	}
	work := fixture.ImportWork(t, published.Digest)
	if _, err := fixture.Service.GC.Collect(ctx, root, "native-dry", types.GCOptions{
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
		report, err := fixture.Service.GC.Collect(ctx, root, string(rune('a'+index)), types.GCOptions{
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
	if err := published.Verify(filepath.Join(fixture.StateDir, "images", published.Digest[7:]), published.Digest); err != nil {
		t.Fatal("GC broke published layer cache", err)
	}
	second := testfixture.PutOCIManifest(t, layout, "gc-concurrent", layers)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			_, err := fixture.ImportOCI(ctx, imageimport.ImageImportOptions{
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
		_, err := fixture.Service.GC.Collect(ctx, root, "concurrent-"+string(rune('a'+index)), types.GCOptions{
			Budget: 20 * time.Second,
		})
		if err != nil && !errors.Is(err, errs.ErrConflict) {
			t.Error(err)
		}
	}
	workers.Wait()
}

func TestHavenGCCompletionRetainsChildProgress(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT,'private GC completion failure'",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, runtime, _ := testapp.GoldenSupplyFixture(t, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			runtime.Mu.Lock()
			runtime.FailBoot = true
			runtime.Mu.Unlock()
			original, err := service.SandboxRequests.Execute(ctx, authorization.Principal{
				TenantID: "tenant",
			}, "failed-refill", operation.Command{
				Action:     "refill",
				TemplateID: "golden",
			})
			if err == nil || original.Status != "failed" {
				t.Fatal("fixture did not retain failed supply", original, err)
			}
			runtime.Mu.Lock()
			runtime.FailBoot = false
			runtime.Mu.Unlock()
			if _, err = service.State.Database().Exec("CREATE TRIGGER reject_gc_completion BEFORE UPDATE ON operation_steps WHEN OLD.step_key='gc' AND NEW.status='succeeded' BEGIN SELECT RAISE(" + rejection + "); END"); err != nil {
				t.Fatal(err)
			}
			root := authorization.Principal{
				Root:     true,
				TenantID: "root",
			}
			options := types.GCOptions{
				Batch:  1,
				Budget: 10 * time.Second,
			}
			report, err := service.GC.Collect(ctx, root, "collect", options)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || report.OperationID == "" {
				t.Fatal("GC did not expose rejected final progress", report, err)
			}
			before, err := gc.Load(ctx, service.State.Database(), report.OperationID)
			if err != nil || before.Status != "running" || before.State.Next != 1 || len(before.State.Report.Collected) != 1 || len(before.Input.Report.Candidates) != 1 {
				t.Fatal("failed GC completion lost durable child progress", before, err)
			}
			childID := before.State.Report.Collected[0].OperationID
			child, err := service.Operations.GetManagementOperation(ctx, root, childID)
			if err != nil || child.Status != "succeeded" {
				t.Fatal("fixture did not cross independent child completion", child, err)
			}
			stops, removals := runtime.Count("stop"), runtime.Count("remove")
			if _, err = service.State.Database().Exec("DROP TRIGGER reject_gc_completion"); err != nil {
				t.Fatal(err)
			}
			replayed, err := service.GC.Collect(ctx, root, "collect", options)
			if err != nil || replayed.OperationID != report.OperationID || len(replayed.Collected) != 1 || replayed.Collected[0].OperationID != childID {
				t.Fatal("GC replay changed accepted pass or child result", replayed, err)
			}
			done, err := gc.Load(ctx, service.State.Database(), report.OperationID)
			if err != nil || done.Status != "succeeded" || done.Hash != before.Hash || !reflect.DeepEqual(done.Input, before.Input) || done.State.Next != before.State.Next {
				t.Fatal("GC replay changed frozen inventory, budget or deadline", done, err)
			}
			if runtime.Count("stop") != stops || runtime.Count("remove") != removals {
				t.Fatal("GC replay repeated retired child effects")
			}
			unchanged, err := service.Operations.GetOperation(ctx, "tenant", original.ID)
			if err != nil || !reflect.DeepEqual(unchanged, original) {
				t.Fatal("GC changed original supply failure", unchanged, err)
			}
		})
	}
}
