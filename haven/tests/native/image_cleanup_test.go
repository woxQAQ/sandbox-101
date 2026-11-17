package native

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	image "github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func failedCachedImport(t *testing.T, fault func(string) error) (*application.App, operation.Record) {
	t.Helper()
	options := privateOCIImport(t)
	var abort atomic.Bool
	abort.Store(true)
	service, _, _, _ := testapp.SandboxFixtureWithFault(t, func(point string) error {
		if abort.Load() && point == "image_import_layer_saved" {
			return errors.New("known fixture abort after cache responsibility persisted")
		}
		if fault != nil {
			return fault(point)
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	acceptedOperation, err := service.Images.AdmitImport(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "import", options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Operations.WaitManagement(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, acceptedOperation.ID)
	if err == nil || result.Status != "failed" {
		t.Fatal(result, err)
	}
	waitImportInterruption(t, service, acceptedOperation.ID)
	abort.Store(false)
	return service, result
}

func TestHavenActualOCIFailedImportAndIndependentCacheGC(t *testing.T) {
	service, original := failedCachedImport(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	report, err := service.GarbageCollection.CollectGarbage(ctx, root, "work", types.GCOptions{
		Budget: 20 * time.Second,
	})
	if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "image_work" {
		t.Fatal(report, err)
	}
	var caches, imports, cacheCharges int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM image_caches),(SELECT COUNT(*) FROM reservations WHERE owner LIKE 'import:%'),(SELECT COUNT(*) FROM reservations WHERE owner LIKE 'cache:%')").Scan(&caches, &imports, &cacheCharges); err != nil || caches != 1 || imports != 0 || cacheCharges != 1 {
		t.Fatal(caches, imports, cacheCharges, err)
	}
	report, err = service.GarbageCollection.CollectGarbage(ctx, root, "cache", types.GCOptions{
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

func TestHavenActualOCIWorkCleanupRecovery(t *testing.T) {
	for _, boundary := range []string{
		"image_cleanup_prepared",
		"image_cleanup_file_removed",
		"image_cleanup_completion_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, original := failedCachedImport(t, func(point string) error {
				if point == boundary {
					return errs.ErrInterrupted
				}
				return nil
			})
			backend := service.Options.Backend
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			collector, err := service.Images.AdmitCleanup(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "cleanup", image.ImageCleanupRequest{
				ImportOperationID: original.ID,
			})
			if err != nil {
				t.Fatal(err)
			}
			waitImportInterruption(t, service, collector.ID)
			var imports int
			if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM reservations WHERE owner=?", "import:"+original.ID).Scan(&imports); err != nil || imports != 1 {
				t.Fatal("unconfirmed collection retired import charge", imports, err)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			if _, err = service.Operations.WaitManagement(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, collector.ID); err != nil {
				t.Fatal(err)
			}
			if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM reservations WHERE owner=?", "import:"+original.ID).Scan(&imports); err != nil || imports != 0 {
				t.Fatal(imports, err)
			}
			unchanged, err := service.Operations.GetManagementOperation(ctx, authorization.Principal{
				Root: true,
			}, original.ID)
			if err != nil || !reflect.DeepEqual(unchanged, original) {
				t.Fatal("work cleanup changed failed import", unchanged, original, err)
			}
		})
	}
}
