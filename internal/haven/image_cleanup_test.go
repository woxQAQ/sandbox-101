package haven

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func failedCachedImport(t *testing.T) (*Service, Operation) {
	t.Helper()
	options := privateOCIImport(t)
	service, _, _, _ := sandboxFixture(t)
	service.opts.Fault = func(point string) error {
		if point == "image_import_layer_saved" {
			return errors.New("known fixture abort after cache responsibility persisted")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	operation, err := service.AdmitImageImport(ctx, Principal{Root: true, TenantID: "root"}, "import", options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.WaitManagement(ctx, Principal{Root: true, TenantID: "root"}, operation.ID)
	if err == nil || result.Status != "failed" {
		t.Fatal(result, err)
	}
	waitImportInterruption(t, service, operation.ID)
	service.opts.Fault = nil
	return service, result
}

func TestHavenActualOCIFailedImportAndIndependentCacheGC(t *testing.T) {
	service, original := failedCachedImport(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	root := Principal{Root: true, TenantID: "root"}
	report, err := service.CollectGarbage(ctx, root, "work", GCOptions{Budget: 20 * time.Second})
	if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "image_work" {
		t.Fatal(report, err)
	}
	var caches, imports, cacheCharges int
	if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM image_caches),(SELECT COUNT(*) FROM reservations WHERE owner LIKE 'import:%'),(SELECT COUNT(*) FROM reservations WHERE owner LIKE 'cache:%')").Scan(&caches, &imports, &cacheCharges); err != nil || caches != 1 || imports != 0 || cacheCharges != 1 {
		t.Fatal(caches, imports, cacheCharges, err)
	}
	report, err = service.CollectGarbage(ctx, root, "cache", GCOptions{Budget: 20 * time.Second})
	if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "image_cache" {
		t.Fatal(report, err)
	}
	if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM image_caches),(SELECT COUNT(*) FROM reservations WHERE owner LIKE 'cache:%')").Scan(&caches, &cacheCharges); err != nil || caches != 0 || cacheCharges != 0 {
		t.Fatal(caches, cacheCharges, err)
	}
	unchanged, err := service.GetManagementOperation(ctx, Principal{Root: true}, original.ID)
	if err != nil || !reflect.DeepEqual(unchanged, original) {
		t.Fatal("cache GC changed original failed import", unchanged, original, err)
	}
}

func TestHavenActualOCIWorkCleanupRecovery(t *testing.T) {
	for _, boundary := range []string{"image_cleanup_prepared", "image_cleanup_file_removed", "image_cleanup_completion_before_commit"} {
		t.Run(boundary, func(t *testing.T) {
			service, original := failedCachedImport(t)
			backend := service.opts.Backend
			service.opts.Fault = func(point string) error {
				if point == boundary {
					return ErrInterrupted
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			collector, err := service.AdmitImageCleanup(ctx, Principal{Root: true, TenantID: "root"}, "cleanup", ImageCleanupRequest{ImportOperationID: original.ID})
			if err != nil {
				t.Fatal(err)
			}
			waitImportInterruption(t, service, collector.ID)
			var imports int
			if err = service.store.db.QueryRow("SELECT COUNT(*) FROM reservations WHERE owner=?", "import:"+original.ID).Scan(&imports); err != nil || imports != 1 {
				t.Fatal("unconfirmed collection retired import charge", imports, err)
			}
			service = reopenCloneService(t, service, backend)
			if _, err = service.WaitManagement(ctx, Principal{Root: true, TenantID: "root"}, collector.ID); err != nil {
				t.Fatal(err)
			}
			if err = service.store.db.QueryRow("SELECT COUNT(*) FROM reservations WHERE owner=?", "import:"+original.ID).Scan(&imports); err != nil || imports != 0 {
				t.Fatal(imports, err)
			}
			unchanged, err := service.GetManagementOperation(ctx, Principal{Root: true}, original.ID)
			if err != nil || !reflect.DeepEqual(unchanged, original) {
				t.Fatal("work cleanup changed failed import", unchanged, original, err)
			}
		})
	}
}
