package image_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/quota"
)

func TestHavenActualOCIWorkCleanupRecovery(t *testing.T) {
	for _, boundary := range []string{
		"image_cleanup_prepared",
		"image_cleanup_file_removed",
		"image_cleanup_completion_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, original := testapp.FailedCachedImport(t, func(point string) error {
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
			testutil.WaitImportInterruption(t, service, collector.ID)
			owner := "import:" + original.ID
			imports, err := quota.CountReservation(ctx, service.State.Database(), owner)
			if err != nil || imports != 1 {
				t.Fatal("unconfirmed collection retired the import reservation", imports, err)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			if _, err = service.OperationMonitor.WaitManagement(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, collector.ID); err != nil {
				t.Fatal(err)
			}
			if imports, err = quota.CountReservation(ctx, service.State.Database(), owner); err != nil || imports != 0 {
				t.Fatal(imports, err)
			}
			unchanged, err := service.OperationMonitor.GetManagementOperation(ctx, authorization.Principal{
				Root: true,
			}, original.ID)
			if err != nil || !reflect.DeepEqual(unchanged, original) {
				t.Fatal("work cleanup changed failed import", unchanged, original, err)
			}
		})
	}
}
