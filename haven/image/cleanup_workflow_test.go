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
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenActualOCIWorkCleanupRecovery(t *testing.T) {
	for _, boundary := range []string{
		"image_cleanup_prepared",
		"image_cleanup_file_removed",
		"image_cleanup_completion_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, original := fixtures.FailedCachedImport(t, func(point string) error {
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
			fixtures.WaitImportInterruption(t, service, collector.ID)
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
