package workflows_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom/imageimport"
)

func TestHavenImageCleanupRetainsResponsibilityUntilAtomicCompletion(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, condition := range []string{
		"ignored_step",
		"changed_host_reservation",
	} {
		t.Run(condition, func(t *testing.T) {
			service, id, options := pendingImageRecovery(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			root := authorization.Principal{
				Root:     true,
				TenantID: "root",
			}
			service.Reconcile.Dispatch(id)
			original, err := service.OperationMonitor.WaitManagement(ctx, root, id)
			if err == nil || original.Status != "failed" {
				t.Fatal(original, err)
			}
			expectedStorageBytes := int64(64 << 20)
			if condition == "ignored_step" {
				_, err = service.State.Database().Exec(`CREATE TRIGGER ignore_image_completion BEFORE UPDATE ON operation_steps
					WHEN OLD.step_key='image_cleanup' AND NEW.status='succeeded'
					BEGIN SELECT RAISE(IGNORE); END`)
			} else {
				expectedStorageBytes++
				_, err = service.State.Database().Exec("UPDATE host_capacity_reservations SET storage_bytes=storage_bytes+1 WHERE owner_key=?", "import:"+id)
			}
			if err != nil {
				t.Fatal(err)
			}
			collector, err := service.Images.AdmitCleanup(ctx, root, "cleanup", image.ImageCleanupRequest{
				ImportOperationID: id,
			})
			if err != nil {
				t.Fatal(err)
			}
			retained, err := service.OperationMonitor.WaitManagement(ctx, root, collector.ID)
			if err == nil || retained.Status != "pending" || ctx.Err() != nil {
				t.Fatal("completion did not retain unfinished responsibility", retained, err)
			}
			if _, err = os.Stat(filepath.Join(service.State.Directory(), "imports", id)); !os.IsNotExist(err) {
				t.Fatal("fixture did not reach physical deletion before metadata rejection", err)
			}
			var stepStatus, phase string
			if err = service.State.Database().QueryRow("SELECT status,json_extract(body,'$.phase') FROM operation_steps WHERE operation_id=? AND step_key='image_cleanup'", collector.ID).Scan(&stepStatus, &phase); err != nil || stepStatus != "running" || phase != "removed" {
				t.Fatal("collector did not retain the physical deletion proof", stepStatus, phase, err)
			}
			var storageBytes int64
			var intents, pending int
			if err = service.State.Database().QueryRow(`SELECT
				(SELECT storage_bytes FROM host_capacity_reservations WHERE owner_key=?),
				(SELECT COUNT(*) FROM image_intents WHERE operation_id=?),
				(SELECT COUNT(*) FROM cleanup_intents WHERE id IN (?,?) AND status='pending')`,
				"import:"+id, id, "image_import:"+id, "image_cleanup:"+collector.ID,
			).Scan(&storageBytes, &intents, &pending); err != nil || storageBytes != expectedStorageBytes || intents != 1 || pending != 2 {
				t.Fatal("failed handoff partially retired responsibility", storageBytes, intents, pending, err)
			}
			if condition == "ignored_step" {
				_, err = service.State.Database().Exec("DROP TRIGGER ignore_image_completion")
			} else {
				_, err = service.State.Database().Exec("UPDATE host_capacity_reservations SET storage_bytes=storage_bytes-1 WHERE owner_key=?", "import:"+id)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = service.Reconcile.ResumePending(); err != nil {
				t.Fatal(err)
			}
			if _, err = service.OperationMonitor.WaitManagement(ctx, root, collector.ID); err != nil {
				t.Fatal(err)
			}
			var remaining int
			if err = service.State.Database().QueryRow(`SELECT
				(SELECT COUNT(*) FROM host_capacity_reservations WHERE owner_key=?)+
				(SELECT COUNT(*) FROM image_intents WHERE digest=?)+
				(SELECT COUNT(*) FROM cleanup_intents WHERE id IN (?,?) AND status='pending')`,
				"import:"+id, options.Digest, "image_import:"+id, "image_cleanup:"+collector.ID,
			).Scan(&remaining); err != nil || remaining != 0 {
				t.Fatal("confirmed cleanup retained discharged responsibility", remaining, err)
			}
			unchanged, err := service.OperationMonitor.GetManagementOperation(ctx, root, id)
			if err != nil || !reflect.DeepEqual(original, unchanged) {
				t.Fatal("collector rewrote the original import outcome", unchanged, err)
			}
		})
	}
}

// This private recovery fixture starts with a frozen plan whose source vanished
// before staging. The real runner prepares owned directories and records failure;
// no EROFS materialization, namespace or VM behavior is represented by this fixture.
func pendingImageRecovery(t *testing.T) (*application.App, string, imageimport.ImageImportOptions) {
	t.Helper()
	service, _ := testapp.ColdService(t, nil)
	id, options := testfixture.SeedPendingImageImport(t, service.State)
	return service, id, options
}
