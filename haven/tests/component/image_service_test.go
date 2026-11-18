package component

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenImageCleanupRetainsResponsibilityUntilAtomicCompletion(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, condition := range []string{
		"ignored_step",
		"changed_charge",
	} {
		t.Run(condition, func(t *testing.T) {
			service, id, options := fixtures.PendingImageRecovery(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			root := authorization.Principal{
				Root:     true,
				TenantID: "root",
			}
			service.Maintenance.Dispatch(id)
			original, err := service.Operations.WaitManagement(ctx, root, id)
			if err == nil || original.Status != "failed" {
				t.Fatal(original, err)
			}
			expectedCharge := int64(64 << 20)
			if condition == "ignored_step" {
				_, err = service.State.Database().Exec(`CREATE TRIGGER ignore_image_completion BEFORE UPDATE ON operation_steps
					WHEN OLD.step_key='image_cleanup' AND NEW.status='succeeded'
					BEGIN SELECT RAISE(IGNORE); END`)
			} else {
				expectedCharge++
				_, err = service.State.Database().Exec("UPDATE reservations SET bytes=bytes+1 WHERE owner=?", "import:"+id)
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
			retained, err := service.Operations.WaitManagement(ctx, root, collector.ID)
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
			var charge int64
			var intents, pending int
			if err = service.State.Database().QueryRow(`SELECT
				(SELECT bytes FROM reservations WHERE owner=?),
				(SELECT COUNT(*) FROM image_intents WHERE operation_id=?),
				(SELECT COUNT(*) FROM cleanup_intents WHERE id IN (?,?) AND status='pending')`,
				"import:"+id, id, "image_import:"+id, "image_cleanup:"+collector.ID,
			).Scan(&charge, &intents, &pending); err != nil || charge != expectedCharge || intents != 1 || pending != 2 {
				t.Fatal("failed handoff partially retired responsibility", charge, intents, pending, err)
			}
			if condition == "ignored_step" {
				_, err = service.State.Database().Exec("DROP TRIGGER ignore_image_completion")
			} else {
				_, err = service.State.Database().Exec("UPDATE reservations SET bytes=bytes-1 WHERE owner=?", "import:"+id)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = service.Maintenance.Reconcile(); err != nil {
				t.Fatal(err)
			}
			if _, err = service.Operations.WaitManagement(ctx, root, collector.ID); err != nil {
				t.Fatal(err)
			}
			var remaining int
			if err = service.State.Database().QueryRow(`SELECT
				(SELECT COUNT(*) FROM reservations WHERE owner=?)+
				(SELECT COUNT(*) FROM image_intents WHERE digest=?)+
				(SELECT COUNT(*) FROM cleanup_intents WHERE id IN (?,?) AND status='pending')`,
				"import:"+id, options.Digest, "image_import:"+id, "image_cleanup:"+collector.ID,
			).Scan(&remaining); err != nil || remaining != 0 {
				t.Fatal("confirmed cleanup retained discharged responsibility", remaining, err)
			}
			unchanged, err := service.Operations.GetManagementOperation(ctx, root, id)
			if err != nil || !reflect.DeepEqual(original, unchanged) {
				t.Fatal("collector rewrote the original import outcome", unchanged, err)
			}
		})
	}
}
