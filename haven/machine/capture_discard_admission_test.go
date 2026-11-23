package machine_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenCaptureDiscardAdmissionAtomicity(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, write := range []struct {
		event     string
		table     string
		condition string
	}{
		{"INSERT", "operations", "1"},
		{"INSERT", "operation_steps", "NEW.action='capture_discard'"},
		{"UPDATE", "snapshots", "NEW.deleting=1"},
		{"UPDATE", "quota_refs", "NEW.holder_kind='cleanup'"},
		{"UPDATE", "vms", "NEW.operation_id IS NOT NULL"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture capture admission rejection'",
			"IGNORE",
		} {
			t.Run(write.table+mode, func(t *testing.T) {
				service, _, request, failed := failedCaptureFixture(t, false)
				statement := fmt.Sprintf("CREATE TRIGGER deny_capture_discard BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				if _, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request); err == nil {
					t.Fatal("capture discard admission denial ignored")
				}
				captureDiscardLiability(t, service, failed, 192)
				var operations, deleting int
				if err := service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM operations WHERE idem_key=?),(SELECT deleting FROM snapshots WHERE id=?)", request.Key, failed.Input.Snapshot.ID).Scan(&operations, &deleting); err != nil || operations != 0 || deleting != 0 {
					t.Fatal("admission partially committed", operations, deleting, err)
				}
				current, err := machine.LoadCapture(context.Background(), service.State.Database(), failed.OperationID)
				if err != nil || !reflect.DeepEqual(current, failed) {
					t.Fatal("rejected cleanup changed failed capture", err)
				}
			})
		}
	}
}

func TestHavenCaptureDiscardAuthorization(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, kind := range []string{
		"tenant",
		"instance",
		"stop",
		"not_capture",
		"extra_fee_holder",
		"snapshot_reader",
	} {
		t.Run(kind, func(t *testing.T) {
			service, backend, request, failed := failedCaptureFixture(t, false)
			switch kind {
			case "tenant":
				request.TenantID = "another-tenant"
			case "instance":
				request.ExpectedInstance = operation.NewID("run-")
			case "stop":
				request.StopOperationID = failed.OperationID
			case "not_capture":
				request.FailedOperationID = failed.Input.Source.OperationID
			case "extra_fee_holder":
				if _, err := service.State.Database().Exec("INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) VALUES(?,'pending_storage',?,'cleanup','foreign')", request.TenantID, failed.OperationID+":capture"); err != nil {
					t.Fatal(err)
				}
			case "snapshot_reader":
				if _, err := service.State.Database().Exec("INSERT INTO resource_refs(resource_kind,resource_id,holder_kind,holder_id) VALUES('snapshot',?,'step','foreign')", failed.Input.Snapshot.ID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request); err == nil {
				t.Fatal("unscoped or shared capture cleanup accepted")
			}
			captureDiscardLiability(t, service, failed, 192)
			if backend.Count("launch") != 1 {
				t.Fatal("rejected cleanup launched source")
			}
		})
	}
}
