package component

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
)

func TestHavenCaptureDiscardRejectsCustodyFromAnotherStage(t *testing.T) {
	parallelLifecycle(t)
	for _, stage := range []string{
		"admission",
		"execution",
	} {
		for _, fact := range []string{
			"catalog",
			"cleanup",
			"holder",
		} {
			t.Run(stage+"/"+fact, func(t *testing.T) {
				service, backend, request, failed := failedCaptureFixtureWithFault(t, true, func(point string) error {
					if point == "capture_discard_admitted" {
						return errs.ErrInterrupted
					}
					return nil
				})
				ctx := context.Background()
				var discardID string
				if stage == "execution" {
					accepted, err := service.CaptureDiscard.AdmitCaptureDiscard(ctx, request)
					if !errors.Is(err, errs.ErrInterrupted) || accepted.ID == "" {
						t.Fatal("fixture did not stop after committed admission", accepted, err)
					}
					discardID = accepted.ID
				}
				deleting, status, holderKind, holderID := 1, "transferred", "cleanup", "foreign-stage"
				if stage == "execution" {
					deleting, status, holderKind, holderID = 0, "blocked", "engine_dependency", failed.OperationID+":capture"
				}
				var statement string
				var args []any
				switch fact {
				case "catalog":
					statement = "UPDATE snapshots SET deleting=? WHERE id=?"
					args = []any{
						deleting,
						failed.Input.Snapshot.ID,
					}
				case "cleanup":
					statement = "UPDATE cleanup_intents SET status=? WHERE id=?"
					args = []any{
						status,
						"capture:" + failed.OperationID,
					}
				case "holder":
					statement = "UPDATE quota_refs SET holder_kind=?,holder_id=? WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?"
					args = []any{
						holderKind,
						holderID,
						request.TenantID,
						failed.OperationID + ":capture",
					}
				}
				result, err := service.State.Database().ExecContext(ctx, statement, args...)
				if err != nil {
					t.Fatal(err)
				}
				if affected, err := result.RowsAffected(); err != nil || affected != 1 {
					t.Fatal("fixture did not replace exactly one custody fact", affected, err)
				}
				removes := backend.Count("remove")
				if stage == "admission" {
					if _, err = service.CaptureDiscard.AdmitCaptureDiscard(ctx, request); err == nil {
						t.Fatal("admission accepted transferred custody")
					}
					var count int
					if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM operations WHERE idem_key=?", request.Key).Scan(&count); err != nil || count != 0 {
						t.Fatal("rejected admission retained a child operation", count, err)
					}
				} else {
					if handled, err := service.CaptureDiscard.Resume(discardID); !handled || err == nil {
						t.Fatal("execution accepted original capture custody", handled, err)
					}
					step, err := loadCaptureDiscard(ctx, service.State.Database(), discardID)
					if err != nil || step.Phase != "admitted" || terminal(step.Status) {
						t.Fatal("rejected execution advanced the child", step, err)
					}
					var receipts int
					if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM deletion_intents WHERE operation_id=?", discardID).Scan(&receipts); err != nil || receipts != 0 {
						t.Fatal("rejected custody authorized deletion receipts", receipts, err)
					}
				}
				captureDiscardLiability(t, service, failed, 192)
				for _, path := range []string{
					failed.State.Boundary.Path,
					failed.Input.Snapshot.Dir,
				} {
					if _, err = os.Stat(path); err != nil {
						t.Fatal("invalid custody removed capture material", path, err)
					}
				}
				current, err := machine.LoadCapture(ctx, service.State.Database(), failed.OperationID)
				if err != nil || !reflect.DeepEqual(current, failed) || backend.Count("remove") != removes {
					t.Fatal("invalid custody changed the source", err)
				}
			})
		}
	}
}
