package component

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenCaptureDiscard(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, final := range []bool{
		false,
		true,
	} {
		t.Run(fmt.Sprint(final), func(t *testing.T) {
			service, backend, request, failed := fixtures.FailedCaptureFixture(t, final)
			original, err := service.Operations.GetOperation(context.Background(), request.TenantID, failed.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			operation, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			done := fixtures.AwaitCaptureDiscard(t, service, operation.ID)
			if done.Status != "succeeded" {
				reason := service.Workers.State(operation.ID).Err
				t.Fatal("capture discard failed", done, reason)
			}
			fixtures.CaptureDiscardLiability(t, service, failed, 0)
			for _, path := range fixtures.CaptureDiscardPaths(done) {
				if _, err = os.Lstat(path); !os.IsNotExist(err) {
					t.Fatal("capture directory retained", path, err)
				}
			}
			if err = loom.VerifyWritableDisk(failed.Input.Source.Input.Resource, *failed.Input.Source.State.DiskIdentity); err != nil {
				t.Fatal("cleanup removed source disk", err)
			}
			current, err := machine.LoadCapture(context.Background(), service.State.Database(), failed.OperationID)
			if err != nil || !reflect.DeepEqual(current, failed) {
				t.Fatal("cleanup rewrote failed capture", err)
			}
			result, err := service.Operations.GetOperation(context.Background(), request.TenantID, failed.OperationID)
			if err != nil || !reflect.DeepEqual(result, original) {
				t.Fatal("cleanup changed original public result", err)
			}
			var owner, slot, disk int
			if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=?),(SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'),(SELECT SUM(storage_mib) FROM quota_resources WHERE kind='vm_disk' AND tenant_id='tenant')", failed.Input.Snapshot.ID).Scan(&owner, &slot, &disk); err != nil || owner != 1 || slot != 1 || disk != 64 {
				t.Fatal("cleanup changed permanent owner or source debt", owner, slot, disk, err)
			}
			replay, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request)
			if err != nil || replay.ID != operation.ID || replay.Status != "succeeded" {
				t.Fatal("discard same-key replay changed", replay, err)
			}
			release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
				TenantID:  request.TenantID,
				Key:       "release-source",
				SandboxID: request.SandboxID,
			})
			if err != nil {
				t.Fatal("cleanup did not unblock source release", err)
			}
			if step := fixtures.AwaitColdWorker(t, service, release.ID); step.Status != "succeeded" {
				t.Fatal(step.State)
			}
			if backend.Count("launch") != 1 {
				t.Fatal("cleanup relaunched source")
			}
		})
	}
}

func TestHavenCaptureDiscardRecovery(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, point := range []string{
		"capture_discard_admitted",
		"capture_discard_receipts",
		"capture_discard_unlinked",
		"capture_discard_directory_removed",
		"capture_discard_files_removed",
		"capture_discard_completion_before_commit",
		"capture_discard_completed",
	} {
		t.Run(point, func(t *testing.T) {
			service, backend, request, failed := fixtures.FailedCaptureFixtureWithFault(t, true, func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			operation, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request)
			if err != nil && !errors.Is(err, errs.ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "capture_discard_admitted" {
				fixtures.AwaitCaptureDiscard(t, service, operation.ID)
			}
			service = fixtures.ResumeColdService(t, service, backend)
			step := fixtures.AwaitCaptureDiscard(t, service, operation.ID)
			if step.Status != "succeeded" {
				reason := service.Workers.State(operation.ID).Err
				t.Fatal(step, reason)
			}
			fixtures.CaptureDiscardLiability(t, service, failed, 0)
			original, err := machine.LoadCapture(context.Background(), service.State.Database(), failed.OperationID)
			if err != nil || !reflect.DeepEqual(original, failed) {
				t.Fatal("recovery changed original failure", err)
			}
			if backend.Count("launch") != 1 {
				t.Fatal("recovery launched source")
			}
		})
	}
}

func TestHavenCaptureDiscardRetirementAtomicity(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, write := range []struct {
		event     string
		table     string
		condition string
	}{
		{"DELETE", "quota_resources", "OLD.kind='pending_storage'"},
		{"DELETE", "snapshots", "1"},
		{"DELETE", "reservations", "OLD.owner LIKE 'snapshot:%'"},
		{"UPDATE", "operation_steps", "NEW.action='capture_discard' AND NEW.status='succeeded'"},
		{"UPDATE", "operations", "NEW.status='succeeded'"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture capture retirement rejection'",
			"IGNORE",
		} {
			t.Run(write.table+mode, func(t *testing.T) {
				service, backend, request, failed := fixtures.FailedCaptureFixture(t, true)
				statement := fmt.Sprintf("CREATE TRIGGER deny_capture_retirement BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				operation, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request)
				if err != nil {
					t.Fatal(err)
				}
				pending := fixtures.AwaitCaptureDiscard(t, service, operation.ID)
				if fixtures.Terminal(pending.Status) || pending.Phase != "files_removed" {
					t.Fatal("retirement denial committed", pending)
				}
				fixtures.CaptureDiscardLiability(t, service, failed, 192)
				for _, path := range fixtures.CaptureDiscardPaths(pending) {
					if _, err = os.Lstat(path); !os.IsNotExist(err) {
						t.Fatal("fixture did not reclaim physical material", err)
					}
				}
				if _, err = service.State.Database().Exec("DROP TRIGGER deny_capture_retirement"); err != nil {
					t.Fatal(err)
				}
				service = fixtures.ResumeColdService(t, service, backend)
				if step := fixtures.AwaitCaptureDiscard(t, service, operation.ID); step.Status != "succeeded" {
					t.Fatal(step)
				}
				fixtures.CaptureDiscardLiability(t, service, failed, 0)
			})
		}
	}
}

func TestHavenCaptureDiscardRejectsCustodyFromAnotherStage(t *testing.T) {
	fixtures.ParallelLifecycle(t)
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
				service, backend, request, failed := fixtures.FailedCaptureFixtureWithFault(t, true, func(point string) error {
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
					step, err := machine.LoadCaptureDiscard(ctx, service.State.Database(), discardID)
					if err != nil || step.Phase != "admitted" || fixtures.Terminal(step.Status) {
						t.Fatal("rejected execution advanced the child", step, err)
					}
					var receipts int
					if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM deletion_intents WHERE operation_id=?", discardID).Scan(&receipts); err != nil || receipts != 0 {
						t.Fatal("rejected custody authorized deletion receipts", receipts, err)
					}
				}
				fixtures.CaptureDiscardLiability(t, service, failed, 192)
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

func TestHavenCaptureDiscardRequiresOwnPendingResult(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, field := range []string{
		"sandbox_id",
		"vm_id",
		"snapshot_id",
	} {
		t.Run(field, func(t *testing.T) {
			service, _, request, failed := fixtures.FailedCaptureFixtureWithFault(t, true, func(point string) error {
				if point == "capture_discard_admitted" {
					return errs.ErrInterrupted
				}
				return nil
			})
			ctx := context.Background()
			accepted, err := service.CaptureDiscard.AdmitCaptureDiscard(ctx, request)
			if !errors.Is(err, errs.ErrInterrupted) || accepted.ID == "" {
				t.Fatal("fixture did not stop after committed admission", accepted, err)
			}
			var body []byte
			if err = service.State.Database().QueryRowContext(ctx, "SELECT body FROM operations WHERE id=?", accepted.ID).Scan(&body); err != nil {
				t.Fatal(err)
			}
			var result map[string]json.RawMessage
			if err = json.Unmarshal(body, &result); err != nil {
				t.Fatal(err)
			}
			if _, found := result[field]; !found {
				t.Fatal("fixture result lacks the original field", field)
			}
			delete(result, field)
			body, err = json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.State.Database().ExecContext(ctx, "UPDATE operations SET body=? WHERE id=? AND status='pending'", body, accepted.ID); err != nil {
				t.Fatal(err)
			}
			if handled, err := service.CaptureDiscard.Resume(accepted.ID); !handled || err == nil {
				t.Fatal("discard inherited missing identity from the failed capture", handled, err)
			}
			step, err := machine.LoadCaptureDiscard(ctx, service.State.Database(), accepted.ID)
			if err != nil || step.Phase != "admitted" || fixtures.Terminal(step.Status) {
				t.Fatal("incomplete result advanced the discard", step, err)
			}
			var receipts int
			if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM deletion_intents WHERE operation_id=?", accepted.ID).Scan(&receipts); err != nil || receipts != 0 {
				t.Fatal("incomplete result authorized deletion receipts", receipts, err)
			}
			fixtures.CaptureDiscardLiability(t, service, failed, 192)
			for _, path := range []string{
				failed.State.Boundary.Path,
				failed.Input.Snapshot.Dir,
			} {
				if _, err = os.Stat(path); err != nil {
					t.Fatal("incomplete discard result removed original material", path, err)
				}
			}
		})
	}
}
