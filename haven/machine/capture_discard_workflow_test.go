package machine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenCaptureDiscardRetainsUncertainObjects(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, kind := range []string{
		"unknown_raw",
		"unknown_publication",
		"replaced_raw",
		"replaced_publication",
		"reader",
		"inspection_unknown",
	} {
		t.Run(kind, func(t *testing.T) {
			service, backend, request, failed := failedCaptureFixture(t, true)
			raw := failed.State.Boundary.Path
			final := failed.Input.Snapshot.Dir
			retained := ""
			switch kind {
			case "unknown_raw":
				retained = filepath.Join(raw, "unknown")
				if err := os.WriteFile(retained, []byte("retained"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "unknown_publication":
				retained = filepath.Join(final, "unknown")
				if err := os.WriteFile(retained, []byte("retained"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "replaced_raw", "replaced_publication":
				path := raw
				if kind == "replaced_publication" {
					path = final
				}
				if err := os.Rename(path, path+".held"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o711); err != nil {
					t.Fatal(err)
				}
				retained = filepath.Join(path, "marker")
				if err := os.WriteFile(retained, []byte("retained"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "reader":
				file, err := os.Open(filepath.Join(final, "disk.raw"))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = file.Close()
				}()
			case "inspection_unknown":
				backend.InspectError = fmt.Errorf("fixture unknown stopped source")
			}
			stops, networks, removes := backend.Count("stop"), backend.Count("network"), backend.Count("remove")
			operation, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			pending := awaitCaptureDiscard(t, service, operation.ID)
			if fixtures.Terminal(pending.Status) {
				t.Fatal("uncertain material retired", pending.Phase)
			}
			captureDiscardLiability(t, service, failed, 192)
			if retained != "" {
				if data, err := os.ReadFile(retained); err != nil || string(data) != "retained" {
					t.Fatal("unknown/replaced bytes removed", err)
				}
			}
			if backend.Count("stop") != stops || backend.Count("network") != networks || backend.Count("remove") != removes {
				t.Fatal("capture cleanup mutated source VM effects")
			}
		})
	}
}

func TestHavenCaptureDiscardReceiptReplacement(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend, request, failed := failedCaptureFixtureWithFault(t, true, func(point string) error {
		if point == "capture_discard_receipts" {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	pending := awaitCaptureDiscard(t, service, operation.ID)
	if pending.Phase != "deleting" {
		t.Fatal(pending)
	}
	final := failed.Input.Snapshot.Dir
	if err = os.Rename(final, final+".held"); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(final, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(final, "disk.raw")
	if err = os.WriteFile(marker, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	service = fixtures.ResumeColdService(t, service, backend)
	pending = awaitCaptureDiscard(t, service, operation.ID)
	if fixtures.Terminal(pending.Status) {
		t.Fatal("replaced publication retired")
	}
	captureDiscardLiability(t, service, failed, 192)
	if data, err := os.ReadFile(marker); err != nil || string(data) != "replacement" {
		t.Fatal("replaced receipt authorized unlink", err)
	}
	if _, err = os.Stat(filepath.Join(failed.State.Boundary.Path, "ch/state.json")); err != nil {
		t.Fatal("one destination deleted before validating the other", err)
	}
}

func TestHavenCaptureDiscardReceiptAtomicity(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, mode := range []string{
		"ABORT,'fixture second receipt rejection'",
		"IGNORE",
	} {
		t.Run(mode, func(t *testing.T) {
			service, backend, request, failed := failedCaptureFixture(t, true)
			statement := fmt.Sprintf("CREATE TRIGGER deny_capture_receipt BEFORE INSERT ON deletion_intents WHEN NEW.path LIKE '%%/snapshots/%%' BEGIN SELECT RAISE(%s); END", mode)
			if _, err := service.State.Database().Exec(statement); err != nil {
				t.Fatal(err)
			}
			operation, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			step := awaitCaptureDiscard(t, service, operation.ID)
			if step.Phase != "admitted" || step.Status != "pending" {
				t.Fatal("receipt denial advanced phase", step.Phase)
			}
			var receipts int
			if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM deletion_intents WHERE operation_id=?", operation.ID).Scan(&receipts); err != nil || receipts != 0 {
				t.Fatal("partial receipt set committed", receipts, err)
			}
			captureDiscardLiability(t, service, failed, 192)
			for _, path := range captureDiscardPaths(step) {
				if _, err = os.Stat(path); err != nil {
					t.Fatal("receipt denial removed files", err)
				}
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER deny_capture_receipt"); err != nil {
				t.Fatal(err)
			}
			service = fixtures.ResumeColdService(t, service, backend)
			if result := awaitCaptureDiscard(t, service, operation.ID); result.Status != "succeeded" {
				t.Fatal(result.Phase)
			}
		})
	}
}

func TestHavenCaptureDiscardRejectsUnprovedBoundary(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend := fixtures.ColdService(t, func(point string) error {
		if point == "capture_admitted" {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation, request := fixtures.CaptureReady(t, service, "unproved")
	pending, err := machine.LoadCapture(context.Background(), service.State.Database(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw := filepath.Join(pending.Input.Source.Input.Resource.Dir, "capture", operation.ID)
	if err = os.Mkdir(raw, 0o711); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(raw, "marker")
	if err = os.WriteFile(marker, []byte("unowned"), 0o600); err != nil {
		t.Fatal(err)
	}
	backend.Mu.Lock()
	backend.Observations[pending.Input.Source.Candidate] = loom.Observation{}
	backend.Mu.Unlock()
	service = fixtures.ResumeColdService(t, service, backend)
	failed := fixtures.AwaitCapture(t, service, operation.ID)
	if failed.Status != "failed" || failed.State.Boundary != nil {
		t.Fatal("fixture established missing preparation proof", failed.State)
	}
	stop, err := service.Stops.AdmitStop(context.Background(), machine.StopRequest{
		TenantID:         request.TenantID,
		Key:              "stop-unproved",
		SandboxID:        request.SandboxID,
		ExpectedInstance: request.ExpectedInstance,
	})
	if err != nil {
		t.Fatal(err)
	}
	if proof := fixtures.AwaitStop(t, service, stop.ID); proof.Status != "succeeded" {
		t.Fatal(proof)
	}
	discard, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), machine.CaptureDiscardRequest{
		TenantID:          request.TenantID,
		Key:               "discard-unproved",
		SandboxID:         request.SandboxID,
		FailedOperationID: operation.ID,
		ExpectedInstance:  request.ExpectedInstance,
		StopOperationID:   stop.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if proof := awaitCaptureDiscard(t, service, discard.ID); fixtures.Terminal(proof.Status) {
		t.Fatal("unproved boundary retired", proof)
	}
	captureDiscardLiability(t, service, failed, 192)
	if data, err := os.ReadFile(marker); err != nil || string(data) != "unowned" {
		t.Fatal("cleanup inferred ownership from directory name", err)
	}
}

func TestHavenCaptureDiscard(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, final := range []bool{
		false,
		true,
	} {
		t.Run(fmt.Sprint(final), func(t *testing.T) {
			service, backend, request, failed := failedCaptureFixture(t, final)
			original, err := service.Operations.GetOperation(context.Background(), request.TenantID, failed.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			operation, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			done := awaitCaptureDiscard(t, service, operation.ID)
			if done.Status != "succeeded" {
				reason := service.Workers.State(operation.ID).Err
				t.Fatal("capture discard failed", done, reason)
			}
			captureDiscardLiability(t, service, failed, 0)
			for _, path := range captureDiscardPaths(done) {
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
			service, backend, request, failed := failedCaptureFixtureWithFault(t, true, func(candidate string) error {
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
				awaitCaptureDiscard(t, service, operation.ID)
			}
			service = fixtures.ResumeColdService(t, service, backend)
			step := awaitCaptureDiscard(t, service, operation.ID)
			if step.Status != "succeeded" {
				reason := service.Workers.State(operation.ID).Err
				t.Fatal(step, reason)
			}
			captureDiscardLiability(t, service, failed, 0)
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
				service, backend, request, failed := failedCaptureFixture(t, true)
				statement := fmt.Sprintf("CREATE TRIGGER deny_capture_retirement BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				operation, err := service.CaptureDiscard.AdmitCaptureDiscard(context.Background(), request)
				if err != nil {
					t.Fatal(err)
				}
				pending := awaitCaptureDiscard(t, service, operation.ID)
				if fixtures.Terminal(pending.Status) || pending.Phase != "files_removed" {
					t.Fatal("retirement denial committed", pending)
				}
				captureDiscardLiability(t, service, failed, 192)
				for _, path := range captureDiscardPaths(pending) {
					if _, err = os.Lstat(path); !os.IsNotExist(err) {
						t.Fatal("fixture did not reclaim physical material", err)
					}
				}
				if _, err = service.State.Database().Exec("DROP TRIGGER deny_capture_retirement"); err != nil {
					t.Fatal(err)
				}
				service = fixtures.ResumeColdService(t, service, backend)
				if step := awaitCaptureDiscard(t, service, operation.ID); step.Status != "succeeded" {
					t.Fatal(step)
				}
				captureDiscardLiability(t, service, failed, 0)
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
					step, err := machine.LoadCaptureDiscard(ctx, service.State.Database(), discardID)
					if err != nil || step.Phase != "admitted" || fixtures.Terminal(step.Status) {
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

func TestHavenCaptureDiscardRequiresOwnPendingResult(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, field := range []string{
		"sandbox_id",
		"vm_id",
		"snapshot_id",
	} {
		t.Run(field, func(t *testing.T) {
			service, _, request, failed := failedCaptureFixtureWithFault(t, true, func(point string) error {
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
			captureDiscardLiability(t, service, failed, 192)
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

func awaitCaptureDiscard(t *testing.T, service *application.App, id string) machine.CaptureDiscardStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, seen := worker.Active, worker.Completed
		step, err := machine.LoadCaptureDiscard(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || fixtures.Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("capture discard worker timed out", step.Phase)
		}
		time.Sleep(time.Millisecond)
	}
}

func failedCaptureFixture(t *testing.T, final bool) (*application.App, *testfixture.ColdRuntime, machine.CaptureDiscardRequest, machine.CaptureStep) {
	t.Helper()
	return failedCaptureFixtureWithFault(t, final, nil)
}

func failedCaptureFixtureWithFault(t *testing.T, final bool, fault func(string) error) (*application.App, *testfixture.ColdRuntime, machine.CaptureDiscardRequest, machine.CaptureStep) {
	t.Helper()
	point := "captured"
	if final {
		point = "publish_intent"
	}
	service, backend := fixtures.ColdService(t, func(candidate string) error {
		if candidate == point {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation, request := fixtures.CaptureReady(t, service, "failed-capture")
	before := fixtures.AwaitCapture(t, service, operation.ID)
	if final {
		if err := os.Chmod(filepath.Join(before.Input.Snapshot.Dir, "ch/state.json"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(before.Input.Snapshot.Dir, "ch/state.json"), []byte("damaged captured bytes"), 0o444); err != nil {
			t.Fatal(err)
		}
	}
	service = fixtures.ResumeColdServiceWithFault(t, service, backend, fault)
	failed := fixtures.AwaitCapture(t, service, operation.ID)
	if failed.Status != "failed" || failed.State.ResumeRequired {
		t.Fatal("fixture capture did not fail with resumed source", failed.State)
	}
	stop, err := service.Stops.AdmitStop(context.Background(), machine.StopRequest{
		TenantID:         request.TenantID,
		Key:              "stop-failed-capture",
		SandboxID:        request.SandboxID,
		ExpectedInstance: request.ExpectedInstance,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stopped := fixtures.AwaitStop(t, service, stop.ID); stopped.Status != "succeeded" {
		t.Fatal("fixture source stop failed", stopped)
	}
	return service, backend, machine.CaptureDiscardRequest{
		TenantID:          request.TenantID,
		Key:               "discard-capture",
		SandboxID:         request.SandboxID,
		FailedOperationID: operation.ID,
		ExpectedInstance:  request.ExpectedInstance,
		StopOperationID:   stop.ID,
	}, failed
}

func captureDiscardLiability(t *testing.T, service *application.App, failed machine.CaptureStep, want int) {
	t.Helper()
	var cost, reserved int
	if err := service.State.Database().QueryRow("SELECT (SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE source_operation_id=?),(SELECT COUNT(*) FROM reservations WHERE owner=?)", failed.OperationID, "snapshot:"+failed.Input.Snapshot.ID).Scan(&cost, &reserved); err != nil || cost != want || (want > 0) != (reserved == 1) {
		t.Fatal("capture liability changed without retirement", cost, reserved, err)
	}
}

func captureDiscardPaths(step machine.CaptureDiscardStep) []string {
	source := step.Input.Source
	return []string{
		filepath.Join(source.Input.Source.Input.Resource.Dir, "capture", source.OperationID),
		source.Input.Snapshot.Dir,
	}
}
