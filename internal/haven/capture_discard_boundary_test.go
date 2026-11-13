package haven

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/loom"
)

func TestHavenCaptureDiscardRetainsUncertainObjects(t *testing.T) {
	parallelLifecycle(t)
	for _, kind := range []string{"unknown_raw", "unknown_publication", "replaced_raw", "replaced_publication", "reader", "inspection_unknown"} {
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
				defer func() { _ = file.Close() }()
			case "inspection_unknown":
				backend.inspectError = fmt.Errorf("fixture unknown stopped source")
			}
			stops, networks, removes := backend.count("stop"), backend.count("network"), backend.count("remove")
			operation, err := service.AdmitCaptureDiscard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			pending := awaitCaptureDiscard(t, service, operation.ID)
			if terminal(pending.Status) {
				t.Fatal("uncertain material retired", pending.Phase)
			}
			captureDiscardLiability(t, service, failed, 192)
			if retained != "" {
				if data, err := os.ReadFile(retained); err != nil || string(data) != "retained" {
					t.Fatal("unknown/replaced bytes removed", err)
				}
			}
			if backend.count("stop") != stops || backend.count("network") != networks || backend.count("remove") != removes {
				t.Fatal("capture cleanup mutated source VM effects")
			}
		})
	}
}

func TestHavenCaptureDiscardReceiptReplacement(t *testing.T) {
	parallelLifecycle(t)
	service, backend, request, failed := failedCaptureFixture(t, true)
	service.opts.Fault = func(point string) error {
		if point == "capture_discard_receipts" {
			return ErrInterrupted
		}
		return nil
	}
	operation, err := service.AdmitCaptureDiscard(context.Background(), request)
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
	service = resumeColdService(t, service, backend)
	pending = awaitCaptureDiscard(t, service, operation.ID)
	if terminal(pending.Status) {
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

func TestHavenCaptureDiscardAdmissionAtomicity(t *testing.T) {
	parallelLifecycle(t)
	for _, write := range []struct{ event, table, condition string }{
		{"INSERT", "operations", "1"}, {"INSERT", "operation_steps", "NEW.action='capture_discard'"}, {"UPDATE", "snapshots", "NEW.deleting=1"}, {"UPDATE", "quota_refs", "NEW.holder_kind='cleanup'"}, {"UPDATE", "vms", "NEW.operation_id IS NOT NULL"},
	} {
		for _, mode := range []string{"ABORT,'fixture capture admission rejection'", "IGNORE"} {
			t.Run(write.table+mode, func(t *testing.T) {
				service, _, request, failed := failedCaptureFixture(t, false)
				statement := fmt.Sprintf("CREATE TRIGGER deny_capture_discard BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.store.db.Exec(statement); err != nil {
					t.Fatal(err)
				}
				if _, err := service.AdmitCaptureDiscard(context.Background(), request); err == nil {
					t.Fatal("capture discard admission denial ignored")
				}
				captureDiscardLiability(t, service, failed, 192)
				var operations, deleting int
				if err := service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM operations WHERE idem_key=?),(SELECT deleting FROM snapshots WHERE id=?)", request.Key, failed.Input.Snapshot.ID).Scan(&operations, &deleting); err != nil || operations != 0 || deleting != 0 {
					t.Fatal("admission partially committed", operations, deleting, err)
				}
				current, err := loadCapture(context.Background(), service.store.db, failed.OperationID)
				if err != nil || !reflect.DeepEqual(current, failed) {
					t.Fatal("rejected cleanup changed failed capture", err)
				}
			})
		}
	}
}

func TestHavenCaptureDiscardReceiptAtomicity(t *testing.T) {
	parallelLifecycle(t)
	for _, mode := range []string{"ABORT,'fixture second receipt rejection'", "IGNORE"} {
		t.Run(mode, func(t *testing.T) {
			service, backend, request, failed := failedCaptureFixture(t, true)
			statement := fmt.Sprintf("CREATE TRIGGER deny_capture_receipt BEFORE INSERT ON deletion_intents WHEN NEW.path LIKE '%%/snapshots/%%' BEGIN SELECT RAISE(%s); END", mode)
			if _, err := service.store.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			operation, err := service.AdmitCaptureDiscard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			step := awaitCaptureDiscard(t, service, operation.ID)
			if step.Phase != "admitted" || step.Status != "pending" {
				t.Fatal("receipt denial advanced phase", step.Phase)
			}
			var receipts int
			if err = service.store.db.QueryRow("SELECT COUNT(*) FROM deletion_intents WHERE operation_id=?", operation.ID).Scan(&receipts); err != nil || receipts != 0 {
				t.Fatal("partial receipt set committed", receipts, err)
			}
			captureDiscardLiability(t, service, failed, 192)
			for _, path := range captureDiscardPaths(step) {
				if _, err = os.Stat(path); err != nil {
					t.Fatal("receipt denial removed files", err)
				}
			}
			if _, err = service.store.db.Exec("DROP TRIGGER deny_capture_receipt"); err != nil {
				t.Fatal(err)
			}
			service = resumeColdService(t, service, backend)
			if result := awaitCaptureDiscard(t, service, operation.ID); result.Status != "succeeded" {
				t.Fatal(result.Phase)
			}
		})
	}
}

func TestHavenCaptureDiscardAuthorization(t *testing.T) {
	parallelLifecycle(t)
	for _, kind := range []string{"tenant", "instance", "stop", "not_capture", "extra_fee_holder", "snapshot_reader"} {
		t.Run(kind, func(t *testing.T) {
			service, backend, request, failed := failedCaptureFixture(t, false)
			switch kind {
			case "tenant":
				request.TenantID = "another-tenant"
			case "instance":
				request.ExpectedInstance = newID("run-")
			case "stop":
				request.StopOperationID = failed.OperationID
			case "not_capture":
				request.FailedOperationID = failed.Input.Source.OperationID
			case "extra_fee_holder":
				if _, err := service.store.db.Exec("INSERT INTO quota_refs VALUES(?,'pending_storage',?,'cleanup','foreign')", request.TenantID, failed.OperationID+":capture"); err != nil {
					t.Fatal(err)
				}
			case "snapshot_reader":
				if _, err := service.store.db.Exec("INSERT INTO resource_refs VALUES('snapshot',?,'step','foreign')", failed.Input.Snapshot.ID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := service.AdmitCaptureDiscard(context.Background(), request); err == nil {
				t.Fatal("unscoped or shared capture cleanup accepted")
			}
			captureDiscardLiability(t, service, failed, 192)
			if backend.count("launch") != 1 {
				t.Fatal("rejected cleanup launched source")
			}
		})
	}
}

func TestHavenCaptureDiscardRejectsUnprovedBoundary(t *testing.T) {
	parallelLifecycle(t)
	service, backend := coldService(t, func(point string) error {
		if point == "capture_admitted" {
			return ErrInterrupted
		}
		return nil
	})
	operation, request := captureReady(t, service, "unproved")
	pending, err := loadCapture(context.Background(), service.store.db, operation.ID)
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
	backend.mu.Lock()
	backend.observations[pending.Input.Source.Candidate] = loom.Observation{}
	backend.mu.Unlock()
	service = resumeColdService(t, service, backend)
	failed := awaitCapture(t, service, operation.ID)
	if failed.Status != "failed" || failed.State.Boundary != nil {
		t.Fatal("fixture established missing preparation proof", failed.State)
	}
	stop, err := service.AdmitStop(context.Background(), StopRequest{TenantID: request.TenantID, Key: "stop-unproved", SandboxID: request.SandboxID, ExpectedInstance: request.ExpectedInstance})
	if err != nil {
		t.Fatal(err)
	}
	if proof := awaitStop(t, service, stop.ID); proof.Status != "succeeded" {
		t.Fatal(proof)
	}
	discard, err := service.AdmitCaptureDiscard(context.Background(), CaptureDiscardRequest{TenantID: request.TenantID, Key: "discard-unproved", SandboxID: request.SandboxID, FailedOperationID: operation.ID, ExpectedInstance: request.ExpectedInstance, StopOperationID: stop.ID})
	if err != nil {
		t.Fatal(err)
	}
	if proof := awaitCaptureDiscard(t, service, discard.ID); terminal(proof.Status) {
		t.Fatal("unproved boundary retired", proof)
	}
	captureDiscardLiability(t, service, failed, 192)
	if data, err := os.ReadFile(marker); err != nil || string(data) != "unowned" {
		t.Fatal("cleanup inferred ownership from directory name", err)
	}
}
