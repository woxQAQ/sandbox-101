package haven

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func failedColdFixture(t *testing.T) (*Service, *coldRuntime, resourceStep) {
	t.Helper()
	service, backend := coldService(t, nil)
	backend.failBoot = true
	operation := coldClaim(t, service, "failed-cold")
	source := awaitColdWorker(t, service, operation.ID)
	if source.Status != "failed" || source.State.Witness != "" {
		t.Fatal("cold fixture did not fail", source.State)
	}
	backend.mu.Lock()
	backend.failBoot = false
	backend.mu.Unlock()
	return service, backend, source
}

func failedCloneFixture(t *testing.T, transferred bool) (*Service, *restorableRuntime, resourceStep) {
	t.Helper()
	service, backend, snapshot := cloneFixture(t)
	if transferred {
		backend.mu.Lock()
		backend.failBoot = true
		backend.mu.Unlock()
	} else {
		service.opts.Fault = func(point string) error {
			if point == "restore_candidate_copied" {
				return ErrInterrupted
			}
			return nil
		}
	}
	clone := admitCloneFixture(t, service, snapshot.Result.SnapshotID, "failed-clone")
	awaitColdWorker(t, service, clone.ID)
	if !transferred {
		service = reopenCloneService(t, service, backend)
	}
	source := awaitColdWorker(t, service, clone.ID)
	if source.Status != "failed" || !source.State.RestoreBlocked || source.State.BackingReady != transferred {
		t.Fatal("clone fixture did not fix expected failure", source.State)
	}
	backend.mu.Lock()
	backend.failBoot = false
	backend.mu.Unlock()
	return service, backend, source
}

func discardRequest(source resourceStep) DiscardRequest {
	return DiscardRequest{
		TenantID:          source.Input.TenantID,
		Key:               "discard",
		SandboxID:         source.Input.SandboxID,
		FailedOperationID: source.OperationID,
		ExpectedInstance:  source.Input.Resource.Instance,
	}
}

func awaitDiscard(t *testing.T, service *Service, id string) discardStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		service.mu.Lock()
		active := service.active[id]
		_, seen := service.lastErrors[id]
		service.mu.Unlock()
		step, err := loadDiscard(context.Background(), service.store.db, id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("discard worker did not finish", step.State)
		}
		time.Sleep(time.Millisecond)
	}
}

func discardSucceeded(t *testing.T, service *Service, source resourceStep, discard discardStep, original []byte) {
	t.Helper()
	if discard.Status != "succeeded" {
		service.mu.Lock()
		reason := service.lastErrors[discard.OperationID]
		service.mu.Unlock()
		t.Fatal("discard did not complete", discard.State, reason)
	}
	if _, err := os.Lstat(source.Input.Resource.Dir); !os.IsNotExist(err) {
		t.Fatal("discard reported success with candidate files retained", err)
	}
	after, err := service.GetOperation(context.Background(), source.Input.TenantID, source.OperationID)
	if err != nil || !bytes.Equal(encode(after), original) {
		t.Fatal("discard rewrote original public failure", err)
	}
	history, err := loadStep(context.Background(), service.store.db, source.OperationID)
	if err != nil || !bytes.Equal(encode(history), encode(source)) {
		t.Fatal("discard upgraded or changed failed resource witness", err)
	}
	for _, check := range []struct {
		q   string
		arg any
	}{
		{"SELECT COUNT(*) FROM vms WHERE id=?", source.Candidate},
		{"SELECT COUNT(*) FROM quota_resources WHERE source_operation_id=?", source.OperationID},
		{"SELECT COUNT(*) FROM source_leases WHERE operation_id=?", source.OperationID},
		{"SELECT COUNT(*) FROM backing WHERE vm_id=?", source.Candidate},
	} {
		var count int
		if err = service.store.db.QueryRow(check.q, check.arg).Scan(&count); err != nil || count != 0 {
			t.Fatal("discard did not retire proven candidate liability", check.q, count, err)
		}
	}
}

func TestHavenDiscardFailedCandidate(t *testing.T) {
	parallelLifecycle(t)
	for _, kind := range []string{
		"cold",
		"clone-partial",
		"clone-transferred",
	} {
		t.Run(kind, func(t *testing.T) {
			var service *Service
			var backend loom.Backend
			var source resourceStep
			if kind == "cold" {
				var b *coldRuntime
				service, b, source = failedColdFixture(t)
				backend = b
			} else {
				var b *restorableRuntime
				service, b, source = failedCloneFixture(t, kind == "clone-transferred")
				backend = b
			}
			original, err := service.GetOperation(context.Background(), source.Input.TenantID, source.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			request := discardRequest(source)
			operation, err := service.AdmitDiscard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			result := awaitDiscard(t, service, operation.ID)
			discardSucceeded(t, service, source, result, encode(original))
			service = reopenCloneService(t, service, backend)
			cached, err := service.AdmitDiscard(context.Background(), request)
			if err != nil || cached.ID != operation.ID || cached.Status != "succeeded" {
				t.Fatal("completed discard did not replay original result", err)
			}
			replay, err := service.GetOperation(context.Background(), source.Input.TenantID, source.OperationID)
			if err != nil || !bytes.Equal(encode(replay), encode(original)) {
				t.Fatal("reopen changed failed operation", err)
			}
			if source.Input.Restore != nil {
				var cost int
				if err = service.store.db.QueryRow("SELECT storage_mib FROM quota_resources WHERE tenant_id=? AND kind='snapshot' AND resource_key=?", source.Input.TenantID, source.Input.Restore.Snapshot.ID).Scan(&cost); err != nil || cost != 192 {
					t.Fatal("discard refunded independent source snapshot", cost, err)
				}
			}
		})
	}
}

func TestHavenDiscardCrashRecovery(t *testing.T) {
	parallelLifecycle(t)
	for _, point := range []string{
		"discard_admitted",
		"discard_stop_intent",
		"discard_effect_stopped",
		"discard_removed",
		"discard_deletion_intent",
		"discard_unlinked",
		"discard_effect_files_removed",
		"discard_completion_before_commit",
	} {
		t.Run(point, func(t *testing.T) {
			service, backend, source := failedColdFixture(t)
			original, err := service.GetOperation(context.Background(), "tenant", source.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			service.opts.Fault = func(candidate string) error {
				if candidate == point {
					return ErrInterrupted
				}
				return nil
			}
			operation, err := service.AdmitDiscard(context.Background(), discardRequest(source))
			if err != nil && !errors.Is(err, ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "discard_admitted" {
				awaitDiscard(t, service, operation.ID)
			}
			var held int
			if err = service.store.db.QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?", source.OperationID).Scan(&held); err != nil || held != 64 {
				t.Fatal("incomplete discard retired cost", held, err)
			}
			service = reopenCloneService(t, service, backend)
			result := awaitDiscard(t, service, operation.ID)
			discardSucceeded(t, service, source, result, encode(original))
			if backend.count("launch") != 1 {
				t.Fatal("discard relaunched candidate")
			}
		})
	}
}

func TestHavenDiscardAtomicRetirement(t *testing.T) {
	parallelLifecycle(t)
	for _, write := range []struct {
		event, table, condition string
	}{
		{"DELETE", "quota_resources", "1"},
		{"DELETE", "vms", "1"},
		{"UPDATE", "cleanup_intents", "NEW.status='done'"},
		{"UPDATE", "operation_steps", "NEW.action='discard' AND NEW.status='succeeded'"},
		{"UPDATE", "operations", "NEW.status='succeeded'"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture discard rejection'",
			"IGNORE",
		} {
			t.Run(write.table+"/"+mode, func(t *testing.T) {
				service, backend, source := failedColdFixture(t)
				original, err := service.GetOperation(context.Background(), "tenant", source.OperationID)
				if err != nil {
					t.Fatal(err)
				}
				statement := fmt.Sprintf("CREATE TRIGGER deny_discard_retirement BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err = service.store.db.Exec(statement); err != nil {
					t.Fatal(err)
				}
				operation, err := service.AdmitDiscard(context.Background(), discardRequest(source))
				if err != nil {
					t.Fatal(err)
				}
				result := awaitDiscard(t, service, operation.ID)
				if result.Status != "running" || result.State.Phase != "files_removed" {
					t.Fatal("rejected SQL published discard success", result.State)
				}
				var held, vm, receipt int
				if err = service.store.db.QueryRow("SELECT (SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?),(SELECT COUNT(*) FROM vms WHERE id=?),(SELECT COUNT(*) FROM deletion_intents WHERE operation_id=?)", source.OperationID, source.Candidate, operation.ID).Scan(&held, &vm, &receipt); err != nil || held != 64 || vm != 1 || receipt != 1 {
					t.Fatal("partial discard retirement", held, vm, receipt, err)
				}
				if _, err = service.store.db.Exec("DROP TRIGGER deny_discard_retirement"); err != nil {
					t.Fatal(err)
				}
				service = reopenCloneService(t, service, backend)
				discardSucceeded(t, service, source, awaitDiscard(t, service, operation.ID), encode(original))
			})
		}
	}
}

func TestHavenDiscardPreservesUnknownOrReplacedObjects(t *testing.T) {
	parallelLifecycle(t)
	for _, damage := range []string{
		"foreign-ref",
		"unknown-file",
		"replaced-after-receipt",
	} {
		t.Run(damage, func(t *testing.T) {
			service, backend, source := failedColdFixture(t)
			point := "discard_admitted"
			if damage == "replaced-after-receipt" {
				point = "discard_deletion_intent"
			}
			service.opts.Fault = func(candidate string) error {
				if candidate == point {
					return ErrInterrupted
				}
				return nil
			}
			operation, err := service.AdmitDiscard(context.Background(), discardRequest(source))
			if err != nil && !errors.Is(err, ErrInterrupted) {
				t.Fatal(err)
			}
			if damage == "replaced-after-receipt" {
				awaitDiscard(t, service, operation.ID)
			}
			var path string
			switch damage {
			case "foreign-ref":
				if _, err = service.store.db.Exec("INSERT INTO resource_refs VALUES('vm',?,'reader','foreign')", source.Candidate); err != nil {
					t.Fatal(err)
				}
			case "unknown-file":
				path = filepath.Join(source.Input.Resource.Dir, "unknown")
				if err = os.WriteFile(path, []byte("retained fixture"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "replaced-after-receipt":
				path = source.Input.Resource.Disk
				if err = os.Rename(path, filepath.Join(t.TempDir(), "original-disk")); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, []byte("retained fixture"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			service = reopenCloneService(t, service, backend)
			result := awaitDiscard(t, service, operation.ID)
			if result.Status == "succeeded" {
				t.Fatal("unproved object was reclaimed")
			}
			if path != "" {
				data, err := os.ReadFile(path)
				if err != nil || string(data) != "retained fixture" {
					t.Fatal("unknown/replaced object changed", err)
				}
			}
			var held int
			if err = service.store.db.QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?", source.OperationID).Scan(&held); err != nil || held != 64 {
				t.Fatal("uncertain discard retired cost", held, err)
			}
		})
	}
}
