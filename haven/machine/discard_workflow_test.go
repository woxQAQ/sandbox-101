package machine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenDiscardDoesNotMutateDevicesOnUnknownInspection(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend, source := failedColdFixture(t)
	backend.Mu.Lock()
	backend.InspectError = errors.New("fixture unknown sibling")
	backend.Mu.Unlock()
	networks := backend.Count("network")
	discard, err := service.Deletion.AdmitDiscard(context.Background(), discardRequest(source))
	if err != nil {
		t.Fatal(err)
	}
	result := fixtures.AwaitDiscard(t, service, discard.ID)
	if result.Status == "succeeded" || backend.Count("stop") != 1 || backend.Count("network") != networks || backend.Count("remove") != 0 {
		t.Fatal("unknown full inventory authorized a device mutation")
	}
	var held int
	if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?", source.OperationID).Scan(&held); err != nil || held != 64 {
		t.Fatal("unknown sibling retired liability", held, err)
	}
}

func TestHavenDiscardRetainsUnprovedRestoreDirectory(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend, snapshot := fixtures.CloneFixtureWithFault(t, func(point string) error {
		if point == "restore_prepare_intent" {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation := fixtures.AdmitCloneFixture(t, service, snapshot.Result.SnapshotID, "unproved")
	pending := fixtures.AwaitColdWorker(t, service, operation.ID)
	if err := os.MkdirAll(pending.Input.Resource.RestoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(pending.Input.Resource.RestoreDir, "config.json")
	if err := os.WriteFile(unknown, []byte("unproved fixture"), 0o444); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	source := fixtures.AwaitColdWorker(t, service, operation.ID)
	if source.Status != "failed" || source.State.Materials != nil {
		t.Fatal("fixture unexpectedly established restore boundary", source.State)
	}
	discard, err := service.Deletion.AdmitDiscard(context.Background(), discardRequest(source))
	if err != nil {
		t.Fatal(err)
	}
	result := fixtures.AwaitDiscard(t, service, discard.ID)
	if result.Status == "succeeded" {
		t.Fatal("current pathname created missing historical directory proof")
	}
	data, err := os.ReadFile(unknown)
	if err != nil || string(data) != "unproved fixture" {
		t.Fatal("unproved directory was reclaimed", err)
	}
	var held int
	if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?", source.OperationID).Scan(&held); err != nil || held != 64 {
		t.Fatal("unproved directory retired cost", held, err)
	}
}

func TestHavenDiscardFailedCandidate(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, kind := range []string{
		"cold",
		"clone-partial",
		"clone-transferred",
	} {
		t.Run(kind, func(t *testing.T) {
			var service *application.App
			var backend loom.Backend
			var source machine.ResourceStep
			if kind == "cold" {
				var b *testfixture.ColdRuntime
				service, b, source = failedColdFixture(t)
				backend = b
			} else {
				var b *fixtures.RestorableRuntime
				service, b, source = failedCloneFixture(t, kind == "clone-transferred")
				backend = b
			}
			original, err := service.Operations.GetOperation(context.Background(), source.Input.TenantID, source.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			request := discardRequest(source)
			operation, err := service.Deletion.AdmitDiscard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			result := fixtures.AwaitDiscard(t, service, operation.ID)
			discardSucceeded(t, service, result, discardExpectation{
				Source:   source,
				Original: testfixture.Encode(original),
			})
			service = testapp.ReopenCloneService(t, service, backend)
			cached, err := service.Deletion.AdmitDiscard(context.Background(), request)
			if err != nil || cached.ID != operation.ID || cached.Status != "succeeded" {
				t.Fatal("completed discard did not replay original result", err)
			}
			replay, err := service.Operations.GetOperation(context.Background(), source.Input.TenantID, source.OperationID)
			if err != nil || !bytes.Equal(testfixture.Encode(replay), testfixture.Encode(original)) {
				t.Fatal("reopen changed failed operation", err)
			}
			if source.Input.Restore != nil {
				var cost int
				if err = service.State.Database().QueryRow("SELECT storage_mib FROM quota_resources WHERE tenant_id=? AND kind='snapshot' AND resource_key=?", source.Input.TenantID, source.Input.Restore.Snapshot.ID).Scan(&cost); err != nil || cost != 192 {
					t.Fatal("discard refunded independent source snapshot", cost, err)
				}
			}
		})
	}
}

func TestHavenDiscardCrashRecovery(t *testing.T) {
	fixtures.ParallelLifecycle(t)
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
			service, backend, source := failedColdFixtureWithFault(t, func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			original, err := service.Operations.GetOperation(context.Background(), "tenant", source.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			operation, err := service.Deletion.AdmitDiscard(context.Background(), discardRequest(source))
			if err != nil && !errors.Is(err, errs.ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "discard_admitted" {
				fixtures.AwaitDiscard(t, service, operation.ID)
			}
			var held int
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?", source.OperationID).Scan(&held); err != nil || held != 64 {
				t.Fatal("incomplete discard retired cost", held, err)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			result := fixtures.AwaitDiscard(t, service, operation.ID)
			discardSucceeded(t, service, result, discardExpectation{
				Source:   source,
				Original: testfixture.Encode(original),
			})
			if backend.Count("launch") != 1 {
				t.Fatal("discard relaunched candidate")
			}
		})
	}
}

func TestHavenDiscardAtomicRetirement(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, write := range []struct {
		event     string
		table     string
		condition string
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
				original, err := service.Operations.GetOperation(context.Background(), "tenant", source.OperationID)
				if err != nil {
					t.Fatal(err)
				}
				statement := fmt.Sprintf("CREATE TRIGGER deny_discard_retirement BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err = service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				operation, err := service.Deletion.AdmitDiscard(context.Background(), discardRequest(source))
				if err != nil {
					t.Fatal(err)
				}
				result := fixtures.AwaitDiscard(t, service, operation.ID)
				if result.Status != "running" || result.State.Phase != "files_removed" {
					t.Fatal("rejected SQL published discard success", result.State)
				}
				var held, vm, receipt int
				if err = service.State.Database().QueryRow("SELECT (SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?),(SELECT COUNT(*) FROM vms WHERE id=?),(SELECT COUNT(*) FROM deletion_intents WHERE operation_id=?)", source.OperationID, source.Candidate, operation.ID).Scan(&held, &vm, &receipt); err != nil || held != 64 || vm != 1 || receipt != 1 {
					t.Fatal("partial discard retirement", held, vm, receipt, err)
				}
				if _, err = service.State.Database().Exec("DROP TRIGGER deny_discard_retirement"); err != nil {
					t.Fatal(err)
				}
				service = testapp.ReopenCloneService(t, service, backend)
				discardSucceeded(t, service, fixtures.AwaitDiscard(t, service, operation.ID), discardExpectation{
					Source:   source,
					Original: testfixture.Encode(original),
				})
			})
		}
	}
}

func TestHavenDiscardPreservesUnknownOrReplacedObjects(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, damage := range []string{
		"foreign-ref",
		"unknown-file",
		"replaced-after-receipt",
	} {
		t.Run(damage, func(t *testing.T) {
			point := "discard_admitted"
			if damage == "replaced-after-receipt" {
				point = "discard_deletion_intent"
			}
			service, backend, source := failedColdFixtureWithFault(t, func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			operation, err := service.Deletion.AdmitDiscard(context.Background(), discardRequest(source))
			if err != nil && !errors.Is(err, errs.ErrInterrupted) {
				t.Fatal(err)
			}
			if damage == "replaced-after-receipt" {
				fixtures.AwaitDiscard(t, service, operation.ID)
			}
			var path string
			switch damage {
			case "foreign-ref":
				if _, err = service.State.Database().Exec("INSERT INTO resource_refs VALUES('vm',?,'reader','foreign')", source.Candidate); err != nil {
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
			service = testapp.ReopenCloneService(t, service, backend)
			result := fixtures.AwaitDiscard(t, service, operation.ID)
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
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?", source.OperationID).Scan(&held); err != nil || held != 64 {
				t.Fatal("uncertain discard retired cost", held, err)
			}
		})
	}
}

func failedColdFixture(t *testing.T) (*application.App, *testfixture.ColdRuntime, machine.ResourceStep) {
	t.Helper()
	return failedColdFixtureWithFault(t, nil)
}

func failedColdFixtureWithFault(t *testing.T, fault func(string) error) (*application.App, *testfixture.ColdRuntime, machine.ResourceStep) {
	t.Helper()
	service, backend := fixtures.ColdService(t, fault)
	backend.FailBoot = true
	operation := fixtures.ColdClaim(t, service, "failed-cold")
	source := fixtures.AwaitColdWorker(t, service, operation.ID)
	if source.Status != "failed" || source.State.Witness != "" {
		t.Fatal("cold fixture did not fail", source.State)
	}
	backend.Mu.Lock()
	backend.FailBoot = false
	backend.Mu.Unlock()
	return service, backend, source
}

func failedCloneFixture(t *testing.T, transferred bool) (*application.App, *fixtures.RestorableRuntime, machine.ResourceStep) {
	t.Helper()
	service, backend, snapshot := fixtures.CloneFixtureWithFault(t, func(point string) error {
		if !transferred && point == "restore_candidate_copied" {
			return errs.ErrInterrupted
		}
		return nil
	})
	if transferred {
		backend.Mu.Lock()
		backend.FailBoot = true
		backend.Mu.Unlock()
	}
	clone := fixtures.AdmitCloneFixture(t, service, snapshot.Result.SnapshotID, "failed-clone")
	fixtures.AwaitColdWorker(t, service, clone.ID)
	if !transferred {
		service = testapp.ReopenCloneService(t, service, backend)
	}
	source := fixtures.AwaitColdWorker(t, service, clone.ID)
	if source.Status != "failed" || !source.State.RestoreBlocked || source.State.BackingReady != transferred {
		t.Fatal("clone fixture did not fix expected failure", source.State)
	}
	backend.Mu.Lock()
	backend.FailBoot = false
	backend.Mu.Unlock()
	return service, backend, source
}

func discardRequest(source machine.ResourceStep) machine.DiscardRequest {
	return machine.DiscardRequest{
		TenantID:          source.Input.TenantID,
		Key:               "discard",
		SandboxID:         source.Input.SandboxID,
		FailedOperationID: source.OperationID,
		ExpectedInstance:  source.Input.Resource.Instance,
	}
}

type discardExpectation struct {
	Source   machine.ResourceStep
	Original []byte
}

func discardSucceeded(t *testing.T, service *application.App, discard machine.DiscardStep, expected discardExpectation) {
	source, original := expected.Source, expected.Original
	t.Helper()
	if discard.Status != "succeeded" {
		reason := service.Workers.State(discard.OperationID).Err
		t.Fatal("discard did not complete", discard.State, reason)
	}
	if _, err := os.Lstat(source.Input.Resource.Dir); !os.IsNotExist(err) {
		t.Fatal("discard reported success with candidate files retained", err)
	}
	after, err := service.Operations.GetOperation(context.Background(), source.Input.TenantID, source.OperationID)
	if err != nil || !bytes.Equal(testfixture.Encode(after), original) {
		t.Fatal("discard rewrote original public failure", err)
	}
	history, err := machine.LoadResourceStep(context.Background(), service.State.Database(), source.OperationID)
	if err != nil || !bytes.Equal(testfixture.Encode(history), testfixture.Encode(source)) {
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
		if err = service.State.Database().QueryRow(check.q, check.arg).Scan(&count); err != nil || count != 0 {
			t.Fatal("discard did not retire proven candidate liability", check.q, count, err)
		}
	}
}
