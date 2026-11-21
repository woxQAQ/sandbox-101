package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenRestoreStoppedTargetDiscard(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, point := range []string{
		"restore_candidate_copied",
		"restore_resources_ready",
		"effect_launched",
	} {
		t.Run(point, func(t *testing.T) {
			faults := &testfixture.SnapshotFaults{}
			service, backend, request, prior := testapp.TargetSetupWithFault(t, faults.Check)
			faults.Set(func(candidate string) error {
				if point == "restore_candidate_copied" && candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			backend.Before = func(name string, _ loom.Resource) error {
				if point == "restore_resources_ready" && name == "launch" || point == "effect_launched" && name == "boot" {
					return fmt.Errorf("fixture target restore effect failure")
				}
				return nil
			}
			acceptedOperation, err := service.Provisioning.AdmitRestore(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			failed := testutil.AwaitColdWorker(t, service, acceptedOperation.ID)
			if point == "restore_candidate_copied" {
				service = testapp.ReopenCloneService(t, service, backend)
				failed = testutil.AwaitColdWorker(t, service, acceptedOperation.ID)
			}
			backend.Before = nil
			if failed.Status != "failed" || failed.State.Witness != "" {
				t.Fatal("restore failure promoted", failed.State)
			}
			var cost int
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id IN (?,?)", prior.OperationID, acceptedOperation.ID).Scan(&cost); err != nil || cost != 128 {
				t.Fatal("failure lost old/new disk debt", cost, err)
			}
			faults.Set(nil)
			discard, err := service.Deletion.AdmitDiscard(context.Background(), machine.DiscardRequest{
				TenantID:          "tenant",
				Key:               "discard-target",
				SandboxID:         request.SandboxID,
				FailedOperationID: acceptedOperation.ID,
				ExpectedInstance:  failed.Input.Resource.Instance,
			})
			if err != nil {
				t.Fatal(err)
			}
			deleted := testutil.AwaitDiscard(t, service, discard.ID)
			if deleted.Status != "succeeded" {
				reason := service.Workers.State(discard.ID).Err
				t.Fatal("multi-version discard failed", deleted.State, reason)
			}
			original, err := machine.LoadResourceStep(context.Background(), service.State.Database(), acceptedOperation.ID)
			if err != nil || original.Status != "failed" || original.State.Witness != "" {
				t.Fatal("discard rewrote failure", original.State, err)
			}
			if backend.Count("launch") > 3 {
				t.Fatal("discard relaunched a generation")
			}
		})
	}
}

func TestHavenRestoreStoppedTargetRecovery(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, point := range []string{
		"restore_admitted",
		"restore_prepare_intent",
		"restore_candidate_prepared",
		"restore_candidate_copied",
		"restore_candidate_renamed",
		"restore_candidate_synced",
		"restore_materials_complete",
		"restore_resources_ready",
		"step_launch_intent",
		"effect_launched",
		"step_launched",
		"step_booted",
		"step_agent_ready",
		"completion_before_commit",
	} {
		t.Run(point, func(t *testing.T) {
			faults := &testfixture.SnapshotFaults{}
			service, backend, request, prior := testapp.TargetSetupWithFault(t, faults.Check)
			faults.Set(func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation, err := service.Provisioning.AdmitRestore(context.Background(), request)
			if err != nil && !errors.Is(err, errs.ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "restore_admitted" {
				testutil.AwaitColdWorker(t, service, acceptedOperation.ID)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			step := testutil.AwaitColdWorker(t, service, acceptedOperation.ID)
			incomplete := point == "restore_candidate_prepared" || point == "restore_candidate_copied" || point == "restore_candidate_renamed" || point == "restore_candidate_synced"
			if incomplete && (step.Status != "failed" || !step.State.RestoreBlocked) || !incomplete && step.Status != "succeeded" {
				t.Fatal("target recovery state", step.State)
			}
			historical, err := machine.LoadResourceStep(context.Background(), service.State.Database(), prior.OperationID)
			if err != nil || !reflect.DeepEqual(historical, prior) {
				t.Fatal("restore changed previous published result", err)
			}
			var cost int
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id IN (?,?)", prior.OperationID, acceptedOperation.ID).Scan(&cost); err != nil || cost != 128 {
				t.Fatal("recovery lost disk debt", cost, err)
			}
			launches := 3
			if incomplete {
				launches = 2
			}
			if backend.Count("launch") != launches {
				t.Fatal("recovery launch count", backend.Count("launch"), launches)
			}
		})
	}
}

func TestHavenRestoreStoppedTargetRepeated(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend, acceptedOperation, first := targetFixture(t)
	if first.Status != "succeeded" {
		t.Fatal(first.State)
	}
	stop, err := service.Stops.AdmitStop(context.Background(), machine.StopRequest{
		TenantID:         "tenant",
		Key:              "stop-again",
		SandboxID:        acceptedOperation.Result.SandboxID,
		ExpectedInstance: first.State.Witness,
	})
	if err != nil {
		t.Fatal(err)
	}
	if proof := testutil.AwaitStop(t, service, stop.ID); proof.Status != "succeeded" {
		t.Fatal("second stop failed", proof)
	}
	second, err := service.Provisioning.AdmitRestore(context.Background(), machine.RestoreRequest{
		TenantID:         "tenant",
		Key:              "restore-again",
		SandboxID:        acceptedOperation.Result.SandboxID,
		SnapshotID:       first.Input.Restore.Snapshot.ID,
		ExpectedInstance: first.State.Witness,
		StopOperationID:  stop.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	restored := testutil.AwaitColdWorker(t, service, second.ID)
	if restored.Status != "succeeded" || len(restored.Input.Ancestors) != 2 {
		reason := service.Workers.State(second.ID).Err
		t.Fatal(restored.State, reason)
	}
	var backing int
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM backing WHERE vm_id=?", restored.Candidate).Scan(&backing); err != nil || backing != 2 {
		t.Fatal("multiple retained RAM versions lost", backing, err)
	}
	release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release-generations",
		SandboxID: acceptedOperation.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if proof := testutil.AwaitColdWorker(t, service, release.ID); proof.Status != "succeeded" {
		reason := service.Workers.State(release.ID).Err
		t.Fatal(proof.State, reason)
	}
	if backend.Count("launch") != 4 {
		t.Fatal("historical generation relaunched")
	}
}

func TestHavenRestoreStoppedTargetAdmissionAtomicity(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, write := range []struct {
		event     string
		table     string
		condition string
	}{
		{"INSERT", "instances", "1"},
		{"INSERT", "quota_resources", "NEW.kind='pending_storage'"},
		{"UPDATE", "vms", "NEW.operation_id IS NOT NULL"},
		{"UPDATE", "resource_refs", "NEW.resource_kind='vm'"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture target admission rejection'",
			"IGNORE",
		} {
			t.Run(write.table+mode, func(t *testing.T) {
				service, _, request, prior := testapp.TargetSetup(t)
				statement := fmt.Sprintf("CREATE TRIGGER deny_target_admission BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				if _, err := service.Provisioning.AdmitRestore(context.Background(), request); err == nil {
					t.Fatal("admission denial ignored")
				}
				var versions, instances, operations int
				if err := service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM resource_versions WHERE vm_id=?),(SELECT COUNT(*) FROM instances WHERE vm_id=?),(SELECT COUNT(*) FROM operations WHERE idem_key=?)", prior.Candidate, prior.Candidate, request.Key).Scan(&versions, &instances, &operations); err != nil || versions != 1 || instances != 1 || operations != 0 {
					t.Fatal("admission partly committed", versions, instances, operations, err)
				}
			})
		}
	}
}

func TestHavenRestoreStoppedTargetRetirementAtomicity(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, mode := range []string{
		"ABORT,'fixture generation retirement rejection'",
		"IGNORE",
	} {
		t.Run(mode, func(t *testing.T) {
			service, backend, acceptedOperation, step := targetFixture(t)
			if step.Status != "succeeded" {
				t.Fatal(step.State)
			}
			ancestor := step.Input.Ancestors[0].OperationID
			statement := fmt.Sprintf("CREATE TRIGGER deny_old_generation_retirement BEFORE DELETE ON instances WHEN OLD.operation_id='%s' BEGIN SELECT RAISE(%s); END", ancestor, mode)
			if _, err := service.State.Database().Exec(statement); err != nil {
				t.Fatal(err)
			}
			release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
				TenantID:  "tenant",
				Key:       "release-restored",
				SandboxID: acceptedOperation.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			pending := testutil.AwaitColdWorker(t, service, release.ID)
			if testutil.Terminal(pending.Status) {
				t.Fatal("retirement denial committed", pending.State)
			}
			var instances, versions, cost int
			if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM instances WHERE vm_id=?),(SELECT COUNT(*) FROM resource_versions WHERE vm_id=?),(SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id IN (?,?))", step.Candidate, step.Candidate, ancestor, acceptedOperation.ID).Scan(&instances, &versions, &cost); err != nil || instances != 2 || versions != 2 || cost != 128 {
				t.Fatal("retirement denial partially removed history/debt", instances, versions, cost, err)
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER deny_old_generation_retirement"); err != nil {
				t.Fatal(err)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			done := testutil.AwaitColdWorker(t, service, release.ID)
			if done.Status != "succeeded" {
				reason := service.Workers.State(release.ID).Err
				t.Fatal(done.State, reason)
			}
		})
	}
}

func TestHavenRestoreStoppedTargetRelease(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend, acceptedOperation, step := targetFixture(t)
	if step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release-restored",
		SandboxID: acceptedOperation.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	deleted := testutil.AwaitColdWorker(t, service, release.ID)
	if deleted.Status != "succeeded" {
		reason := service.Workers.State(release.ID).Err
		t.Fatal("multi-version release failed", deleted.State, reason)
	}
	var versions, instances int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM resource_versions WHERE vm_id=?),(SELECT COUNT(*) FROM instances WHERE vm_id=?)", step.Candidate, step.Candidate).Scan(&versions, &instances); err != nil || versions != 0 || instances != 0 {
		t.Fatal("historical versions were not retired", versions, instances, err)
	}
	if backend.Count("launch") != 3 {
		t.Fatal("cleanup launched an old instance")
	}
}

func TestHavenRestoreStoppedTarget(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend, acceptedOperation, step := targetFixture(t)
	if step.Status != "succeeded" {
		reason := service.Workers.State(acceptedOperation.ID).Err
		t.Fatal("restore failed", step.State, reason)
	}
	if len(step.Input.Ancestors) != 1 || step.Candidate != acceptedOperation.Result.VMID || step.State.Witness == step.ExpectedInstance {
		t.Fatal("restore did not preserve logical VM and replace instance")
	}
	var disks, slots int
	if err := service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM quota_resources WHERE kind='vm_disk' AND tenant_id='tenant'),(SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant')").Scan(&disks, &slots); err != nil || disks != 3 || slots != 2 {
		t.Fatal("restore lost old disk debt or allocated another slot", disks, slots, err)
	}
	if backend.Count("launch") != 3 {
		t.Fatal("restore duplicate launch")
	}
}

func targetFixture(t *testing.T) (*application.App, *testfixture.GenerationRuntime, operation.State, machine.ResourceStep) {
	t.Helper()
	service, backend, request, _ := testapp.TargetSetup(t)
	restored, err := service.Provisioning.AdmitRestore(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	step := testutil.AwaitColdWorker(t, service, restored.ID)
	return service, backend, restored, step
}
