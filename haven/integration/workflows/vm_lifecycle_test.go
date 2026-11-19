package workflows_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenStepAdmissionAtomicity(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, failure := range []string{
		"abort-quota",
		"ignore-version",
		"before-commit",
	} {
		t.Run(failure, func(t *testing.T) {
			var fault func(string) error
			if failure == "before-commit" {
				fault = func(point string) error {
					if point == "admission_before_commit" {
						return errs.ErrInterrupted
					}
					return nil
				}
			}
			service, backend := fixtures.ColdService(t, fault)
			switch failure {
			case "abort-quota":
				_, err := service.State.Database().Exec("CREATE TRIGGER deny_quota BEFORE INSERT ON quota_resources BEGIN SELECT RAISE(ABORT,'fixture quota rejection'); END")
				if err != nil {
					t.Fatal(err)
				}
			case "ignore-version":
				_, err := service.State.Database().Exec("CREATE TRIGGER deny_version BEFORE INSERT ON resource_versions BEGIN SELECT RAISE(IGNORE); END")
				if err != nil {
					t.Fatal(err)
				}
			}
			tables := []string{
				"sandboxes",
				"operation_steps",
				"vms",
				"instances",
				"resource_versions",
				"reservations",
				"quota_resources",
				"quota_refs",
				"resource_refs",
			}
			baseline := map[string]int{}
			for _, table := range tables {
				var count int
				if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
					t.Fatal(err)
				}
				baseline[table] = count
			}
			if _, err := service.Provisioning.AdmitCold(context.Background(), machine.FreshRequest{
				TenantID:   "tenant",
				Key:        "atomic",
				TemplateID: "cold",
			}); err == nil {
				t.Fatal("failed admission reported success")
			}
			for _, table := range tables {
				var count int
				if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != baseline[table] {
					t.Fatalf("partial admission in %s: %d %v", table, count, err)
				}
			}
			var operations int
			if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM operations WHERE idem_key='atomic'").Scan(&operations); err != nil || operations != 0 {
				t.Fatal("rejected operation persisted", operations, err)
			}
			if backend.Count("prepare") != 0 || backend.Count("launch") != 0 {
				t.Fatal("external effect preceded admission commit")
			}
		})
	}
	t.Run("committed-before-effects", func(t *testing.T) {
		service, backend := fixtures.ColdService(t, nil)
		backend.Before = func(_ string, resource loom.Resource) error {
			if !service.Admission.TryLock() {
				return fmt.Errorf("effect held the global admission lock")
			}
			service.Admission.Unlock()
			var count int
			if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM vms v JOIN instances i ON i.vm_id=v.id JOIN resource_versions r ON r.vm_id=v.id WHERE v.id=? AND i.id=?", resource.VMID, resource.Instance).Scan(&count); err != nil {
				return err
			}
			if count != 1 {
				return fmt.Errorf("effect did not have a committed candidate")
			}
			return nil
		}
		operation := fixtures.ColdClaim(t, service, "committed")
		step := fixtures.AwaitColdWorker(t, service, operation.ID)
		if step.Status != "succeeded" {
			t.Fatal("cold step did not complete", step.State)
		}
		coldResponsibility(t, service, operation, true)
		original, err := service.Provisioning.AdmitCold(context.Background(), machine.FreshRequest{
			TenantID:   "tenant",
			Key:        "committed",
			TemplateID: "cold",
		})
		if err != nil || original.ID != operation.ID {
			t.Fatal("same-key replay lost original admission", err)
		}
		if _, err = service.Provisioning.AdmitCold(context.Background(), machine.FreshRequest{
			TenantID:   "tenant",
			Key:        "committed",
			TemplateID: "changed",
		}); err == nil {
			t.Fatal("same key changed its request")
		}
	})
}

func TestHavenStepCrashRecovery(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, point := range []string{
		"admission_committed",
		"step_prepare_intent",
		"effect_prepared",
		"step_launch_intent",
		"effect_launched",
		"step_launched",
		"step_booted",
		"step_agent_ready",
		"completion_before_commit",
		"step_completed",
	} {
		t.Run(point, func(t *testing.T) {
			service, backend := fixtures.ColdService(t, func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			operation := fixtures.ColdClaim(t, service, "recover")
			if point != "admission_committed" {
				fixtures.AwaitColdWorker(t, service, operation.ID)
			}
			before, err := machine.LoadResourceStep(context.Background(), service.State.Database(), operation.ID)
			if err != nil {
				t.Fatal(err)
			}
			if backend.Count("launch") > 0 {
				file, err := os.OpenFile(before.Input.Resource.Disk, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, writeErr := file.WriteAt([]byte("guest writes survive"), 0)
				if err = errors.Join(writeErr, file.Close()); err != nil {
					t.Fatal(err)
				}
			}
			service = fixtures.ResumeColdService(t, service, backend)
			after := fixtures.AwaitColdWorker(t, service, operation.ID)
			if after.Status != "succeeded" || after.State.Witness != before.Input.Resource.Instance || after.Input.Resource.VMID != before.Input.Resource.VMID || backend.Count("launch") != 1 {
				t.Fatalf("recovery allocated another candidate: before=%+v after=%+v launches=%d", before.Input.Resource, after.State, backend.Count("launch"))
			}
			if point == "effect_launched" || point == "step_launched" || point == "step_booted" || point == "step_agent_ready" || point == "completion_before_commit" || point == "step_completed" {
				file, err := os.Open(after.Input.Resource.Disk)
				if err != nil {
					t.Fatal(err)
				}
				data := make([]byte, len("guest writes survive"))
				_, readErr := file.ReadAt(data, 0)
				err = errors.Join(readErr, file.Close())
				if err != nil || string(data) != "guest writes survive" {
					t.Fatal("running disk was rebuilt during replay", err)
				}
			}
			coldResponsibility(t, service, operation, true)
		})
	}
}

func TestHavenInstanceAtomicCompletion(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, write := range []struct {
		name      string
		event     string
		table     string
		condition string
	}{
		{"vm", "UPDATE", "vms", "NEW.state='running'"},
		{"instance", "UPDATE", "instances", "json_extract(NEW.body,'$.agent_ready')=1"},
		{"sandbox", "UPDATE", "sandboxes", "json_extract(NEW.body,'$.instance')!=''"},
		{"disk", "INSERT", "quota_resources", "NEW.kind='vm_disk'"},
		{"pending-ref", "DELETE", "quota_refs", "OLD.kind='pending_storage'"},
		{"pending-cost", "DELETE", "quota_resources", "OLD.kind='pending_storage'"},
		{"source-ref", "DELETE", "resource_refs", "OLD.holder_kind='step'"},
		{"witness", "UPDATE", "operation_steps", "NEW.status='succeeded'"},
		{"parent", "UPDATE", "operations", "json_extract(NEW.body,'$.resource_ready')=1"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture completion rejection'",
			"IGNORE",
		} {
			t.Run(write.name+"/"+strings.Split(mode, ",")[0], func(t *testing.T) {
				service, backend := fixtures.ColdService(t, nil)
				statement := fmt.Sprintf("CREATE TRIGGER reject_completion BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				operation := fixtures.ColdClaim(t, service, "completion")
				step := fixtures.AwaitColdWorker(t, service, operation.ID)
				if step.Status != "running" || step.State.Phase != "agent_ready" || step.State.Witness != "" {
					t.Fatalf("rejected SQL published a terminal witness: %+v", step.State)
				}
				coldResponsibility(t, service, operation, false)
				var ready int
				if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM instances WHERE json_extract(body,'$.agent_ready')=1").Scan(&ready); err != nil || ready != 0 {
					t.Fatal("instance became ready outside completion transaction", ready, err)
				}
				if backend.Count("launch") != 1 {
					t.Fatal("fixture did not reach the actual host effect")
				}
				if _, err := service.State.Database().Exec("DROP TRIGGER reject_completion"); err != nil {
					t.Fatal(err)
				}
				service = fixtures.ResumeColdService(t, service, backend)
				step = fixtures.AwaitColdWorker(t, service, operation.ID)
				if step.Status != "succeeded" || backend.Count("launch") != 1 {
					t.Fatal("result commit retry relaunched the VM", step.State)
				}
				coldResponsibility(t, service, operation, true)
			})
		}
	}
}

func TestHavenTerminalResultImmutable(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend := fixtures.ColdService(t, nil)
	backend.FailBoot = true
	failed := fixtures.ColdClaim(t, service, "failed")
	step := fixtures.AwaitColdWorker(t, service, failed.ID)
	if step.Status != "failed" || step.State.Witness != "" {
		t.Fatal("failure fabricated a witness")
	}
	original, err := service.Operations.GetOperation(context.Background(), "tenant", failed.ID)
	if err != nil || original.Status != "failed" {
		t.Fatal("original failure not fixed", err)
	}
	originalBytes := testfixture.Encode(original)
	backend.Mu.Lock()
	backend.FailBoot = false
	backend.Mu.Unlock()
	service = fixtures.ResumeColdService(t, service, backend)
	replayed, err := service.Provisioning.AdmitCold(context.Background(), machine.FreshRequest{
		TenantID:   "tenant",
		Key:        "failed",
		TemplateID: "cold",
	})
	if err != nil || !bytes.Equal(testfixture.Encode(replayed), originalBytes) || backend.Count("launch") != 1 {
		t.Fatal("failed key was re-executed or enriched", err)
	}
	if _, err = service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "unproved-delete",
		SandboxID: failed.Result.SandboxID,
	}); err == nil {
		t.Fatal("failed candidate supplied deletion authority")
	}
	coldResponsibility(t, service, failed, false)
	current := fixtures.ColdClaim(t, service, "new-generation")
	good := fixtures.AwaitColdWorker(t, service, current.ID)
	if good.Status != "succeeded" {
		t.Fatal("new request did not complete", good.State)
	}
	var witnessBefore []byte
	if err = service.State.Database().QueryRow("SELECT body FROM operation_steps WHERE operation_id=?", current.ID).Scan(&witnessBefore); err != nil {
		t.Fatal(err)
	}
	service = fixtures.ResumeColdServiceWithFault(t, service, backend, func(point string) error {
		if point == "release_admitted" {
			return errs.ErrInterrupted
		}
		return nil
	})
	release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release",
		SandboxID: current.Result.SandboxID,
	})
	if !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal("cleanup was not paused after its durable admission", err)
	}
	parentFailure, err := service.Operations.GetOperation(context.Background(), "tenant", current.ID)
	if err != nil || parentFailure.Status != "failed" {
		t.Fatal("released parent was not fixed before cleanup", err)
	}
	parentFailureBytes := testfixture.Encode(parentFailure)
	service = fixtures.ResumeColdService(t, service, backend)
	deleted := fixtures.AwaitColdWorker(t, service, release.ID)
	if deleted.Status != "succeeded" {
		t.Fatal("release did not retire its own responsibility", deleted.State)
	}
	parentAfterCleanup, err := service.Operations.GetOperation(context.Background(), "tenant", current.ID)
	if err != nil || !bytes.Equal(testfixture.Encode(parentAfterCleanup), parentFailureBytes) {
		t.Fatal("independent cleanup rewrote its failed parent's public result", err)
	}
	var witnessAfter []byte
	if err = service.State.Database().QueryRow("SELECT body FROM operation_steps WHERE operation_id=?", current.ID).Scan(&witnessAfter); err != nil || !bytes.Equal(witnessBefore, witnessAfter) {
		t.Fatal("cleanup rewrote historical generation proof", err)
	}
	old, err := service.Operations.GetOperation(context.Background(), "tenant", failed.ID)
	if err != nil || !bytes.Equal(testfixture.Encode(old), originalBytes) {
		t.Fatal("another generation/cleanup changed original failure", err)
	}
	var held int64
	if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id='tenant'").Scan(&held); err != nil || held != 64 {
		t.Fatal("cleanup refunded unrelated unknown allocation", held, err)
	}
	cached, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release",
		SandboxID: current.Result.SandboxID,
	})
	if err != nil || cached.ID != release.ID || cached.Status != "succeeded" {
		t.Fatal("release terminal result was not replayed", err)
	}
}

func TestHavenStepReleaseRecovery(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, point := range []string{
		"release_admitted",
		"effect_stopped",
		"effect_removed",
		"deletion_intent",
		"release_unlinked",
		"effect_files_removed",
		"release_completion_before_commit",
	} {
		t.Run(point, func(t *testing.T) {
			var armed bool
			service, backend := fixtures.ColdService(t, func(candidate string) error {
				if armed && candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			claim := fixtures.ColdClaim(t, service, "claim")
			if step := fixtures.AwaitColdWorker(t, service, claim.ID); step.Status != "succeeded" {
				t.Fatal(step.State)
			}
			armed = true
			release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
				TenantID:  "tenant",
				Key:       "release",
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil && !errors.Is(err, errs.ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "release_admitted" {
				fixtures.AwaitColdWorker(t, service, release.ID)
			}
			var storage int64
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE tenant_id='tenant'").Scan(&storage); err != nil || storage != 64 {
				t.Fatal("incomplete deletion refunded charge", storage, err)
			}
			service = fixtures.ResumeColdService(t, service, backend)
			step := fixtures.AwaitColdWorker(t, service, release.ID)
			if step.Status != "succeeded" {
				t.Fatal("release did not resume original cleanup", step.State)
			}
			var remaining int
			if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM quota_resources").Scan(&remaining); err != nil || remaining != 0 {
				t.Fatal("trusted cleanup did not retire responsibility", remaining, err)
			}
			if backend.Count("launch") != 1 {
				t.Fatal("cleanup started another VM")
			}
		})
	}
}

func TestHavenStepReleasePreservesReplacedDisk(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend := fixtures.ColdService(t, func(point string) error {
		if point == "release_admitted" {
			return errs.ErrInterrupted
		}
		return nil
	})
	claim := fixtures.ColdClaim(t, service, "claim")
	cold := fixtures.AwaitColdWorker(t, service, claim.ID)
	if cold.Status != "succeeded" {
		t.Fatal(cold.State)
	}
	release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release",
		SandboxID: claim.Result.SandboxID,
	})
	if !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal("release was not frozen before side effects", err)
	}
	disk := cold.Input.Resource.Disk
	if err = os.Rename(disk, filepath.Join(t.TempDir(), "original-disk")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(disk, []byte("unknown replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	service = fixtures.ResumeColdService(t, service, backend)
	step := fixtures.AwaitColdWorker(t, service, release.ID)
	if step.Status == "succeeded" {
		t.Fatal("unproved replacement was reclaimed")
	}
	data, err := os.ReadFile(disk)
	if err != nil || string(data) != "unknown replacement" {
		t.Fatal("replacement file changed", err)
	}
	var storage int64
	if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources").Scan(&storage); err != nil || storage != 64 {
		t.Fatal("uncertain replacement retired its charge", storage, err)
	}
}

func coldResponsibility(t *testing.T, service *application.App, acceptedOperation operation.Record, witness bool) {
	t.Helper()
	var slots, storage, pending, disks int64
	if err := service.State.Database().QueryRow("SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0),COALESCE(SUM(kind='pending_storage'),0),COALESCE(SUM(kind='vm_disk'),0) FROM quota_resources WHERE source_operation_id=?", acceptedOperation.ID).Scan(&slots, &storage, &pending, &disks); err != nil {
		t.Fatal(err)
	}
	if slots != 1 || storage != 64 || witness && (pending != 0 || disks != 1) || !witness && (pending != 1 || disks != 0) {
		t.Fatalf("responsibility changed without handoff: slots=%d storage=%d pending=%d disks=%d", slots, storage, pending, disks)
	}
	step, err := machine.LoadResourceStep(context.Background(), service.State.Database(), acceptedOperation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if witness != (step.State.Witness != "") || witness != (step.Status == "succeeded") {
		t.Fatalf("witness and step completion disagree: %+v", step.State)
	}
	var phase, instance string
	if err := service.State.Database().QueryRow("SELECT state,current_instance FROM vms WHERE id=?", acceptedOperation.Result.VMID).Scan(&phase, &instance); err != nil {
		t.Fatal(err)
	}
	if witness && (phase != "running" || instance != step.State.Witness) || !witness && phase == "running" {
		t.Fatal("VM and witness were not published together")
	}
	var sandboxState string
	if err := service.State.Database().QueryRow("SELECT state FROM sandboxes WHERE id=?", acceptedOperation.Result.SandboxID).Scan(&sandboxState); err != nil {
		t.Fatal(err)
	}
	if sandboxState != "provisioning" {
		t.Fatal("System ready was incorrectly published as sandbox delivery")
	}
	sandboxView, err := sandbox.LoadSandbox(context.Background(), service.State.Database(), acceptedOperation.Result.SandboxID, "tenant")
	if err != nil || witness && sandboxView.Instance != step.State.Witness || !witness && sandboxView.Instance != "" {
		t.Fatal("sandbox instance handoff was not atomic", err)
	}
	var vmHolder, imageHolder, stepHolders int
	if err := service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='sandbox' AND holder_id=?),(SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND holder_kind='vm' AND holder_id=?),(SELECT COUNT(*) FROM resource_refs WHERE holder_kind='step' AND holder_id=?)", acceptedOperation.Result.VMID, acceptedOperation.Result.SandboxID, acceptedOperation.Result.VMID, acceptedOperation.ID+":resource").Scan(&vmHolder, &imageHolder, &stepHolders); err != nil {
		t.Fatal(err)
	}
	if witness && (vmHolder != 1 || imageHolder != 1 || stepHolders != 0) || !witness && (vmHolder != 0 || imageHolder != 0 || stepHolders != 2) {
		t.Fatal("physical references were not transferred atomically", vmHolder, imageHolder, stepHolders)
	}
}
