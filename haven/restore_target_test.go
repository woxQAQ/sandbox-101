package haven

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/loom"
)

type generationRuntime struct {
	*restorableRuntime

	generations    sync.Mutex
	known          map[string]loom.Resource
	states         map[string]loom.Observation
	unknownSibling bool
}

func (backend *generationRuntime) Inspect(_ context.Context, r loom.Resource) (loom.Observation, error) {
	backend.generations.Lock()
	defer backend.generations.Unlock()
	if backend.unknownSibling {
		return loom.Observation{}, fmt.Errorf("fixture unknown sibling")
	}
	for id, resource := range backend.known {
		if resource.VMID == r.VMID && id != r.Instance && backend.states[id].Alive {
			return loom.Observation{}, fmt.Errorf("fixture live sibling")
		}
	}
	return backend.states[r.Instance], nil
}

func (backend *generationRuntime) Launch(ctx context.Context, r loom.Resource, restore bool) error {
	backend.generations.Lock()
	defer backend.generations.Unlock()
	backend.mu.Lock()
	backend.observations[r.VMID] = loom.Observation{}
	backend.mu.Unlock()
	if err := backend.coldRuntime.Launch(ctx, r, restore); err != nil {
		return err
	}
	backend.mu.Lock()
	observation := backend.observations[r.VMID]
	backend.mu.Unlock()
	backend.known[r.Instance] = r
	backend.states[r.Instance] = observation
	return nil
}

func (backend *generationRuntime) Boot(ctx context.Context, r loom.Resource, restore bool) error {
	backend.generations.Lock()
	defer backend.generations.Unlock()
	backend.mu.Lock()
	backend.observations[r.VMID] = backend.states[r.Instance]
	backend.mu.Unlock()
	if err := backend.coldRuntime.Boot(ctx, r, restore); err != nil {
		return err
	}
	backend.mu.Lock()
	backend.states[r.Instance] = backend.observations[r.VMID]
	backend.mu.Unlock()
	return nil
}

func (backend *generationRuntime) Stop(_ context.Context, r loom.Resource) error {
	if err := backend.effect("stop", r); err != nil {
		return err
	}
	backend.generations.Lock()
	defer backend.generations.Unlock()
	backend.states[r.Instance] = loom.Observation{}
	backend.mu.Lock()
	backend.observations[r.VMID] = loom.Observation{}
	backend.mu.Unlock()
	return nil
}

func (backend *generationRuntime) Network(_ context.Context, r loom.Resource, on bool) error {
	if err := backend.effect("network", r); err != nil {
		return err
	}
	backend.generations.Lock()
	defer backend.generations.Unlock()
	for id, resource := range backend.known {
		if resource.VMID == r.VMID && id != r.Instance && backend.states[id].Alive {
			return fmt.Errorf("fixture cannot mutate devices with live sibling")
		}
	}
	observation := backend.states[r.Instance]
	observation.Network = on
	backend.states[r.Instance] = observation
	return nil
}

func (backend *generationRuntime) Remove(_ context.Context, r loom.Resource) error {
	if err := backend.effect("remove", r); err != nil {
		return err
	}
	observation, err := backend.Inspect(context.Background(), r)
	if err != nil {
		return err
	}
	if observation.Alive {
		return fmt.Errorf("fixture removal of live instance")
	}
	return nil
}

func (backend *generationRuntime) Pause(ctx context.Context, r loom.Resource) error {
	backend.generations.Lock()
	defer backend.generations.Unlock()
	if err := backend.coldRuntime.Pause(ctx, r); err != nil {
		return err
	}
	observation := backend.states[r.Instance]
	observation.Paused = true
	backend.states[r.Instance] = observation
	return nil
}

func (backend *generationRuntime) Resume(ctx context.Context, r loom.Resource) error {
	backend.generations.Lock()
	defer backend.generations.Unlock()
	if err := backend.coldRuntime.Resume(ctx, r); err != nil {
		return err
	}
	observation := backend.states[r.Instance]
	observation.Paused = false
	backend.states[r.Instance] = observation
	return nil
}

func targetSetup(t *testing.T) (*Service, *generationRuntime, RestoreRequest, resourceStep) {
	t.Helper()
	service, base := coldService(t, nil)
	backend := &generationRuntime{
		restorableRuntime: &restorableRuntime{base},
		known:             map[string]loom.Resource{},
		states:            map[string]loom.Observation{},
	}
	service.opts.Backend = backend
	capture, _ := captureReady(t, service, "target-source")
	if proof := awaitCapture(t, service, capture.ID); proof.Status != "succeeded" {
		t.Fatal(proof.State)
	}
	target := coldClaim(t, service, "target")
	ready := awaitColdWorker(t, service, target.ID)
	if ready.Status != "succeeded" {
		t.Fatal(ready.State)
	}
	stop, err := service.AdmitStop(context.Background(), StopRequest{
		TenantID:         "tenant",
		Key:              "target-stop",
		SandboxID:        target.Result.SandboxID,
		ExpectedInstance: ready.State.Witness,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		service.mu.Lock()
		active := service.active[stop.ID]
		_, seen := service.lastErrors[stop.ID]
		service.mu.Unlock()
		if !active && seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stop did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	result, err := service.GetOperation(context.Background(), "tenant", stop.ID)
	if err != nil || result.Status != "succeeded" {
		service.mu.Lock()
		reason := service.lastErrors[stop.ID]
		service.mu.Unlock()
		t.Fatal("stop did not succeed", result, reason, err)
	}
	request := RestoreRequest{
		TenantID:         "tenant",
		Key:              "restore",
		SandboxID:        target.Result.SandboxID,
		SnapshotID:       capture.Result.SnapshotID,
		ExpectedInstance: ready.State.Witness,
		StopOperationID:  stop.ID,
	}
	return service, backend, request, ready
}

func targetFixture(t *testing.T) (*Service, *generationRuntime, Operation, resourceStep) {
	t.Helper()
	service, backend, request, _ := targetSetup(t)
	restored, err := service.AdmitRestore(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	step := awaitColdWorker(t, service, restored.ID)
	return service, backend, restored, step
}

func TestHavenRestoreStoppedTargetDiscard(t *testing.T) {
	parallelLifecycle(t)
	for _, point := range []string{
		"restore_candidate_copied",
		"restore_resources_ready",
		"effect_launched",
	} {
		t.Run(point, func(t *testing.T) {
			service, backend, request, prior := targetSetup(t)
			service.opts.Fault = func(candidate string) error {
				if point == "restore_candidate_copied" && candidate == point {
					return ErrInterrupted
				}
				return nil
			}
			backend.before = func(name string, _ loom.Resource) error {
				if point == "restore_resources_ready" && name == "launch" || point == "effect_launched" && name == "boot" {
					return fmt.Errorf("fixture target restore effect failure")
				}
				return nil
			}
			operation, err := service.AdmitRestore(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			failed := awaitColdWorker(t, service, operation.ID)
			if point == "restore_candidate_copied" {
				service = reopenCloneService(t, service, backend)
				failed = awaitColdWorker(t, service, operation.ID)
			}
			backend.before = nil
			if failed.Status != "failed" || failed.State.Witness != "" {
				t.Fatal("restore failure promoted", failed.State)
			}
			var cost int
			if err = service.store.db.QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id IN (?,?)", prior.OperationID, operation.ID).Scan(&cost); err != nil || cost != 128 {
				t.Fatal("failure lost old/new disk debt", cost, err)
			}
			service.opts.Fault = nil
			discard, err := service.AdmitDiscard(context.Background(), DiscardRequest{
				TenantID:          "tenant",
				Key:               "discard-target",
				SandboxID:         request.SandboxID,
				FailedOperationID: operation.ID,
				ExpectedInstance:  failed.Input.Resource.Instance,
			})
			if err != nil {
				t.Fatal(err)
			}
			deleted := awaitDiscard(t, service, discard.ID)
			if deleted.Status != "succeeded" {
				service.mu.Lock()
				reason := service.lastErrors[discard.ID]
				service.mu.Unlock()
				t.Fatal("multi-version discard failed", deleted.State, reason)
			}
			original, err := loadStep(context.Background(), service.store.db, operation.ID)
			if err != nil || original.Status != "failed" || original.State.Witness != "" {
				t.Fatal("discard rewrote failure", original.State, err)
			}
			if backend.count("launch") > 3 {
				t.Fatal("discard relaunched a generation")
			}
		})
	}
}

func TestHavenRestoreStoppedTargetRecovery(t *testing.T) {
	parallelLifecycle(t)
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
			service, backend, request, prior := targetSetup(t)
			service.opts.Fault = func(candidate string) error {
				if candidate == point {
					return ErrInterrupted
				}
				return nil
			}
			operation, err := service.AdmitRestore(context.Background(), request)
			if err != nil && !errors.Is(err, ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "restore_admitted" {
				awaitColdWorker(t, service, operation.ID)
			}
			service = reopenCloneService(t, service, backend)
			step := awaitColdWorker(t, service, operation.ID)
			incomplete := point == "restore_candidate_prepared" || point == "restore_candidate_copied" || point == "restore_candidate_renamed" || point == "restore_candidate_synced"
			if incomplete && (step.Status != "failed" || !step.State.RestoreBlocked) || !incomplete && step.Status != "succeeded" {
				t.Fatal("target recovery state", step.State)
			}
			historical, err := loadStep(context.Background(), service.store.db, prior.OperationID)
			if err != nil || !reflect.DeepEqual(historical, prior) {
				t.Fatal("restore changed previous published result", err)
			}
			var cost int
			if err = service.store.db.QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id IN (?,?)", prior.OperationID, operation.ID).Scan(&cost); err != nil || cost != 128 {
				t.Fatal("recovery lost disk debt", cost, err)
			}
			launches := 3
			if incomplete {
				launches = 2
			}
			if backend.count("launch") != launches {
				t.Fatal("recovery launch count", backend.count("launch"), launches)
			}
		})
	}
}

func awaitStop(t *testing.T, service *Service, id string) stopStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		service.mu.Lock()
		active := service.active[id]
		_, seen := service.lastErrors[id]
		service.mu.Unlock()
		if !active && seen {
			step, err := loadStop(context.Background(), service.store.db, id)
			if err != nil {
				t.Fatal(err)
			}
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("stop worker timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHavenRestoreStoppedTargetRepeated(t *testing.T) {
	parallelLifecycle(t)
	service, backend, operation, first := targetFixture(t)
	if first.Status != "succeeded" {
		t.Fatal(first.State)
	}
	stop, err := service.AdmitStop(context.Background(), StopRequest{
		TenantID:         "tenant",
		Key:              "stop-again",
		SandboxID:        operation.Result.SandboxID,
		ExpectedInstance: first.State.Witness,
	})
	if err != nil {
		t.Fatal(err)
	}
	if proof := awaitStop(t, service, stop.ID); proof.Status != "succeeded" {
		t.Fatal("second stop failed", proof)
	}
	second, err := service.AdmitRestore(context.Background(), RestoreRequest{
		TenantID:         "tenant",
		Key:              "restore-again",
		SandboxID:        operation.Result.SandboxID,
		SnapshotID:       first.Input.Restore.Snapshot.ID,
		ExpectedInstance: first.State.Witness,
		StopOperationID:  stop.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	restored := awaitColdWorker(t, service, second.ID)
	if restored.Status != "succeeded" || len(restored.Input.Ancestors) != 2 {
		service.mu.Lock()
		reason := service.lastErrors[second.ID]
		service.mu.Unlock()
		t.Fatal(restored.State, reason)
	}
	var backing int
	if err = service.store.db.QueryRow("SELECT COUNT(*) FROM backing WHERE vm_id=?", restored.Candidate).Scan(&backing); err != nil || backing != 2 {
		t.Fatal("multiple retained RAM versions lost", backing, err)
	}
	release, err := service.AdmitRelease(context.Background(), ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release-generations",
		SandboxID: operation.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if proof := awaitColdWorker(t, service, release.ID); proof.Status != "succeeded" {
		service.mu.Lock()
		reason := service.lastErrors[release.ID]
		service.mu.Unlock()
		t.Fatal(proof.State, reason)
	}
	if backend.count("launch") != 4 {
		t.Fatal("historical generation relaunched")
	}
}

func TestHavenRestoreStoppedTargetAdmissionAtomicity(t *testing.T) {
	parallelLifecycle(t)
	for _, write := range []struct {
		event, table, condition string
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
				service, _, request, prior := targetSetup(t)
				statement := fmt.Sprintf("CREATE TRIGGER deny_target_admission BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.store.db.Exec(statement); err != nil {
					t.Fatal(err)
				}
				if _, err := service.AdmitRestore(context.Background(), request); err == nil {
					t.Fatal("admission denial ignored")
				}
				var versions, instances, operations int
				if err := service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM resource_versions WHERE vm_id=?),(SELECT COUNT(*) FROM instances WHERE vm_id=?),(SELECT COUNT(*) FROM operations WHERE idem_key=?)", prior.Candidate, prior.Candidate, request.Key).Scan(&versions, &instances, &operations); err != nil || versions != 1 || instances != 1 || operations != 0 {
					t.Fatal("admission partly committed", versions, instances, operations, err)
				}
			})
		}
	}
}

func TestHavenRestoreStoppedTargetRetirementAtomicity(t *testing.T) {
	parallelLifecycle(t)
	for _, mode := range []string{
		"ABORT,'fixture generation retirement rejection'",
		"IGNORE",
	} {
		t.Run(mode, func(t *testing.T) {
			service, backend, operation, step := targetFixture(t)
			if step.Status != "succeeded" {
				t.Fatal(step.State)
			}
			ancestor := step.Input.Ancestors[0].OperationID
			statement := fmt.Sprintf("CREATE TRIGGER deny_old_generation_retirement BEFORE DELETE ON instances WHEN OLD.operation_id='%s' BEGIN SELECT RAISE(%s); END", ancestor, mode)
			if _, err := service.store.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			release, err := service.AdmitRelease(context.Background(), ReleaseRequest{
				TenantID:  "tenant",
				Key:       "release-restored",
				SandboxID: operation.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			pending := awaitColdWorker(t, service, release.ID)
			if terminal(pending.Status) {
				t.Fatal("retirement denial committed", pending.State)
			}
			var instances, versions, cost int
			if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM instances WHERE vm_id=?),(SELECT COUNT(*) FROM resource_versions WHERE vm_id=?),(SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id IN (?,?))", step.Candidate, step.Candidate, ancestor, operation.ID).Scan(&instances, &versions, &cost); err != nil || instances != 2 || versions != 2 || cost != 128 {
				t.Fatal("retirement denial partially removed history/debt", instances, versions, cost, err)
			}
			if _, err = service.store.db.Exec("DROP TRIGGER deny_old_generation_retirement"); err != nil {
				t.Fatal(err)
			}
			service = reopenCloneService(t, service, backend)
			done := awaitColdWorker(t, service, release.ID)
			if done.Status != "succeeded" {
				service.mu.Lock()
				reason := service.lastErrors[release.ID]
				service.mu.Unlock()
				t.Fatal(done.State, reason)
			}
		})
	}
}

func TestHavenRestoreStoppedTargetRelease(t *testing.T) {
	parallelLifecycle(t)
	service, backend, operation, step := targetFixture(t)
	if step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	release, err := service.AdmitRelease(context.Background(), ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release-restored",
		SandboxID: operation.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	deleted := awaitColdWorker(t, service, release.ID)
	if deleted.Status != "succeeded" {
		service.mu.Lock()
		reason := service.lastErrors[release.ID]
		service.mu.Unlock()
		t.Fatal("multi-version release failed", deleted.State, reason)
	}
	var versions, instances int
	if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM resource_versions WHERE vm_id=?),(SELECT COUNT(*) FROM instances WHERE vm_id=?)", step.Candidate, step.Candidate).Scan(&versions, &instances); err != nil || versions != 0 || instances != 0 {
		t.Fatal("historical versions were not retired", versions, instances, err)
	}
	if backend.count("launch") != 3 {
		t.Fatal("cleanup launched an old instance")
	}
}

func TestHavenRestoreStoppedTarget(t *testing.T) {
	parallelLifecycle(t)
	service, backend, operation, step := targetFixture(t)
	if step.Status != "succeeded" {
		service.mu.Lock()
		reason := service.lastErrors[operation.ID]
		service.mu.Unlock()
		t.Fatal("restore failed", step.State, reason)
	}
	if len(step.Input.Ancestors) != 1 || step.Candidate != operation.Result.VMID || step.State.Witness == step.ExpectedInstance {
		t.Fatal("restore did not preserve logical VM and replace instance")
	}
	var disks, slots int
	if err := service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM quota_resources WHERE kind='vm_disk' AND tenant_id='tenant'),(SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant')").Scan(&disks, &slots); err != nil || disks != 3 || slots != 2 {
		t.Fatal("restore lost old disk debt or allocated another slot", disks, slots, err)
	}
	if backend.count("launch") != 3 {
		t.Fatal("restore duplicate launch")
	}
}
