package fixtures

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

type generationRuntime struct {
	*RestorableRuntime

	generations    sync.Mutex
	known          map[string]loom.Resource
	states         map[string]loom.Observation
	unknownSibling bool
}

func GenerationBackend(runtime *testfixture.ColdRuntime) loom.Backend {
	return &generationRuntime{
		RestorableRuntime: &RestorableRuntime{runtime},
		known:             map[string]loom.Resource{},
		states:            map[string]loom.Observation{},
	}
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
	backend.Mu.Lock()
	backend.Observations[r.VMID] = loom.Observation{}
	backend.Mu.Unlock()
	if err := backend.ColdRuntime.Launch(ctx, r, restore); err != nil {
		return err
	}
	backend.Mu.Lock()
	observation := backend.Observations[r.VMID]
	backend.Mu.Unlock()
	backend.known[r.Instance] = r
	backend.states[r.Instance] = observation
	return nil
}

func (backend *generationRuntime) Boot(ctx context.Context, r loom.Resource, restore bool) error {
	backend.generations.Lock()
	defer backend.generations.Unlock()
	backend.Mu.Lock()
	backend.Observations[r.VMID] = backend.states[r.Instance]
	backend.Mu.Unlock()
	if err := backend.ColdRuntime.Boot(ctx, r, restore); err != nil {
		return err
	}
	backend.Mu.Lock()
	backend.states[r.Instance] = backend.Observations[r.VMID]
	backend.Mu.Unlock()
	return nil
}

func (backend *generationRuntime) Stop(_ context.Context, r loom.Resource) error {
	if err := backend.Effect("stop", r); err != nil {
		return err
	}
	backend.generations.Lock()
	defer backend.generations.Unlock()
	backend.states[r.Instance] = loom.Observation{}
	backend.Mu.Lock()
	backend.Observations[r.VMID] = loom.Observation{}
	backend.Mu.Unlock()
	return nil
}

func (backend *generationRuntime) Network(_ context.Context, r loom.Resource, on bool) error {
	if err := backend.Effect("network", r); err != nil {
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
	if err := backend.Effect("remove", r); err != nil {
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
	if err := backend.ColdRuntime.Pause(ctx, r); err != nil {
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
	if err := backend.ColdRuntime.Resume(ctx, r); err != nil {
		return err
	}
	observation := backend.states[r.Instance]
	observation.Paused = false
	backend.states[r.Instance] = observation
	return nil
}

func TargetSetup(t *testing.T) (*application.App, *generationRuntime, machine.RestoreRequest, machine.ResourceStep) {
	t.Helper()
	return TargetSetupWithFault(t, nil)
}

func TargetSetupWithFault(t *testing.T, fault func(string) error) (*application.App, *generationRuntime, machine.RestoreRequest, machine.ResourceStep) {
	t.Helper()
	service, _ := ColdServiceWithBackend(t, fault, GenerationBackend)
	backend := service.Options.Backend.(*generationRuntime)
	capture, _ := CaptureReady(t, service, "target-source")
	if proof := AwaitCapture(t, service, capture.ID); proof.Status != "succeeded" {
		t.Fatal(proof.State)
	}
	target := ColdClaim(t, service, "target")
	ready := AwaitColdWorker(t, service, target.ID)
	if ready.Status != "succeeded" {
		t.Fatal(ready.State)
	}
	stop, err := service.VMStops.AdmitStop(context.Background(), machine.StopRequest{
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
		worker := service.Workers.State(stop.ID)
		active, seen := worker.Active, worker.Completed
		if !active && seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stop did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	result, err := service.Operations.GetOperation(context.Background(), "tenant", stop.ID)
	if err != nil || result.Status != "succeeded" {
		reason := service.Workers.State(stop.ID).Err
		t.Fatal("stop did not succeed", result, reason, err)
	}
	request := machine.RestoreRequest{
		TenantID:         "tenant",
		Key:              "restore",
		SandboxID:        target.Result.SandboxID,
		SnapshotID:       capture.Result.SnapshotID,
		ExpectedInstance: ready.State.Witness,
		StopOperationID:  stop.ID,
	}
	return service, backend, request, ready
}

func TargetFixture(t *testing.T) (*application.App, *generationRuntime, operation.Record, machine.ResourceStep) {
	t.Helper()
	service, backend, request, _ := TargetSetup(t)
	restored, err := service.VMProvisioning.AdmitRestore(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	step := AwaitColdWorker(t, service, restored.ID)
	return service, backend, restored, step
}

func AwaitStop(t *testing.T, service *application.App, id string) machine.StopStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, seen := worker.Active, worker.Completed
		if !active && seen {
			step, err := machine.LoadStop(context.Background(), service.State.Database(), id)
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
