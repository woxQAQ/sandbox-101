package testfixture

import (
	"context"
	"fmt"
	"sync"

	"github.com/woxQAQ/haven/loom"
)

type GenerationRuntime struct {
	*RestorableRuntime

	generations    sync.Mutex
	known          map[string]loom.Resource
	states         map[string]loom.Observation
	unknownSibling bool
}

func GenerationBackend(runtime *ColdRuntime) loom.Backend {
	return &GenerationRuntime{
		RestorableRuntime: &RestorableRuntime{runtime},
		known:             map[string]loom.Resource{},
		states:            map[string]loom.Observation{},
	}
}

func (backend *GenerationRuntime) Inspect(_ context.Context, r loom.Resource) (loom.Observation, error) {
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

func (backend *GenerationRuntime) Launch(ctx context.Context, r loom.Resource, restore bool) error {
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

func (backend *GenerationRuntime) Boot(ctx context.Context, r loom.Resource, restore bool) error {
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

func (backend *GenerationRuntime) Stop(_ context.Context, r loom.Resource) error {
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

func (backend *GenerationRuntime) SetNetworkEnabled(_ context.Context, r loom.Resource, on bool) error {
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

func (backend *GenerationRuntime) Remove(_ context.Context, r loom.Resource) error {
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

func (backend *GenerationRuntime) Pause(ctx context.Context, r loom.Resource) error {
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

func (backend *GenerationRuntime) Resume(ctx context.Context, r loom.Resource) error {
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
