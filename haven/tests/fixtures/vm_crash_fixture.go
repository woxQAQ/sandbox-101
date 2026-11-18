package fixtures

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
)

// Persistent fixture effects survive the controller child. This proves SQLite
// crash recovery and stable resource identity, not VMM/guest/KVM behavior.
type PersistentColdRuntime struct {
	*testfixture.ColdRuntime

	Directory string
}

type persistedColdEffect struct {
	Resource    loom.Resource    `json:"resource"`
	Observation loom.Observation `json:"observation"`
}

func (backend *PersistentColdRuntime) hydrate(resource loom.Resource) error {
	var effect persistedColdEffect
	err := loom.ReadJSON(filepath.Join(backend.Directory, resource.VMID+".json"), &effect)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(effect.Resource, resource) {
		return fmt.Errorf("fixture resource identity changed across controller crash")
	}
	backend.Mu.Lock()
	backend.Resources[resource.VMID] = effect.Resource
	backend.Observations[resource.VMID] = effect.Observation
	backend.Mu.Unlock()
	return nil
}

func (backend *PersistentColdRuntime) persist(resource loom.Resource) error {
	observation, err := backend.ColdRuntime.Inspect(context.Background(), resource)
	if err != nil {
		return err
	}
	return loom.WriteJSON(filepath.Join(backend.Directory, resource.VMID+".json"), persistedColdEffect{resource, observation})
}

func (backend *PersistentColdRuntime) Inspect(ctx context.Context, resource loom.Resource) (loom.Observation, error) {
	if err := backend.hydrate(resource); err != nil {
		return loom.Observation{}, err
	}
	return backend.ColdRuntime.Inspect(ctx, resource)
}

func (backend *PersistentColdRuntime) Launch(ctx context.Context, resource loom.Resource, restore bool) error {
	if err := backend.hydrate(resource); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(backend.Directory, "launches"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString("launch\n")
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	if err = backend.ColdRuntime.Launch(ctx, resource, restore); err != nil {
		return err
	}
	return backend.persist(resource)
}

func (backend *PersistentColdRuntime) Boot(ctx context.Context, resource loom.Resource, restore bool) error {
	if err := backend.hydrate(resource); err != nil {
		return err
	}
	if err := backend.ColdRuntime.Boot(ctx, resource, restore); err != nil {
		return err
	}
	return backend.persist(resource)
}

func RunAbruptExitChild(t *testing.T) {
	directory := os.Getenv("HAVEN_TEST_ABRUPT_DIRECTORY")
	if directory == "" {
		return
	}
	point := os.Getenv("HAVEN_TEST_ABRUPT_POINT")
	service, _ := ColdServiceWithBackend(t, func(candidate string) error {
		if candidate == point {
			os.Exit(86)
		}
		return nil
	}, func(backend *testfixture.ColdRuntime) loom.Backend {
		return &PersistentColdRuntime{
			ColdRuntime: backend,
			Directory:   directory,
		}
	})
	if err := loom.WriteJSON(filepath.Join(directory, "controller.json"), struct {
		Directory string
	}{service.State.Directory()}); err != nil {
		t.Fatal(err)
	}
	operation := ColdClaim(t, service, "abrupt")
	AwaitColdWorker(t, service, operation.ID)
	t.Fatal("abrupt checkpoint was not reached")
}
