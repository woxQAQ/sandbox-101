package haven

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/loom"
)

// Persistent fixture effects survive the controller child. This proves SQLite
// crash recovery and stable resource identity, not VMM/guest/KVM behavior.
type persistentColdRuntime struct {
	*coldRuntime

	directory string
}

type persistedColdEffect struct {
	Resource    loom.Resource    `json:"resource"`
	Observation loom.Observation `json:"observation"`
}

func (backend *persistentColdRuntime) hydrate(resource loom.Resource) error {
	var effect persistedColdEffect
	err := loom.ReadJSON(filepath.Join(backend.directory, resource.VMID+".json"), &effect)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(effect.Resource, resource) {
		return fmt.Errorf("fixture resource identity changed across controller crash")
	}
	backend.mu.Lock()
	backend.resources[resource.VMID] = effect.Resource
	backend.observations[resource.VMID] = effect.Observation
	backend.mu.Unlock()
	return nil
}

func (backend *persistentColdRuntime) persist(resource loom.Resource) error {
	observation, err := backend.coldRuntime.Inspect(context.Background(), resource)
	if err != nil {
		return err
	}
	return loom.WriteJSON(filepath.Join(backend.directory, resource.VMID+".json"), persistedColdEffect{resource, observation})
}

func (backend *persistentColdRuntime) Inspect(ctx context.Context, resource loom.Resource) (loom.Observation, error) {
	if err := backend.hydrate(resource); err != nil {
		return loom.Observation{}, err
	}
	return backend.coldRuntime.Inspect(ctx, resource)
}

func (backend *persistentColdRuntime) Launch(ctx context.Context, resource loom.Resource, restore bool) error {
	if err := backend.hydrate(resource); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(backend.directory, "launches"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString("launch\n")
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	if err = backend.coldRuntime.Launch(ctx, resource, restore); err != nil {
		return err
	}
	return backend.persist(resource)
}

func (backend *persistentColdRuntime) Boot(ctx context.Context, resource loom.Resource, restore bool) error {
	if err := backend.hydrate(resource); err != nil {
		return err
	}
	if err := backend.coldRuntime.Boot(ctx, resource, restore); err != nil {
		return err
	}
	return backend.persist(resource)
}

func runAbruptExitChild(t *testing.T) {
	directory := os.Getenv("HAVEN_TEST_ABRUPT_DIRECTORY")
	if directory == "" {
		return
	}
	point := os.Getenv("HAVEN_TEST_ABRUPT_POINT")
	service, backend := coldService(t, func(candidate string) error {
		if candidate == point {
			os.Exit(86)
		}
		return nil
	})
	service.opts.Backend = &persistentColdRuntime{
		coldRuntime: backend,
		directory:   directory,
	}
	if err := loom.WriteJSON(filepath.Join(directory, "controller.json"), struct {
		Directory string
	}{service.store.directory}); err != nil {
		t.Fatal(err)
	}
	operation := coldClaim(t, service, "abrupt")
	awaitColdWorker(t, service, operation.ID)
	t.Fatal("abrupt checkpoint was not reached")
}

func TestHavenStepAbruptSQLiteRecovery(t *testing.T) {
	if os.Getenv("HAVEN_TEST_ABRUPT_DIRECTORY") != "" {
		runAbruptExitChild(t)
		return
	}
	for _, point := range []string{
		"effect_launched",
		"completion_before_commit",
		"step_completed",
	} {
		t.Run(point, func(t *testing.T) {
			directory := t.TempDir()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable, "-test.run=^TestHavenStepAbruptSQLiteRecovery$", "-test.count=1", "-test.timeout=25s")
			command.Env = append(os.Environ(), "HAVEN_TEST_ABRUPT_DIRECTORY="+directory, "HAVEN_TEST_ABRUPT_POINT="+point, "TMPDIR="+directory)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatalf("controller did not abruptly exit at %s: %v %s", point, err, output)
			}
			var controller struct {
				Directory string
			}
			if err = loom.ReadJSON(filepath.Join(directory, "controller.json"), &controller); err != nil {
				t.Fatal(err)
			}
			backend := &persistentColdRuntime{
				coldRuntime: newColdRuntime(),
				directory:   directory,
			}
			service, err := Open(Options{
				StateDir:     controller.Directory,
				Backend:      backend,
				MaxMemoryMiB: 8192,
				MaxVCPUs:     64,
				SafetyBytes:  1,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = service.Close(context.Background())
			})
			var operation string
			if err = service.store.db.QueryRow("SELECT id FROM operations WHERE scope_kind='tenant' AND scope_id='tenant' AND idem_key='abrupt'").Scan(&operation); err != nil {
				t.Fatal(err)
			}
			step := awaitColdWorker(t, service, operation)
			if step.Status != "succeeded" || step.State.Witness != step.Input.Resource.Instance {
				t.Fatal("abrupt restart did not adopt the frozen instance", step.State)
			}
			calls, err := os.ReadFile(filepath.Join(directory, "launches"))
			if err != nil || string(calls) != "launch\n" {
				t.Fatalf("crash recovery called Launch again: %q %v", calls, err)
			}
			result, err := service.GetOperation(context.Background(), "tenant", operation)
			if err != nil {
				t.Fatal(err)
			}
			coldResponsibility(t, service, result, true)
		})
	}
}
