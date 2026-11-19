package workflows_test

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

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/tests/fixtures"
)

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
				ColdRuntime: testfixture.NewColdRuntime(),
				Directory:   directory,
			}
			service, err := application.Open(application.Options{
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
			if err = service.State.Database().QueryRow("SELECT id FROM operations WHERE scope_kind='tenant' AND scope_id='tenant' AND idem_key='abrupt'").Scan(&operation); err != nil {
				t.Fatal(err)
			}
			step := fixtures.AwaitColdWorker(t, service, operation)
			if step.Status != "succeeded" || step.State.Witness != step.Input.Resource.Instance {
				t.Fatal("abrupt restart did not adopt the frozen instance", step.State)
			}
			calls, err := os.ReadFile(filepath.Join(directory, "launches"))
			if err != nil || string(calls) != "launch\n" {
				t.Fatalf("crash recovery called Launch again: %q %v", calls, err)
			}
			result, err := service.Operations.GetOperation(context.Background(), "tenant", operation)
			if err != nil {
				t.Fatal(err)
			}
			coldResponsibility(t, service, result, true)
		})
	}
}

// Persistent fixture effects survive the controller child. This proves SQLite
// crash recovery and stable resource identity with injected host effects.
type persistentColdRuntime struct {
	*testfixture.ColdRuntime

	Directory string
}

type persistedColdEffect struct {
	Resource    loom.Resource    `json:"resource"`
	Observation loom.Observation `json:"observation"`
}

func (backend *persistentColdRuntime) hydrate(resource loom.Resource) error {
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

func (backend *persistentColdRuntime) persist(resource loom.Resource) error {
	observation, err := backend.ColdRuntime.Inspect(context.Background(), resource)
	if err != nil {
		return err
	}
	return loom.WriteJSON(filepath.Join(backend.Directory, resource.VMID+".json"), persistedColdEffect{resource, observation})
}

func (backend *persistentColdRuntime) Inspect(ctx context.Context, resource loom.Resource) (loom.Observation, error) {
	if err := backend.hydrate(resource); err != nil {
		return loom.Observation{}, err
	}
	return backend.ColdRuntime.Inspect(ctx, resource)
}

func (backend *persistentColdRuntime) Launch(ctx context.Context, resource loom.Resource, restore bool) error {
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

func (backend *persistentColdRuntime) Boot(ctx context.Context, resource loom.Resource, restore bool) error {
	if err := backend.hydrate(resource); err != nil {
		return err
	}
	if err := backend.ColdRuntime.Boot(ctx, resource, restore); err != nil {
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
	service, _ := fixtures.ColdServiceWithBackend(t, func(candidate string) error {
		if candidate == point {
			os.Exit(86)
		}
		return nil
	}, func(backend *testfixture.ColdRuntime) loom.Backend {
		return &persistentColdRuntime{
			ColdRuntime: backend,
			Directory:   directory,
		}
	})
	if err := loom.WriteJSON(filepath.Join(directory, "controller.json"), struct {
		Directory string
	}{service.State.Directory()}); err != nil {
		t.Fatal(err)
	}
	operation := fixtures.ColdClaim(t, service, "abrupt")
	fixtures.AwaitColdWorker(t, service, operation.ID)
	t.Fatal("abrupt checkpoint was not reached")
}
