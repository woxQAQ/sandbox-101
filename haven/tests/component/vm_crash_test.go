package component

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenStepAbruptSQLiteRecovery(t *testing.T) {
	if os.Getenv("HAVEN_TEST_ABRUPT_DIRECTORY") != "" {
		fixtures.RunAbruptExitChild(t)
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
			backend := &fixtures.PersistentColdRuntime{
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
			fixtures.ColdResponsibility(t, service, result, true)
		})
	}
}
