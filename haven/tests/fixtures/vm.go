// Package fixtures holds the domain-level test scenarios shared by
// tests/component and tests/native. It sits at the top of the fixture
// layering: testfixture (mechanism) -> testapp (application assembly) ->
// this package. Nothing here may be imported by production code.
package fixtures

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

var lifecycleTestSlots = make(chan struct{}, 4)

// Each lifecycle case owns its store, files and injected runtime. Four concurrent
// cases bound temporary RAM/copy pressure while retaining every recovery matrix.
func ParallelLifecycle(t *testing.T) {
	t.Helper()
	t.Parallel()
	lifecycleTestSlots <- struct{}{}
	t.Cleanup(func() {
		<-lifecycleTestSlots
	})
}

func ColdService(t *testing.T, fault func(string) error) (*application.App, *testfixture.ColdRuntime) {
	t.Helper()
	return ColdServiceWithBackend(t, fault, nil)
}

func ColdServiceWithBackend(t *testing.T, fault func(string) error, factory func(*testfixture.ColdRuntime) loom.Backend) (*application.App, *testfixture.ColdRuntime) {
	t.Helper()
	return testapp.ColdServiceWithOptions(t, application.Options{
		Fault: fault,
	}, factory)
}

func ColdClaim(t *testing.T, service *application.App, key string) operation.Record {
	t.Helper()
	acceptedOperation, err := service.Provisioning.AdmitCold(context.Background(), machine.FreshRequest{
		TenantID:   "tenant",
		Key:        key,
		TemplateID: "cold",
	})
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation
}

func AwaitColdWorker(t *testing.T, service *application.App, id string) machine.ResourceStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, last, seen := worker.Active, worker.Err, worker.Completed
		step, err := machine.LoadResourceStep(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatalf("resource worker did not stop: phase=%s status=%s error=%v", step.State.Phase, step.Status, last)
		}
		time.Sleep(time.Millisecond)
	}
}

func ResumeColdService(t *testing.T, old *application.App, backend *testfixture.ColdRuntime) *application.App {
	t.Helper()
	return ResumeColdServiceWithFault(t, old, backend, nil)
}

func ResumeColdServiceWithFault(t *testing.T, old *application.App, backend *testfixture.ColdRuntime, fault func(string) error) *application.App {
	t.Helper()
	if err := old.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts := old.Options
	opts.Initialize = false
	opts.Fault = fault
	opts.Backend = backend
	service, err := application.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return service
}

func ColdResponsibility(t *testing.T, service *application.App, acceptedOperation operation.Record, witness bool) {
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

// Persistent fixture effects survive the controller child. This proves SQLite
// crash recovery and stable resource identity with injected host effects.
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

type BlockingCloseRuntime struct {
	*testfixture.ColdRuntime

	Entered chan struct{}
	Release <-chan struct{}
}

func (backend *BlockingCloseRuntime) Close() error {
	close(backend.Entered)
	<-backend.Release
	return nil
}
