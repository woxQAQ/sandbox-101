package haven

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

// The fixture models only host effects and writes its own private disk/socket
// entries. All admissions, journals, witnesses and accounting use real SQLite.
type coldRuntime struct {
	mu           sync.Mutex
	resources    map[string]loom.Resource
	observations map[string]loom.Observation
	counts       map[string]int
	failBoot     bool
	inspectError error
	before       func(string, loom.Resource) error
}

func newColdRuntime() *coldRuntime {
	return &coldRuntime{
		resources:    map[string]loom.Resource{},
		observations: map[string]loom.Observation{},
		counts:       map[string]int{},
	}
}

func (backend *coldRuntime) Compatibility() string {
	return "explicit-cold-fixture"
}

func (backend *coldRuntime) Check(context.Context, loom.Config) error {
	return nil
}

func (backend *coldRuntime) effect(name string, resource loom.Resource) error {
	if backend.before != nil {
		if err := backend.before(name, resource); err != nil {
			return err
		}
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.counts[name]++
	return nil
}

func (backend *coldRuntime) Prepare(ctx context.Context, resource loom.Resource) error {
	if err := backend.effect("prepare", resource); err != nil {
		return err
	}
	for _, directory := range []string{
		filepath.Dir(resource.Disk),
		filepath.Join(resource.Dir, "capture"),
	} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(resource.Disk, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if os.IsExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	err = file.Truncate(resource.Config.Shape.DiskMiB << 20)
	return errors.Join(err, file.Close(), ctx.Err())
}

func (backend *coldRuntime) Inspect(_ context.Context, resource loom.Resource) (loom.Observation, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.inspectError != nil {
		return loom.Observation{}, backend.inspectError
	}
	if known, exists := backend.resources[resource.VMID]; exists && !reflect.DeepEqual(known, resource) {
		return loom.Observation{}, fmt.Errorf("unknown instance or sibling")
	}
	return backend.observations[resource.VMID], nil
}

func (backend *coldRuntime) Launch(_ context.Context, resource loom.Resource, _ bool) error {
	if err := backend.effect("launch", resource); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.observations[resource.VMID].Alive {
		return fmt.Errorf("duplicate live launch")
	}
	for _, directory := range []string{
		filepath.Join(resource.RunDir, "io"),
		filepath.Join(resource.RunDir, "root"),
	} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return err
		}
	}
	for _, name := range []string{
		"launch.json",
		"process.json",
		"console.log",
	} {
		if err := os.WriteFile(filepath.Join(resource.RunDir, name), []byte("owned fixture"), 0o600); err != nil {
			return err
		}
	}
	// Bind through an owned directory fd so the fixture can exercise a genuine
	// stale Unix socket even with Go's long test-directory prefixes.
	directory, err := unix.Open(filepath.Join(resource.RunDir, "io"), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(directory)
	socket, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(socket)
	if err = unix.Bind(socket, &unix.SockaddrUnix{
		Name: fmt.Sprintf("/proc/self/fd/%d/api.sock", directory),
	}); err != nil {
		return err
	}
	backend.resources[resource.VMID] = resource
	backend.observations[resource.VMID] = loom.Observation{
		Alive:     true,
		PID:       1000 + backend.counts["launch"],
		StartTime: uint64(backend.counts["launch"]),
	}
	return nil
}

func (backend *coldRuntime) Boot(_ context.Context, resource loom.Resource, _ bool) error {
	if err := backend.effect("boot", resource); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.failBoot {
		return fmt.Errorf("fixture boot failure")
	}
	observation := backend.observations[resource.VMID]
	observation.Booted, observation.Configured = true, true
	backend.observations[resource.VMID] = observation
	return nil
}

func (backend *coldRuntime) Agent(_ context.Context, resource loom.Resource, _ bool) error {
	return backend.effect("agent", resource)
}

func (backend *coldRuntime) Stop(_ context.Context, resource loom.Resource) error {
	if err := backend.effect("stop", resource); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if known, exists := backend.resources[resource.VMID]; exists && !reflect.DeepEqual(known, resource) {
		return fmt.Errorf("refusing another instance")
	}
	backend.observations[resource.VMID] = loom.Observation{}
	return nil
}

func (backend *coldRuntime) Network(_ context.Context, resource loom.Resource, enabled bool) error {
	if err := backend.effect("network", resource); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	observation := backend.observations[resource.VMID]
	observation.Network = enabled
	backend.observations[resource.VMID] = observation
	return nil
}

func (backend *coldRuntime) Remove(_ context.Context, resource loom.Resource) error {
	if err := backend.effect("remove", resource); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.observations[resource.VMID].Alive {
		return fmt.Errorf("refusing live removal")
	}
	return nil
}

func (backend *coldRuntime) Pause(_ context.Context, resource loom.Resource) error {
	if err := backend.effect("pause", resource); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	observation := backend.observations[resource.VMID]
	if !observation.Alive {
		return fmt.Errorf("fixture source is not alive")
	}
	observation.Paused = true
	backend.observations[resource.VMID] = observation
	return nil
}

func (backend *coldRuntime) Resume(_ context.Context, resource loom.Resource) error {
	if err := backend.effect("resume", resource); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	observation := backend.observations[resource.VMID]
	if !observation.Alive {
		return fmt.Errorf("fixture source is not alive")
	}
	observation.Paused = false
	backend.observations[resource.VMID] = observation
	return nil
}

func (backend *coldRuntime) Capture(_ context.Context, resource loom.Resource, directory string) error {
	if err := backend.effect("capture", resource); err != nil {
		return err
	}
	observation, err := backend.Inspect(context.Background(), resource)
	if err != nil {
		return err
	}
	if !observation.Alive || !observation.Paused {
		return fmt.Errorf("fixture capture requires paused source")
	}
	for name, body := range map[string]string{
		"config.json":   "{}",
		"state.json":    "{}",
		"memory-ranges": "frozen memory",
	} {
		if err = os.WriteFile(filepath.Join(directory, name), []byte(body), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (backend *coldRuntime) count(name string) int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.counts[name]
}

var lifecycleTestSlots = make(chan struct{}, 4)

// Each lifecycle case owns its store, files and injected runtime. Four concurrent
// cases bound temporary RAM/copy pressure while retaining every recovery matrix.
func parallelLifecycle(t *testing.T) {
	t.Helper()
	t.Parallel()
	lifecycleTestSlots <- struct{}{}
	t.Cleanup(func() {
		<-lifecycleTestSlots
	})
}

func coldService(t *testing.T, fault func(string) error) (*Service, *coldRuntime) {
	t.Helper()
	backend := newColdRuntime()
	service, err := Open(Options{
		StateDir:     filepath.Join(t.TempDir(), "state"),
		Initialize:   true,
		Backend:      backend,
		MaxMemoryMiB: 8192,
		MaxVCPUs:     64,
		SafetyBytes:  1,
		Fault:        fault,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	manifest := host.ImageManifest{
		Format:       1,
		Layout:       "merged-erofs-v1",
		Digest:       "sha256:" + strings.Repeat("a", 64),
		Architecture: runtime.GOARCH,
		Files:        map[string]string{},
	}
	imageDir := filepath.Join(service.store.directory, "images", manifest.Digest[7:])
	if err = os.MkdirAll(imageDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"kernel",
		"initramfs",
		"root.erofs",
	} {
		path := filepath.Join(imageDir, name)
		if err = os.WriteFile(path, []byte(name), 0o444); err != nil {
			t.Fatal(err)
		}
		manifest.Files[name], err = loom.DigestFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = loom.WriteJSON(filepath.Join(imageDir, "image.json"), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err = service.store.db.Exec("INSERT INTO tenants VALUES(?,?,?)", "tenant", "fixture-hash", encode(tenantRecord{
		MaxSandboxes:  10,
		MaxStorageMiB: 4096,
	})); err != nil {
		t.Fatal(err)
	}
	if _, err = service.store.db.Exec("INSERT INTO images VALUES(?,?)", manifest.Digest, encode(manifest)); err != nil {
		t.Fatal(err)
	}
	service.opts.Fault = nil
	_, err = service.AdmitTemplate(context.Background(), Principal{
		TenantID: "tenant",
	}, "publish-cold", Command{
		Action: "template_put",
		Template: &Template{
			ID:    "cold",
			Image: manifest.Digest,
			Shape: loom.Shape{
				VCPUs:     1,
				MemoryMiB: 128,
				DiskMiB:   64,
			},
		},
	})
	service.opts.Fault = fault
	if err != nil {
		t.Fatal(err)
	}
	return service, backend
}

func coldClaim(t *testing.T, service *Service, key string) Operation {
	t.Helper()
	operation, err := service.AdmitCold(context.Background(), ColdRequest{
		TenantID:   "tenant",
		Key:        key,
		TemplateID: "cold",
	})
	if err != nil && !errors.Is(err, ErrInterrupted) {
		t.Fatal(err)
	}
	return operation
}

func awaitColdWorker(t *testing.T, service *Service, id string) resourceStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		service.mu.Lock()
		active := service.active[id]
		last, seen := service.lastErrors[id]
		service.mu.Unlock()
		step, err := loadStep(context.Background(), service.store.db, id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatalf("resource worker did not stop: phase=%s status=%s error=%v", step.State.Phase, step.Status, last)
		}
		time.Sleep(time.Millisecond)
	}
}

func resumeColdService(t *testing.T, old *Service, backend *coldRuntime) *Service {
	t.Helper()
	if err := old.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts := old.opts
	opts.Initialize = false
	opts.Fault = nil
	opts.Backend = backend
	service, err := Open(opts)
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

func coldResponsibility(t *testing.T, service *Service, operation Operation, witness bool) {
	t.Helper()
	var slots, storage, pending, disks int64
	if err := service.store.db.QueryRow("SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0),COALESCE(SUM(kind='pending_storage'),0),COALESCE(SUM(kind='vm_disk'),0) FROM quota_resources WHERE source_operation_id=?", operation.ID).Scan(&slots, &storage, &pending, &disks); err != nil {
		t.Fatal(err)
	}
	if slots != 1 || storage != 64 || witness && (pending != 0 || disks != 1) || !witness && (pending != 1 || disks != 0) {
		t.Fatalf("responsibility changed without handoff: slots=%d storage=%d pending=%d disks=%d", slots, storage, pending, disks)
	}
	step, err := loadStep(context.Background(), service.store.db, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if witness != (step.State.Witness != "") || witness != (step.Status == "succeeded") {
		t.Fatalf("witness and step completion disagree: %+v", step.State)
	}
	var phase, instance string
	if err := service.store.db.QueryRow("SELECT state,current_instance FROM vms WHERE id=?", operation.Result.VMID).Scan(&phase, &instance); err != nil {
		t.Fatal(err)
	}
	if witness && (phase != "running" || instance != step.State.Witness) || !witness && phase == "running" {
		t.Fatal("VM and witness were not published together")
	}
	var sandboxState string
	if err := service.store.db.QueryRow("SELECT state FROM sandboxes WHERE id=?", operation.Result.SandboxID).Scan(&sandboxState); err != nil {
		t.Fatal(err)
	}
	if sandboxState != "provisioning" {
		t.Fatal("System ready was incorrectly published as sandbox delivery")
	}
	sandbox, err := loadSandbox(context.Background(), service.store.db, operation.Result.SandboxID, "tenant")
	if err != nil || witness && sandbox.Instance != step.State.Witness || !witness && sandbox.Instance != "" {
		t.Fatal("sandbox instance handoff was not atomic", err)
	}
	var vmHolder, imageHolder, stepHolders int
	if err := service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='sandbox' AND holder_id=?),(SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND holder_kind='vm' AND holder_id=?),(SELECT COUNT(*) FROM resource_refs WHERE holder_kind='step' AND holder_id=?)", operation.Result.VMID, operation.Result.SandboxID, operation.Result.VMID, operation.ID+":resource").Scan(&vmHolder, &imageHolder, &stepHolders); err != nil {
		t.Fatal(err)
	}
	if witness && (vmHolder != 1 || imageHolder != 1 || stepHolders != 0) || !witness && (vmHolder != 0 || imageHolder != 0 || stepHolders != 2) {
		t.Fatal("physical references were not transferred atomically", vmHolder, imageHolder, stepHolders)
	}
}
