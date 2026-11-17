package testfixture

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/haven/loom"
)

// The fixture models only host effects and writes its own private disk/socket
// entries. All admissions, journals, witnesses and accounting use real SQLite.
type ColdRuntime struct {
	Mu           sync.Mutex
	Resources    map[string]loom.Resource
	Observations map[string]loom.Observation
	Counts       map[string]int
	FailBoot     bool
	InspectError error
	Before       func(string, loom.Resource) error
}

func NewColdRuntime() *ColdRuntime {
	return &ColdRuntime{
		Resources:    map[string]loom.Resource{},
		Observations: map[string]loom.Observation{},
		Counts:       map[string]int{},
	}
}

func (backend *ColdRuntime) Compatibility() string {
	return "explicit-cold-fixture"
}

func (backend *ColdRuntime) Check(context.Context, loom.Config) error {
	return nil
}

func (backend *ColdRuntime) Effect(name string, resource loom.Resource) error {
	if backend.Before != nil {
		if err := backend.Before(name, resource); err != nil {
			return err
		}
	}
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	backend.Counts[name]++
	return nil
}

func (backend *ColdRuntime) Prepare(ctx context.Context, resource loom.Resource) error {
	if err := backend.Effect("prepare", resource); err != nil {
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

func (backend *ColdRuntime) Inspect(_ context.Context, resource loom.Resource) (loom.Observation, error) {
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	if backend.InspectError != nil {
		return loom.Observation{}, backend.InspectError
	}
	if known, exists := backend.Resources[resource.VMID]; exists && !reflect.DeepEqual(known, resource) {
		return loom.Observation{}, fmt.Errorf("unknown instance or sibling")
	}
	return backend.Observations[resource.VMID], nil
}

func (backend *ColdRuntime) Launch(_ context.Context, resource loom.Resource, _ bool) error {
	if err := backend.Effect("launch", resource); err != nil {
		return err
	}
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	if backend.Observations[resource.VMID].Alive {
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
	defer func() {
		_ = unix.Close(directory)
	}()
	socket, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() {
		_ = unix.Close(socket)
	}()
	if err = unix.Bind(socket, &unix.SockaddrUnix{
		Name: fmt.Sprintf("/proc/self/fd/%d/api.sock", directory),
	}); err != nil {
		return err
	}
	backend.Resources[resource.VMID] = resource
	backend.Observations[resource.VMID] = loom.Observation{
		Alive:     true,
		PID:       1000 + backend.Counts["launch"],
		StartTime: uint64(backend.Counts["launch"]),
	}
	return nil
}

func (backend *ColdRuntime) Boot(_ context.Context, resource loom.Resource, _ bool) error {
	if err := backend.Effect("boot", resource); err != nil {
		return err
	}
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	if backend.FailBoot {
		return fmt.Errorf("fixture boot failure")
	}
	observation := backend.Observations[resource.VMID]
	observation.Booted, observation.Configured = true, true
	backend.Observations[resource.VMID] = observation
	return nil
}

func (backend *ColdRuntime) Agent(_ context.Context, resource loom.Resource, _ bool) error {
	return backend.Effect("agent", resource)
}

func (backend *ColdRuntime) Stop(_ context.Context, resource loom.Resource) error {
	if err := backend.Effect("stop", resource); err != nil {
		return err
	}
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	if known, exists := backend.Resources[resource.VMID]; exists && !reflect.DeepEqual(known, resource) {
		return fmt.Errorf("refusing another instance")
	}
	backend.Observations[resource.VMID] = loom.Observation{}
	return nil
}

func (backend *ColdRuntime) Network(_ context.Context, resource loom.Resource, enabled bool) error {
	if err := backend.Effect("network", resource); err != nil {
		return err
	}
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	observation := backend.Observations[resource.VMID]
	observation.Network = enabled
	backend.Observations[resource.VMID] = observation
	return nil
}

func (backend *ColdRuntime) Remove(_ context.Context, resource loom.Resource) error {
	if err := backend.Effect("remove", resource); err != nil {
		return err
	}
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	if backend.Observations[resource.VMID].Alive {
		return fmt.Errorf("refusing live removal")
	}
	return nil
}

func (backend *ColdRuntime) Pause(_ context.Context, resource loom.Resource) error {
	if err := backend.Effect("pause", resource); err != nil {
		return err
	}
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	observation := backend.Observations[resource.VMID]
	if !observation.Alive {
		return fmt.Errorf("fixture source is not alive")
	}
	observation.Paused = true
	backend.Observations[resource.VMID] = observation
	return nil
}

func (backend *ColdRuntime) Resume(_ context.Context, resource loom.Resource) error {
	if err := backend.Effect("resume", resource); err != nil {
		return err
	}
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	observation := backend.Observations[resource.VMID]
	if !observation.Alive {
		return fmt.Errorf("fixture source is not alive")
	}
	observation.Paused = false
	backend.Observations[resource.VMID] = observation
	return nil
}

func (backend *ColdRuntime) Capture(_ context.Context, resource loom.Resource, directory string) error {
	if err := backend.Effect("capture", resource); err != nil {
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

func (backend *ColdRuntime) Count(name string) int {
	backend.Mu.Lock()
	defer backend.Mu.Unlock()
	return backend.Counts[name]
}
