//go:build integration

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"

	"github.com/woxQAQ/haven/internal/egress"
	"github.com/woxQAQ/haven/internal/haven"
	"github.com/woxQAQ/haven/internal/httpapi"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/unixsocket"
	"github.com/woxQAQ/haven/sdk"
)

// Only host/System/Admin effects are fixtures. Haven owns the actual SQLite
// intents, witnesses, quotas and terminal results; Workload runs the Rust agent.
type transportRuntime struct {
	mu           sync.Mutex
	workload     string
	next         int
	resources    map[string]loom.Resource
	observations map[string]loom.Observation
	listeners    map[string]*net.UnixListener
}

func (backend *transportRuntime) Compatibility() string { return "rust-transport-host-fixture" }
func (backend *transportRuntime) Check(_ context.Context, config loom.Config) error {
	return host.CheckConfig(&config)
}

func (backend *transportRuntime) Prepare(ctx context.Context, resource loom.Resource) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, directory := range []string{filepath.Dir(resource.Disk), filepath.Join(resource.Dir, "capture")} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(resource.Disk, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	return errors.Join(file.Truncate(resource.Config.Shape.DiskMiB<<20), file.Close())
}

func (backend *transportRuntime) Inspect(ctx context.Context, resource loom.Resource) (loom.Observation, error) {
	if err := ctx.Err(); err != nil {
		return loom.Observation{}, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if original, ok := backend.resources[resource.VMID]; ok && !reflect.DeepEqual(original, resource) {
		return loom.Observation{}, errors.New("fixture refuses another generation")
	}
	return backend.observations[resource.VMID], nil
}

func (backend *transportRuntime) Launch(ctx context.Context, resource loom.Resource, _ bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.observations[resource.VMID].Alive {
		return errors.New("fixture refuses duplicate launch")
	}
	for _, directory := range []string{filepath.Join(resource.RunDir, "io"), filepath.Join(resource.RunDir, "root")} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return err
		}
	}
	for _, name := range []string{"launch.json", "process.json", "console.log"} {
		if err := os.WriteFile(filepath.Join(resource.RunDir, name), []byte("owned transport fixture"), 0o600); err != nil {
			return err
		}
	}
	directory, err := unix.Open(filepath.Join(resource.RunDir, "io"), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(directory) }()
	apiSocket, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(apiSocket) }()
	if err = unix.Bind(apiSocket, &unix.SockaddrUnix{Name: fmt.Sprintf("/proc/self/fd/%d/api.sock", directory)}); err != nil {
		return err
	}
	socket := filepath.Join(resource.RunDir, "io", "vsock")
	listener, err := unixsocket.Listen(socket)
	if err != nil {
		return err
	}
	backend.next++
	backend.resources[resource.VMID] = resource
	backend.observations[resource.VMID] = loom.Observation{Alive: true, PID: 1000 + backend.next, StartTime: uint64(backend.next)}
	backend.listeners[resource.VMID] = listener
	go backend.accept(listener)
	return nil
}

func (backend *transportRuntime) accept(listener *net.UnixListener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			reader := bufio.NewReader(conn)
			line, err := reader.ReadString('\n')
			if err != nil || line != "CONNECT 2048\n" {
				_ = conn.Close()
				return
			}
			upstream, err := net.Dial("unix", backend.workload)
			if err != nil {
				_ = conn.Close()
				return
			}
			if _, err = io.WriteString(conn, "OK 2048\n"); err != nil {
				_ = conn.Close()
				_ = upstream.Close()
				return
			}
			if count := reader.Buffered(); count > 0 {
				if _, err = io.CopyN(upstream, reader, int64(count)); err != nil {
					_ = conn.Close()
					_ = upstream.Close()
					return
				}
			}
			duplex(conn, upstream)
		}()
	}
}

func (backend *transportRuntime) Boot(ctx context.Context, resource loom.Resource, _ bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	observation := backend.observations[resource.VMID]
	if !observation.Alive {
		return errors.New("fixture has no live process")
	}
	observation.Booted, observation.Configured = true, true
	backend.observations[resource.VMID] = observation
	return nil
}

func (backend *transportRuntime) Agent(ctx context.Context, _ loom.Resource, _ bool) error {
	return ctx.Err()
}

func (backend *transportRuntime) Network(ctx context.Context, resource loom.Resource, enabled bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if enabled {
		return errors.New("transport fixture forbids routed networking")
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	observation := backend.observations[resource.VMID]
	observation.Network = false
	backend.observations[resource.VMID] = observation
	return nil
}

func (backend *transportRuntime) Stop(ctx context.Context, resource loom.Resource) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if original, ok := backend.resources[resource.VMID]; ok && !reflect.DeepEqual(original, resource) {
		return errors.New("fixture refuses another generation")
	}
	if listener := backend.listeners[resource.VMID]; listener != nil {
		_ = listener.Close()
		delete(backend.listeners, resource.VMID)
	}
	backend.observations[resource.VMID] = loom.Observation{}
	return nil
}

func (backend *transportRuntime) Remove(ctx context.Context, resource loom.Resource) error {
	observation, err := backend.Inspect(ctx, resource)
	if err != nil {
		return err
	}
	if observation.Alive {
		return errors.New("fixture refuses live removal")
	}
	return nil
}

func (backend *transportRuntime) Pause(context.Context, loom.Resource) error {
	return errors.New("transport fixture does not implement capture")
}

func (backend *transportRuntime) Resume(context.Context, loom.Resource) error {
	return errors.New("transport fixture does not implement restore")
}

func (backend *transportRuntime) Capture(context.Context, loom.Resource, string) error {
	return errors.New("transport fixture does not implement capture")
}

func (backend *transportRuntime) close() {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	for _, listener := range backend.listeners {
		_ = listener.Close()
	}
}

type transportGuest struct{ haven.LiveGuest }

func (transportGuest) Activate(context.Context, loom.Resource, haven.Activation) error { return nil }

func (transportGuest) PrepareTemplate(context.Context, loom.Resource) error { return nil }

func closeWrite(conn net.Conn) {
	if half, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = half.CloseWrite()
	}
}

func duplex(left, right net.Conn) {
	defer func() { _ = left.Close() }()
	defer func() { _ = right.Close() }()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(left, right); closeWrite(left); done <- struct{}{} }()
	go func() { _, _ = io.Copy(right, left); closeWrite(right); done <- struct{}{} }()
	<-done
	<-done
}

func privateRoot(t *testing.T) string {
	t.Helper()
	for range 100 {
		var entropy [2]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join("/dev/shm", "e."+hex.EncodeToString(entropy[:]))
		if err := os.Mkdir(path, 0o700); os.IsExist(err) {
			continue
		} else if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.RemoveAll(path); err != nil {
				t.Error(err)
			}
		})
		return path
	}
	t.Fatal("cannot allocate private short tmpfs root")
	return ""
}

// The offline fixture seeds only a private immutable input image. It inserts no
// operation, resource, receipt, successful result or accounting row.
func seedFixtureImage(t *testing.T, root string) string {
	t.Helper()
	manifest := host.ImageManifest{Format: 1, Layout: "merged-erofs-v1", Digest: "sha256:" + strings.Repeat("a", 64), Architecture: runtime.GOARCH, Files: map[string]string{}}
	directory := filepath.Join(root, "images", manifest.Digest[7:])
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"kernel", "initramfs", "root.erofs"} {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, []byte("transport input "+name), 0o444); err != nil {
			t.Fatal(err)
		}
		hash, err := loom.DigestFile(path)
		if err != nil {
			t.Fatal(err)
		}
		manifest.Files[name] = hash
	}
	if err := loom.WriteJSON(filepath.Join(directory, "image.json"), manifest); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", filepath.Join(root, "haven.db"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(manifest)
	if err == nil {
		_, err = database.Exec("INSERT INTO images VALUES(?,?)", manifest.Digest, body)
	}
	err = errors.Join(err, database.Close())
	if err != nil {
		t.Fatal(err)
	}
	return manifest.Digest
}

func transportFixture(t *testing.T) (*haven.Service, *sdk.Client, haven.Principal, string) {
	return transportFixtureRoot(t, false)
}

func transportFixtureRoot(t *testing.T, longPath bool) (*haven.Service, *sdk.Client, haven.Principal, string) {
	t.Helper()
	binary := os.Getenv("SANDBOX_AGENT_FIXTURE")
	if binary == "" {
		t.Fatal("SANDBOX_AGENT_FIXTURE is required; run make integration")
	}
	root := privateRoot(t)
	if longPath {
		root = filepath.Join(root, strings.Repeat("owned-", 24))
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	guestDir := privateRoot(t)
	command := exec.Command(binary, guestDir)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(3 * time.Second):
			_ = command.Process.Kill()
			<-exited
		}
	})
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(stdout).ReadString('\n'); ready <- line }()
	select {
	case line := <-ready:
		if !strings.HasPrefix(line, "ready ") {
			t.Fatalf("fixture startup: %s %s", line, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Rust fixture did not become ready")
	}
	backend := &transportRuntime{workload: filepath.Join(guestDir, "workload.sock"), resources: map[string]loom.Resource{}, observations: map[string]loom.Observation{}, listeners: map[string]*net.UnixListener{}}
	t.Cleanup(backend.close)
	options := haven.Options{StateDir: root, Initialize: true, Backend: backend, Guest: transportGuest{}, RootToken: "integration-root-token", MaxMemoryMiB: 8192, MaxVCPUs: 64, SafetyBytes: 1}
	bootstrap, err := haven.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err = bootstrap.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	image := seedFixtureImage(t, root)
	options.Initialize = false
	service, err := haven.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = service.PutTenant(ctx, haven.Principal{Root: true}, haven.Tenant{ID: "transport", Token: "transport-token", MaxSandboxes: 10, MaxStorageMiB: 4096})
	if err != nil {
		t.Fatal(err)
	}
	principal := haven.Principal{TenantID: "transport"}
	if _, err = service.Execute(ctx, principal, "template", haven.Command{Action: "template_put", Template: &haven.Template{ID: "fixture", Image: image, Shape: loom.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}}}); err != nil {
		t.Fatal(err)
	}
	operation, err := service.Execute(ctx, principal, "claim", haven.Command{Action: "claim", TemplateID: "fixture", Policy: egress.Policy{Mode: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.PublicOperation(ctx, principal, operation.ID)
	if err != nil || result.Result.Sandbox == nil {
		t.Fatal(result, err)
	}
	server := httptest.NewServer(httpapi.New(service))
	t.Cleanup(server.Close)
	return service, sdk.New(server.URL, result.Result.Tokens[result.Result.Sandbox.ID]), principal, result.Result.Sandbox.ID
}
