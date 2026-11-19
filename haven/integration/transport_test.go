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
	"net/http"
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
	"google.golang.org/protobuf/types/known/structpb"
	_ "modernc.org/sqlite"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/unixsocket"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sdk"
	"github.com/woxQAQ/haven/types"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
)

func TestRustAgentThroughSandboxRelay(t *testing.T) {
	service, client, principal, id := transportFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := client.Connect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
	}()
	for range 2 {
		info, err := conn.Call(ctx, "info", map[string]any{}, nil)
		if err != nil || info["portal"] != "workload" {
			t.Fatal("reused info stream", info, err)
		}
	}
	missing := filepath.Join(t.TempDir(), "missing")
	for _, test := range []struct {
		method string
		params map[string]any
		code   string
	}{
		{"fs_read", map[string]any{
			"path": missing,
		}, "not_found"},
		{"fs_stat", map[string]any{
			"path": "relative",
		}, "invalid"},
		{"proc_kill", map[string]any{
			"process_id": "absent",
		}, "not_found"},
		{"session_rm", map[string]any{
			"session_id": "absent",
		}, "not_found"},
		{"activate", map[string]any{
			"sandbox_id": "attack",
		}, "unauthorized"},
	} {
		params, err := conn.Call(ctx, test.method, test.params, nil)
		if err == nil || params["error_code"] != test.code || !strings.Contains(err.Error(), "guest "+test.method) || !strings.Contains(err.Error(), "guest/agent/src/") {
			t.Fatalf("%s structured failure: %v %#v", test.method, params, err)
		}
	}
	var stdout, stderr bytes.Buffer
	code, err := conn.Exec(ctx, "printf 'streamed-out'; printf 'streamed-err' >&2", &stdout, &stderr)
	if err != nil || code != 0 || stdout.String() != "streamed-out" || stderr.String() != "streamed-err" {
		t.Fatalf("exec: %d %v %q %q", code, err, stdout.String(), stderr.String())
	}
	// Frames pass unchanged through Haven; only Rust interprets file operations.
	path := filepath.Join(t.TempDir(), "binary-file")
	payload := bytes.Repeat([]byte{
		0,
		1,
		2,
		0xff,
	}, 50000)
	first := requestFrame(t, "upload", "fs_write", map[string]any{
		"path": path,
	})
	first.Eof = false
	if err = conn.Send(first); err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(payload); {
		end := min(offset+65536, len(payload))
		if err = conn.Send(&pb.Frame{
			Id:   "upload",
			Data: payload[offset:end],
			Eof:  end == len(payload),
		}); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	for {
		frame, err := conn.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if frame.Error != "" {
			t.Fatal(frame.Error)
		}
		if frame.Eof {
			break
		}
	}
	var downloaded bytes.Buffer
	if _, err = conn.Call(ctx, "fs_read", map[string]any{
		"path": path,
	}, func(frame *pb.Frame) error {
		_, err := downloaded.Write(frame.Data)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, downloaded.Bytes()) {
		t.Fatal("streaming file round trip corrupted bytes")
	}
	if _, err = conn.Call(ctx, "activate", map[string]any{
		"sandbox_id": "attack",
	}, nil); err == nil {
		t.Fatal("tenant stream exposed host administration")
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "release", operation.Command{
		Action:    "release",
		SandboxID: id,
	}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err = conn.Receive(); err == nil {
		t.Fatal("released sandbox kept a live data connection")
	}
}

func TestRustKeyedExecReplayThroughSandboxRelay(t *testing.T) {
	_, client, _, id := transportFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "side-effect")
	command := "printf 'once\\n' >> '" + path + "'; printf 'saved-out'; printf 'saved-err' >&2"
	params := map[string]any{
		"exec_key": "durable-work-step",
		"command":  command,
	}
	for attempt := range 2 {
		conn, err := client.Connect(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		var stdout, stderr bytes.Buffer
		result, err := conn.Call(ctx, "exec", params, func(frame *pb.Frame) error {
			output := &stdout
			if frame.Params.AsMap()["stream"] == "stderr" {
				output = &stderr
			}
			_, err := output.Write(frame.Data)
			return err
		})
		if err != nil || result["result_state"] != "completed" || result["exit_code"] != float64(0) || result["replayed"] != (attempt > 0) || stdout.String() != "saved-out" || stderr.String() != "saved-err" {
			_ = conn.Close()
			t.Fatalf("attempt %d: %v %v %q %q", attempt, result, err, stdout.String(), stderr.String())
		}
		if attempt == 1 {
			conflict, err := conn.Call(ctx, "exec", map[string]any{
				"exec_key": "durable-work-step",
				"command":  "printf changed-input",
			}, nil)
			if err == nil || conflict["error_code"] != "exec_key_conflict" || conflict["result_state"] != "conflict" {
				_ = conn.Close()
				t.Fatalf("structured conflict lost: %v %#v", conflict, err)
			}
			if info, err := conn.Call(ctx, "info", nil, nil); err != nil || info["portal"] != "workload" {
				_ = conn.Close()
				t.Fatal("connection after terminal guest error", info, err)
			}
		}
		_ = conn.Close()
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "once\n" {
		t.Fatalf("keyed replay repeated side effect: %q %v", data, err)
	}
}

func TestRustRawForwardHalfClose(t *testing.T) {
	_, client, _, id := transportFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = listener.Close()
	}()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		body, _ := io.ReadAll(conn)
		_, _ = conn.Write(append(bytes.ToUpper(body), []byte("-after-eof")...))
	}()
	conn, err := client.Connect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
	}()
	if err = conn.Send(requestFrame(t, "forward", "port_forward", map[string]any{
		"port": listener.Addr().(*net.TCPAddr).Port,
	})); err != nil {
		t.Fatal(err)
	}
	ready, err := conn.Receive()
	if err != nil || ready.Error != "" || ready.Eof || ready.Params.AsMap()["ready"] != true {
		t.Fatal("forward readiness", ready, err)
	}
	if _, err = conn.Write([]byte("opaque-stream")); err != nil {
		t.Fatal(err)
	}
	if err = conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(conn)
	if err != nil || string(body) != "OPAQUE-STREAM-after-eof" {
		t.Fatalf("half-close discarded reverse stream: %q %v", body, err)
	}
}

func TestRustTransportWithLongRuntimePath(t *testing.T) {
	_, client, _, id := transportFixtureRoot(t, true, transportGuest{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := client.Connect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = connection.Close()
	}()
	result, err := connection.Call(ctx, "info", nil, nil)
	if err != nil || result["portal"] != "workload" {
		t.Fatal(result, err)
	}
}

func TestRustPreviewUsesRawForward(t *testing.T) {
	service, client, principal, id := transportFixture(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("preview forwarded platform credentials")
		}
		w.Header().Add("Referrer-Policy", "unsafe-url")
		_, _ = io.WriteString(w, "preview:"+r.URL.Path)
	}))
	defer upstream.Close()
	port := upstream.Listener.Addr().(*net.TCPAddr).Port
	capability, err := service.Dataplane.PreviewURL(context.Background(), principal, dataplane.PreviewRequest{
		SandboxID: id,
		Port:      port,
		TTL:       time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest("GET", client.Endpoint+capability+"hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer harmless-preview-test")
	request.Header.Set("Cookie", "platform=harmless-preview-test")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 200 || string(body) != "preview:/hello" {
		t.Fatalf("preview: HTTP %d %q", response.StatusCode, body)
	}
	if policies := response.Header.Values("Referrer-Policy"); len(policies) != 1 || policies[0] != "no-referrer" {
		t.Fatalf("guest overrode Preview referrer protection: %v", policies)
	}
}

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

func (backend *transportRuntime) Compatibility() string {
	return "rust-transport-host-fixture"
}

func (backend *transportRuntime) Check(_ context.Context, config loom.Config) error {
	return host.CheckConfig(&config)
}

func (backend *transportRuntime) Prepare(ctx context.Context, resource loom.Resource) error {
	if err := ctx.Err(); err != nil {
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
		if err := os.WriteFile(filepath.Join(resource.RunDir, name), []byte("owned transport fixture"), 0o600); err != nil {
			return err
		}
	}
	directory, err := unix.Open(filepath.Join(resource.RunDir, "io"), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() {
		_ = unix.Close(directory)
	}()
	apiSocket, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() {
		_ = unix.Close(apiSocket)
	}()
	if err = unix.Bind(apiSocket, &unix.SockaddrUnix{
		Name: fmt.Sprintf("/proc/self/fd/%d/api.sock", directory),
	}); err != nil {
		return err
	}
	socket := filepath.Join(resource.RunDir, "io", "vsock")
	listener, err := unixsocket.Listen(socket)
	if err != nil {
		return err
	}
	backend.next++
	backend.resources[resource.VMID] = resource
	backend.observations[resource.VMID] = loom.Observation{
		Alive:     true,
		PID:       1000 + backend.next,
		StartTime: uint64(backend.next),
	}
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

type transportGuest struct {
	guest.Client
}

func (transportGuest) Activate(context.Context, loom.Resource, guest.Activation) error {
	return nil
}

func (transportGuest) PrepareTemplate(context.Context, loom.Resource) error {
	return nil
}

func closeWrite(conn net.Conn) {
	if half, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = half.CloseWrite()
	}
}

func duplex(left, right net.Conn) {
	defer func() {
		_ = left.Close()
	}()
	defer func() {
		_ = right.Close()
	}()
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(left, right)
		closeWrite(left)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(right, left)
		closeWrite(right)
		done <- struct{}{}
	}()
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
	manifest := host.ImageManifest{
		Format:       1,
		Layout:       "merged-erofs-v1",
		Digest:       "sha256:" + strings.Repeat("a", 64),
		Architecture: runtime.GOARCH,
		Files:        map[string]string{},
	}
	directory := filepath.Join(root, "images", manifest.Digest[7:])
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"kernel",
		"initramfs",
		"root.erofs",
	} {
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

func transportFixture(t *testing.T) (*application.App, *sdk.Client, authorization.Principal, string) {
	return transportFixtureRoot(t, false, transportGuest{})
}

func transportFixtureRoot(t *testing.T, longPath bool, guest guest.Agent) (*application.App, *sdk.Client, authorization.Principal, string) {
	t.Helper()
	binary := os.Getenv("SANDBOX_AGENT_FIXTURE")
	if binary == "" {
		t.Fatal("SANDBOX_AGENT_FIXTURE is required; run just integration")
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
	go func() {
		exited <- command.Wait()
	}()
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
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		ready <- line
	}()
	select {
	case line := <-ready:
		if !strings.HasPrefix(line, "ready ") {
			t.Fatalf("fixture startup: %s %s", line, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Rust fixture did not become ready")
	}
	backend := &transportRuntime{
		workload:     filepath.Join(guestDir, "workload.sock"),
		resources:    map[string]loom.Resource{},
		observations: map[string]loom.Observation{},
		listeners:    map[string]*net.UnixListener{},
	}
	t.Cleanup(backend.close)
	options := application.Options{
		StateDir:     root,
		Initialize:   true,
		Backend:      backend,
		Guest:        guest,
		AdminToken:   "integration-root-token",
		MaxMemoryMiB: 8192,
		MaxVCPUs:     64,
		SafetyBytes:  1,
	}
	bootstrap, err := application.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err = bootstrap.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	image := seedFixtureImage(t, root)
	options.Initialize = false
	service, err := application.Open(options)
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
	_, err = service.Tenants.PutTenant(ctx, authorization.Principal{
		Root: true,
	}, types.Tenant{
		ID:            "transport",
		Token:         "transport-token",
		MaxSandboxes:  10,
		MaxStorageMiB: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := authorization.Principal{
		TenantID: "transport",
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "template", operation.Command{
		Action: "template_put",
		Template: &operation.TemplateSpec{
			ID:    "fixture",
			Image: image,
			Shape: loom.Shape{
				VCPUs:     1,
				MemoryMiB: 128,
				DiskMiB:   64,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	acceptedRecord, err := service.SandboxRequests.Execute(ctx, principal, "claim", operation.Command{
		Action:     "claim",
		TemplateID: "fixture",
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Delivery.PublicOperation(ctx, principal, acceptedRecord.ID)
	if err != nil || result.Result.Sandbox == nil {
		t.Fatal(result, err)
	}
	server := httptest.NewServer(api.New(api.Services{
		Authentication: service.Authentication,
		Catalog:        service.Catalog,
		Templates:      service.TemplateCatalog,
		Operations:     service.Operations,
		Tenants:        service.Tenants,
		Requests:       service.SandboxRequests,
		Dataplane:      service.Dataplane,
		Node:           service.Node,
		Delivery:       service.Delivery,
	}))
	t.Cleanup(server.Close)
	return service, sdk.New(server.URL, result.Result.Tokens[result.Result.Sandbox.ID]), principal, result.Result.Sandbox.ID
}

func requestFrame(t *testing.T, id, method string, params map[string]any) *pb.Frame {
	t.Helper()
	values, err := structpb.NewStruct(params)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.Frame{
		Id:     id,
		Method: method,
		Params: values,
		Eof:    true,
	}
}
