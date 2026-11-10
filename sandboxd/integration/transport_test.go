package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
	"github.com/woxQAQ/sandboxd"
	"github.com/woxQAQ/sandboxd/agent"
	"github.com/woxQAQ/sandboxd/egress"
	"github.com/woxQAQ/sandboxd/sdk"
)

// This harness exercises the real Go service/SDK and Rust workload dispatcher.
// VM lifecycle and privileged admin activation are fixtures; they are not boot tests.
type fixtureEngine struct {
	mu     sync.Mutex
	vm     api.VM
	ops    map[string]api.Operation
	socket string
}

func (f *fixtureEngine) Submit(
	_ context.Context,
	r api.Request,
) (api.Operation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if o, ok := f.ops[r.Key]; ok {
		return o, nil
	}
	o := api.Operation{
		ID:      r.Key,
		Key:     r.Key,
		Status:  "succeeded",
		VMID:    r.VMID,
		Request: r,
	}
	switch r.Action {
	case "create":
		o.VMID = "vm-fixture"
		f.vm = api.VM{
			ID:       o.VMID,
			Config:   *r.Config,
			Verified: true,
			Phase:    "created",
		}
	case "start":
		f.vm.Phase = "running"
		f.vm.Instance = "fixture-instance"
		f.vm.VSockPath = f.socket
		f.vm.AgentReady = true
	case "network":
		if r.NetworkEnabled {
			return o, fmt.Errorf("fixture forbids routed networking")
		}
	case "delete":
		f.vm = api.VM{}
	default:
		return o, fmt.Errorf("unsupported fixture action %s", r.Action)
	}
	f.ops[r.Key] = o
	return o, nil
}

func (f *fixtureEngine) GetOperation(
	_ context.Context,
	id string,
) (api.Operation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.ops[id]
	if !ok {
		return o, api.Fail(api.NotFound, "not found")
	}
	return o, nil
}

func (f *fixtureEngine) FindOperation(
	ctx context.Context,
	id string,
) (api.Operation, error) {
	return f.GetOperation(ctx, id)
}

func (f *fixtureEngine) GetVM(_ context.Context, id string) (api.VM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.vm.ID != id {
		return api.VM{}, api.Fail(api.NotFound, "not found")
	}
	return f.vm, nil
}

func (f *fixtureEngine) ListVMs(context.Context) ([]api.VM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.vm.ID == "" {
		return nil, nil
	}
	return []api.VM{f.vm}, nil
}

func (f *fixtureEngine) GetSnapshot(
	context.Context,
	string,
) (api.Snapshot, error) {
	return api.Snapshot{}, api.Fail(api.NotFound, "not found")
}

func (f *fixtureEngine) ListSnapshots(
	context.Context,
) ([]api.Snapshot, error) {
	return nil, nil
}

type fixtureAdmin struct{ agent.Client }

func (*fixtureAdmin) Activate(
	context.Context,
	api.VM,
	sandboxd.Activation,
) error {
	return nil
}

func (*fixtureAdmin) PrepareTemplate(
	context.Context,
	api.VM,
) error {
	return nil
}

func closeWrite(c net.Conn) {
	if h, ok := c.(interface{ CloseWrite() error }); ok {
		_ = h.CloseWrite()
	}
}

func duplex(a, b net.Conn) {
	defer a.Close()
	defer b.Close()
	done := make(chan struct{}, 2)
	go func() { io.Copy(a, b); closeWrite(a); done <- struct{}{} }()
	go func() { io.Copy(b, a); closeWrite(b); done <- struct{}{} }()
	<-done
	<-done
}

func transportFixture(
	t *testing.T,
) (*sandboxd.Service, *sdk.Client, sandboxd.Principal, string) {
	t.Helper()
	binary := os.Getenv("SANDBOX_AGENT_FIXTURE")
	if binary == "" {
		t.Skip(
			"run make integration to build and run the Rust transport fixture",
		)
	}
	dir := t.TempDir()
	cmd := exec.Command(binary, dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
	})
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(out).ReadString('\n'); ready <- line }()
	select {
	case line := <-ready:
		if !strings.HasPrefix(line, "ready ") {
			t.Fatalf("fixture startup: %s %s", line, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Rust fixture did not become ready")
	}
	socket := filepath.Join(dir, "ch.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				reader := bufio.NewReader(conn)
				line, err := reader.ReadString('\n')
				if err != nil || line != "CONNECT 2048\n" {
					conn.Close()
					return
				}
				up, err := net.Dial("unix", filepath.Join(dir, "workload.sock"))
				if err != nil {
					conn.Close()
					return
				}
				if _, err = io.WriteString(conn, "OK 2048\n"); err != nil {
					conn.Close()
					up.Close()
					return
				}
				if n := reader.Buffered(); n > 0 {
					if _, err = io.CopyN(up, reader, int64(n)); err != nil {
						conn.Close()
						up.Close()
						return
					}
				}
				duplex(conn, up)
			}()
		}
	}()
	engine := &fixtureEngine{ops: map[string]api.Operation{}, socket: socket}
	service, err := sandboxd.Open(
		sandboxd.Options{
			StateDir:   filepath.Join(dir, "state"),
			Initialize: true,
			RootToken:  "integration-root-token",
			Engine:     engine,
			Agent:      &fixtureAdmin{},
			Interval:   time.Hour,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.Close() })
	tenant, err := service.PutTenant(
		context.Background(),
		sandboxd.Principal{Root: true},
		sandboxd.Tenant{
			ID:            "transport",
			Token:         "transport-token",
			MaxSandboxes:  10,
			MaxStorageMiB: 4096,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	p := sandboxd.Principal{TenantID: tenant.ID}
	if _, err = service.Execute(
		context.Background(),
		p,
		"template",
		sandboxd.Command{
			Action: "template_put",
			Template: &sandboxd.Template{
				ID:    "fixture",
				Image: "sha256:" + strings.Repeat("a", 64),
				Shape: api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
			},
		},
	); err != nil {
		t.Fatal(err)
	}
	result, err := service.Execute(
		context.Background(),
		p,
		"claim",
		sandboxd.Command{
			Action:     "claim",
			TemplateID: "fixture",
			Policy:     egress.Policy{Mode: "none"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(service.Handler())
	t.Cleanup(server.Close)
	return service, sdk.New(
		server.URL,
		result.Tokens[result.Sandbox.ID],
	), p, result.Sandbox.ID
}

func TestRustAgentThroughSandboxRelay(t *testing.T) {
	service, client, p, id := transportFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := client.Connect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for range 2 {
		info, err := conn.Call(ctx, "info", map[string]any{}, nil)
		if err != nil || info["portal"] != "workload" {
			t.Fatalf("reused info stream: %v %v", info, err)
		}
	}
	missing := filepath.Join(t.TempDir(), "missing")
	for _, tc := range []struct {
		method string
		params map[string]any
		code   string
	}{
		{"fs_read", map[string]any{"path": missing}, "not_found"},
		{"fs_stat", map[string]any{"path": "relative"}, "invalid"},
		{"proc_kill", map[string]any{"process_id": "absent"}, "not_found"},
		{"session_rm", map[string]any{"session_id": "absent"}, "not_found"},
		{"activate", map[string]any{"sandbox_id": "attack"}, "unauthorized"},
	} {
		params, err := conn.Call(ctx, tc.method, tc.params, nil)
		var rpcErr *sdk.RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != tc.code ||
			params["error_code"] != tc.code ||
			!strings.Contains(rpcErr.Message, "guest "+tc.method) ||
			!strings.Contains(rpcErr.Message, "guest/agent/src/") {
			t.Fatalf(
				"%s structured failure: params=%v err=%#v",
				tc.method,
				params,
				err,
			)
		}
	}
	var stdout, stderr bytes.Buffer
	code, err := conn.Exec(
		ctx,
		"printf 'streamed-out'; printf 'streamed-err' >&2",
		&stdout,
		&stderr,
	)
	if err != nil || code != 0 || stdout.String() != "streamed-out" ||
		stderr.String() != "streamed-err" {
		t.Fatalf(
			"exec: code=%d err=%v out=%q stderr=%q",
			code,
			err,
			stdout.String(),
			stderr.String(),
		)
	}
	// Streaming writes pass unchanged through sandboxd; only the guest interprets frames.
	path := filepath.Join(t.TempDir(), "binary-file")
	payload := bytes.Repeat([]byte{0, 1, 2, 0xff}, 50000)
	first, _ := agent.Request(
		"upload",
		"fs_write",
		map[string]any{"path": path},
	)
	first.Eof = false
	if err = conn.Send(first); err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(payload); {
		end := min(offset+65536, len(payload))
		if err = conn.Send(
			&pb.Frame{
				Id:   "upload",
				Data: payload[offset:end],
				Eof:  end == len(payload),
			},
		); err != nil {
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
	if _, err = conn.Call(
		ctx,
		"fs_read",
		map[string]any{"path": path},
		func(frame *pb.Frame) error { _, err := downloaded.Write(frame.Data); return err },
	); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, downloaded.Bytes()) {
		t.Fatal("streaming file round trip corrupted bytes")
	}
	if _, err = conn.Call(
		ctx,
		"activate",
		map[string]any{"sandbox_id": "attack"},
		nil,
	); err == nil {
		t.Fatal("tenant relay exposed host administration")
	}
	if _, err = service.Execute(
		ctx,
		p,
		"release",
		sandboxd.Command{Action: "release", SandboxID: id},
	); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err = conn.Receive(); err == nil {
		t.Fatal("released sandbox kept live data connection")
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
		result, err := conn.Call(
			ctx,
			"exec",
			params,
			func(frame *pb.Frame) error {
				out := &stdout
				if frame.Params.AsMap()["stream"] == "stderr" {
					out = &stderr
				}
				_, err := out.Write(frame.Data)
				return err
			},
		)
		if err != nil || result["result_state"] != "completed" ||
			result["exit_code"] != float64(0) ||
			result["replayed"] != (attempt > 0) ||
			stdout.String() != "saved-out" ||
			stderr.String() != "saved-err" {
			conn.Close()
			t.Fatalf(
				"attempt %d: result=%v err=%v stdout=%q stderr=%q",
				attempt,
				result,
				err,
				stdout.String(),
				stderr.String(),
			)
		}
		if attempt == 1 {
			conflict, err := conn.Call(
				ctx,
				"exec",
				map[string]any{
					"exec_key": "durable-work-step",
					"command":  "printf changed-input",
				},
				nil,
			)
			var rpcErr *sdk.RPCError
			if !errors.As(err, &rpcErr) || rpcErr.Code != "exec_key_conflict" ||
				rpcErr.ResultState != "conflict" ||
				conflict["result_state"] != "conflict" {
				conn.Close()
				t.Fatalf(
					"structured conflict lost: params=%v err=%#v",
					conflict,
					err,
				)
			}
			if info, err := conn.Call(
				ctx,
				"info",
				nil,
				nil,
			); err != nil ||
				info["portal"] != "workload" {
				conn.Close()
				t.Fatalf(
					"connection after terminal guest error: %v %v",
					info,
					err,
				)
			}
		}
		conn.Close()
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "once\n" {
		t.Fatalf("keyed replay repeated the side effect: %q %v", data, err)
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
	defer listener.Close()
	go func() {
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		body, _ := io.ReadAll(c)
		_, _ = c.Write(append(bytes.ToUpper(body), []byte("-after-eof")...))
	}()
	conn, err := client.Connect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	request, _ := agent.Request(
		"forward",
		"port_forward",
		map[string]any{"port": listener.Addr().(*net.TCPAddr).Port},
	)
	if err = conn.Send(request); err != nil {
		t.Fatal(err)
	}
	ready, err := conn.Receive()
	if err != nil || ready.Error != "" || ready.Eof ||
		ready.Params.AsMap()["ready"] != true {
		t.Fatalf("forward readiness: %v %v", ready, err)
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

func TestRustPreviewUsesRawForward(t *testing.T) {
	service, client, p, id := transportFixture(t)
	upstream := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "" ||
				r.Header.Get("Cookie") != "" {
				t.Error("preview forwarded platform credentials")
			}
			w.Header().Add("Referrer-Policy", "unsafe-url")
			io.WriteString(w, "preview:"+r.URL.Path)
		}),
	)
	defer upstream.Close()
	port := upstream.Listener.Addr().(*net.TCPAddr).Port
	cap, err := service.PreviewURL(
		context.Background(),
		p,
		id,
		port,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", client.Endpoint+cap+"hello", nil)
	req.Header.Set("Authorization", "Bearer should-not-reach-guest")
	req.Header.Set("Cookie", "platform=should-not-reach-guest")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "preview:/hello" {
		t.Fatalf("preview: HTTP %d %q", resp.StatusCode, body)
	}
	if policies := resp.Header.Values(
		"Referrer-Policy",
	); len(policies) != 1 ||
		policies[0] != "no-referrer" {
		t.Fatalf("guest overrode Preview referrer protection: %v", policies)
	}
}
