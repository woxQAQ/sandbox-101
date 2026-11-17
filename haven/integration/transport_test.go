//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/operation"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
)

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
