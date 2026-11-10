package agent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd"
)

// Exercise the client over its actual CH handshake and protobuf transport.
func TestClientStructuredFailuresAndKeyedFence(t *testing.T) {
	for _, tc := range []struct {
		method string
		code   string
		want   sandboxd.ErrorCode
	}{
		{"activate", "invalid", sandboxd.ErrInternal},
		{"info", "not_found", sandboxd.ErrManualRecovery},
		{"prepare_template", "conflict", sandboxd.ErrConflict},
		{"prepare_template", "quiesced", sandboxd.ErrConflict},
		{"activate", "resource_exhausted", sandboxd.ErrExhausted},
		{"activate", "unauthorized", sandboxd.ErrInternal},
		{"info", "internal", sandboxd.ErrInternal},
		{"info", "future_code", sandboxd.ErrInternal},
		{"info", "", sandboxd.ErrInternal},
		{"exec", "result_unknown", sandboxd.ErrUnknown},
		{"exec", "exec_in_progress", sandboxd.ErrConflict},
		{"exec", "exec_key_conflict", sandboxd.ErrConflict},
		{"exec", "exec_ledger_capacity_exhausted", sandboxd.ErrExhausted},
		{"exec", "internal", sandboxd.ErrUnknown},
		{"exec", "future_code", sandboxd.ErrUnknown},
		{"exec", "", sandboxd.ErrUnknown},
	} {
		t.Run(tc.method+"/"+tc.code, func(t *testing.T) {
			// sockaddr_un has a 108-byte pathname bound, independent of test names.
			dir, err := os.MkdirTemp("", "agent-rpc-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			path := filepath.Join(dir, "ch.sock")
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- func() error {
					conn, err := listener.Accept()
					if err != nil {
						return err
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
					reader := bufio.NewReader(conn)
					line, err := reader.ReadString('\n')
					if err != nil {
						return err
					}
					if !strings.HasPrefix(line, "CONNECT ") {
						return fmt.Errorf("invalid handshake: %q", line)
					}
					if _, err := io.WriteString(conn, strings.Replace(line, "CONNECT", "OK", 1)); err != nil {
						return err
					}
					request, err := Read(reader)
					if err != nil {
						return err
					}
					if request.Method != tc.method {
						return fmt.Errorf("method=%q, want %q", request.Method, tc.method)
					}
					response, err := Request(request.Id, request.Method, map[string]any{"error_code": tc.code})
					if err != nil {
						return err
					}
					response.Error = "diagnostic stage at guest/agent/src/system.rs:42:7"
					response.Eof = true
					return Write(conn, response)
				}()
			}()
			client := &Client{}
			vm := api.VM{VSockPath: path}
			switch tc.method {
			case "info":
				err = client.Info(ctx, vm)
			case "activate":
				err = client.Activate(ctx, vm, sandboxd.Activation{})
			case "prepare_template":
				err = client.PrepareTemplate(ctx, vm)
			case "exec":
				err = client.Exec(ctx, vm, "same-key", "true")
			}
			var failure *sandboxd.Error
			if !errors.As(err, &failure) || failure.Code != tc.want ||
				!strings.Contains(failure.Message, "guest "+tc.method) ||
				strings.Contains(failure.Message, "system.rs:42:7") ||
				failure.Cause == nil || !strings.Contains(failure.Cause.Error(), "system.rs:42:7") {
				t.Fatalf("structured failure: %#v", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
