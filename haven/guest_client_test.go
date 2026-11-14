package haven

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

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/unixsocket"
	protocol "github.com/woxQAQ/sandbox-protocol/gen"
)

// Keep the control client's structured failure/keyed execution matrix at the
// actual CH handshake and protobuf boundary, using only an owned Unix peer.
func TestHavenGuestStructuredFailuresAndKeyedFence(t *testing.T) {
	for _, scenario := range []struct {
		method, code string
		want         error
	}{
		{"activate", "invalid", nil},
		{"info", "not_found", ErrManualRecovery},
		{"prepare_template", "conflict", ErrConflict},
		{"prepare_template", "quiesced", ErrConflict},
		{"activate", "resource_exhausted", ErrExhausted},
		{"activate", "unauthorized", nil},
		{"info", "internal", nil},
		{"info", "future_code", nil},
		{"info", "", nil},
		{"exec", "result_unknown", ErrUnknown},
		{"exec", "exec_in_progress", ErrConflict},
		{"exec", "exec_key_conflict", ErrConflict},
		{"exec", "exec_ledger_capacity_exhausted", ErrExhausted},
		{"exec", "internal", ErrUnknown},
		{"exec", "future_code", ErrUnknown},
		{"exec", "", ErrUnknown},
	} {
		t.Run(scenario.method+"/"+scenario.code, func(t *testing.T) {
			resource := loom.Resource{
				RunDir: t.TempDir(),
			}
			if err := os.Mkdir(filepath.Join(resource.RunDir, "io"), 0o700); err != nil {
				t.Fatal(err)
			}
			listener, err := unixsocket.Listen(guestSocket(resource))
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
					port := 2046
					if scenario.method == "info" || scenario.method == "exec" {
						port = 2048
					}
					if line != fmt.Sprintf("CONNECT %d\n", port) {
						return fmt.Errorf("unexpected portal handshake %q", line)
					}
					if _, err = io.WriteString(conn, fmt.Sprintf("OK %d\n", port)); err != nil {
						return err
					}
					request, err := protocol.ReadFrame(reader)
					if err != nil {
						return err
					}
					if request.Method != scenario.method || request.Id == "" || !request.Eof {
						return fmt.Errorf("invalid correlated control request")
					}
					if scenario.method == "exec" && (request.Params.AsMap()["exec_key"] != "same-key" || request.Params.AsMap()["command"] != "true") {
						return fmt.Errorf("keyed execution identity changed")
					}
					params, err := structpb.NewStruct(map[string]any{
						"error_code": scenario.code,
					})
					if err != nil {
						return err
					}
					return protocol.WriteFrame(conn, &protocol.Frame{
						Id:     request.Id,
						Method: request.Method,
						Params: params,
						Error:  "private guest/agent/src/system.rs:42:7",
						Eof:    true,
					})
				}()
			}()
			client := LiveGuest{}
			switch scenario.method {
			case "info":
				err = client.Info(ctx, resource)
			case "activate":
				err = client.Activate(ctx, resource, Activation{})
			case "prepare_template":
				err = client.PrepareTemplate(ctx, resource)
			case "exec":
				err = client.Exec(ctx, resource, "same-key", "true")
			}
			if err == nil || (scenario.want != nil && !errors.Is(err, scenario.want)) || strings.Contains(err.Error(), "system.rs") {
				t.Fatalf("structured failure: %#v", err)
			}
			if err := <-done; err != nil && !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}
