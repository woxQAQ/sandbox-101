package agent

import (
	"bufio"
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
)

func TestExecRequiresAndTransmitsExplicitKey(t *testing.T) {
	for _, test := range []struct {
		name   string
		params map[string]any
		error  string
		code   api.Code
	}{
		{"completed", map[string]any{"result_state": "completed", "exit_code": 0}, "", ""},
		{"saved", map[string]any{"result_state": "completed", "exit_code": 0, "replayed": true}, "", ""},
		{"unknown", map[string]any{"result_state": "result_unknown", "error_code": "result_unknown"}, "result_unknown", api.Unknown},
		{"pending", map[string]any{"result_state": "in_progress", "error_code": "exec_in_progress"}, "exec_in_progress", api.Conflict},
		{"mismatch", map[string]any{"error_code": "exec_key_conflict"}, "exec_key_conflict", api.Conflict},
		{"full", map[string]any{"error_code": "exec_ledger_capacity_exhausted"}, "exec_ledger_capacity_exhausted", api.Exhausted},
		{"legacy-agent", map[string]any{"exit_code": 0}, "", api.Unknown},
		{"missing-exit", map[string]any{"result_state": "completed"}, "", api.Unknown},
		{"disconnect", nil, "", api.Unknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "vsock.sock")
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			requests := make(chan *pb.Frame, 1)
			done := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				reader := bufio.NewReader(conn)
				line, err := reader.ReadString('\n')
				if err == nil && line != "CONNECT 2048\n" {
					err = errors.New("wrong guest port")
				}
				if err == nil {
					_, err = conn.Write([]byte("OK 2048\n"))
				}
				var frame *pb.Frame
				if err == nil {
					frame, err = Read(reader)
				}
				if err == nil {
					requests <- frame
					if test.params != nil {
						response, requestErr := Request(
							frame.Id,
							"exec",
							test.params,
						)
						err = requestErr
						if err == nil {
							response.Error = test.error
							err = Write(conn, response)
						}
					}
				}
				done <- err
			}()
			ctx, cancel := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancel()
			client := &Client{}
			err = client.Exec(
				ctx,
				api.VM{VSockPath: path},
				"sbx-op:operation:step:warmup",
				"printf warm",
			)
			if test.code == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var terminal *api.Error
				if !errors.As(err, &terminal) || terminal.Code != test.code {
					t.Fatalf("expected %s, got %v", test.code, err)
				}
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			request := <-requests
			if request.Method != "exec" || !request.Eof ||
				request.Params.AsMap()["exec_key"] != "sbx-op:operation:step:warmup" ||
				request.Params.AsMap()["command"] != "printf warm" {
				t.Fatalf("incorrect keyed warmup request: %+v", request)
			}
		})
	}
	if err := (&Client{}).Exec(
		context.Background(),
		api.VM{},
		"",
		"true",
	); err == nil {
		t.Fatal("empty exec_key was accepted")
	}
}
