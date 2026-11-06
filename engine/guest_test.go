package engine

import (
	"bufio"
	"context"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/woxQAQ/sandbox-protocol/gen"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestSystemAgentWaitsForGuestVsockListener(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "vsock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var attempts atomic.Int32
	done := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			reader := bufio.NewReader(conn)
			line, err := reader.ReadString('\n')
			if err != nil || line != "CONNECT 2047\n" {
				conn.Close()
				done <- io.ErrUnexpectedEOF
				return
			}
			if attempts.Add(1) < 3 {
				conn.Close()
				continue
			}
			if _, err := io.WriteString(conn, "OK 2047\n"); err != nil {
				conn.Close()
				done <- err
				return
			}
			frame, err := protocol.ReadFrame(reader)
			if err == nil {
				params, _ := structpb.NewStruct(map[string]any{"protocol": 1, "portal": "system"})
				err = protocol.WriteFrame(conn, &protocol.Frame{Id: frame.Id, Params: params, Eof: true})
			}
			conn.Close()
			done <- err
			return
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := systemAgent(ctx, socket, "instance", false); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected two pre-listener rejections, got %d attempts", attempts.Load())
	}
}
