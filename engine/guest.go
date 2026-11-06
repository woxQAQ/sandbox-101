package engine

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	protocol "github.com/woxQAQ/sandbox-protocol/gen"
)

func systemAgent(
	ctx context.Context,
	socket, instance string,
	restore bool,
) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	call := func(method string, params map[string]any, data []byte) error {
		var conn net.Conn
		var reader *bufio.Reader
		var err error
		for {
			conn, err = (&net.Dialer{Timeout: time.Second}).DialContext(
				ctx,
				"unix",
				socket,
			)
			if err == nil {
				deadline, _ := ctx.Deadline()
				_ = conn.SetDeadline(deadline)
				// CH exposes its Unix socket before the guest has a listener. A
				// rejected CONNECT closes that connection; retry only this setup,
				// before sending a control request or accepting any reply.
				if _, err = io.WriteString(conn, "CONNECT 2047\n"); err == nil {
					reader = bufio.NewReaderSize(conn, 4096)
					var line []byte
					line, err = reader.ReadSlice('\n')
					if err == nil {
						if len(line) > 128 ||
							!strings.HasPrefix(string(line), "OK ") {
							_ = conn.Close()
							return fmt.Errorf("vsock handshake rejected")
						}
						break
					}
				}
				_ = conn.Close()
			}
			t := time.NewTimer(50 * time.Millisecond)
			select {
			case <-ctx.Done():
				t.Stop()
				return fmt.Errorf("agent %s connect: %w", method, ctx.Err())
			case <-t.C:
			}
		}
		defer func() { _ = conn.Close() }()
		p, err := structpb.NewStruct(params)
		if err != nil {
			return err
		}
		id := instance + "/" + newID("sys-")
		if err = protocol.WriteFrame(
			conn,
			&protocol.Frame{
				Id:     id,
				Method: method,
				Params: p,
				Data:   data,
				Eof:    true,
			},
		); err != nil {
			return err
		}
		frame, err := protocol.ReadFrame(reader)
		if err != nil {
			return err
		}
		if frame.Id != id || !frame.Eof {
			return fmt.Errorf("uncorrelated or nonterminal system agent reply")
		}
		if frame.Error != "" {
			return fmt.Errorf("agent %s: %s", method, frame.Error)
		}
		if method == "ping" {
			if frame.Params == nil ||
				frame.Params.Fields["protocol"].GetNumberValue() != 1 ||
				frame.Params.Fields["portal"].GetStringValue() != "system" {
				return fmt.Errorf("incompatible system agent portal")
			}
		}
		return nil
	}
	if err := call("ping", map[string]any{}, nil); err != nil {
		return err
	}
	if restore {
		entropy := make([]byte, 64)
		if _, err := rand.Read(entropy); err != nil {
			return err
		}
		if err := call("reseed", map[string]any{}, entropy); err != nil {
			return err
		}
		now := time.Now()
		if err := call(
			"clock_sync",
			map[string]any{
				"unix_seconds": float64(now.Unix()),
				"nanoseconds":  float64(now.Nanosecond()),
			},
			nil,
		); err != nil {
			return err
		}
	}
	return nil
}
