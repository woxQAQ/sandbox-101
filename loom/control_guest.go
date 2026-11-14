package loom

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/woxQAQ/haven/loom/unixsocket"
	protocol "github.com/woxQAQ/sandbox-protocol/gen"
)

// DialGuest connects an explicitly selected host portal. Authorization and portal
// selection belong to the caller; tenant relays must always select Workload.
func DialGuest(ctx context.Context, socket string, port int) (net.Conn, error) {
	if port < 1 || port > 65535 {
		return nil, Fail(Invalid, "invalid guest portal")
	}
	conn, err := unixsocket.Dial(ctx, socket)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stop()
	if _, err = fmt.Fprintf(conn, "CONNECT %d\n", port); err != nil {
		_ = conn.Close()
		return nil, err
	}
	var line []byte
	var one [1]byte
	for len(line) < 128 {
		if _, err = io.ReadFull(conn, one[:]); err != nil {
			_ = conn.Close()
			return nil, err
		}
		line = append(line, one[0])
		if one[0] == '\n' {
			break
		}
	}
	fields := strings.Fields(string(line))
	if len(line) == 0 || line[len(line)-1] != '\n' || len(fields) != 2 || fields[0] != "OK" || fields[1] != strconv.Itoa(port) {
		_ = conn.Close()
		return nil, Fail(Incompatible, "guest portal handshake rejected")
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// GuestControl performs one bounded, correlated control exchange. Diagnostics
// contain no guest-provided text or request values. An error cannot prove that a
// mutation did not happen, and keyed exec callers retain their original identity.
func GuestControl(ctx context.Context, socket string, port int, id, method string, params map[string]any) (map[string]any, error) {
	conn, err := DialGuest(ctx, socket, port)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()
	stop := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stop()
	requestParams, err := structpb.NewStruct(params)
	if err != nil {
		return nil, err
	}
	request := &protocol.Frame{
		Id:     id,
		Method: method,
		Params: requestParams,
		Eof:    true,
	}
	if err = protocol.WriteFrame(conn, request); err != nil {
		return nil, err
	}
	output := 0
	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			return nil, err
		}
		if frame.Id != id {
			return nil, Fail(Unknown, "uncorrelated guest response")
		}
		result := frame.Params.AsMap()
		if frame.Error != "" {
			code := Internal
			switch result["error_code"] {
			case "result_unknown":
				code = Unknown
			case "exec_in_progress", "exec_key_conflict", "conflict", "quiesced":
				code = Conflict
			case "resource_exhausted", "exec_ledger_capacity_exhausted":
				code = Exhausted
			case "not_found":
				code = ManualRecovery
			case "invalid":
				if method == "exec" {
					code = Invalid
				}
			}
			return result, Fail(code, "guest control request failed")
		}
		output += len(frame.Data)
		if output > 1<<20 {
			return nil, Fail(Unknown, "guest control output exceeds limit")
		}
		if frame.Eof {
			return result, nil
		}
	}
}
