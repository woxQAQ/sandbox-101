package loom

import (
	"context"
	"errors"
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
		return nil, errors.New("invalid guest portal")
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
		return nil, errors.New("guest portal handshake rejected")
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// GuestControlRequest is one correlated control frame. Portal authorization and
// durable mutation identities remain the caller's responsibility.
type GuestControlRequest struct {
	ID     string
	Method string
	Params map[string]any
}

// GuestControl performs one bounded, correlated control exchange. Diagnostics
// contain no guest-provided text or request values. An error cannot prove that a
// mutation did not happen, and keyed exec callers retain their original identity.
func GuestControl(ctx context.Context, socket string, port int, call GuestControlRequest) (map[string]any, error) {
	id, method, params := call.ID, call.Method, call.Params
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
			return nil, errors.New("uncorrelated guest response")
		}
		result := frame.Params.AsMap()
		if frame.Error != "" {
			return result, fmt.Errorf("guest %s control request failed", method)
		}
		output += len(frame.Data)
		if output > 1<<20 {
			return nil, errors.New("guest control output exceeds limit")
		}
		if frame.Eof {
			return result, nil
		}
	}
}
