// Package agent speaks the bounded Protobuf guest protocol over Cloud Hypervisor UDS.
package agent

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/woxQAQ/sandbox-engine/api"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
	"github.com/woxQAQ/sandboxd"
)

const MaxFrame = 8 << 20

type Client struct{}

var _ sandboxd.Agent = (*Client)(nil)

func Dial(ctx context.Context, path string, port int) (net.Conn, error) {
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	if _, err = fmt.Fprintf(c, "CONNECT %d\n", port); err != nil {
		_ = c.Close()
		return nil, err
	}
	// No agent frame can arrive until our first request. Read exactly the bounded handshake.
	var line []byte
	var b [1]byte
	for len(line) < 128 {
		if _, err = io.ReadFull(c, b[:]); err != nil {
			_ = c.Close()
			return nil, err
		}
		line = append(line, b[0])
		if b[0] == '\n' {
			break
		}
	}
	if !strings.HasPrefix(string(line), "OK ") || len(line) == 128 ||
		line[len(line)-1] != '\n' {
		_ = c.Close()
		return nil, errors.New("invalid CH vsock handshake")
	}
	if err = c.SetDeadline(time.Time{}); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func Write(w io.Writer, f *pb.Frame) error {
	b, err := proto.Marshal(f)
	if err != nil {
		return err
	}
	if len(b) == 0 || len(b) > MaxFrame {
		return errors.New("frame exceeds size limit")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(b)))
	if err = writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, b)
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func Read(r io.Reader) (*pb.Frame, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 || n > MaxFrame {
		return nil, errors.New("invalid frame length")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	f := new(pb.Frame)
	if err := proto.Unmarshal(b, f); err != nil {
		return nil, err
	}
	return f, nil
}

func Request(id, method string, params map[string]any) (*pb.Frame, error) {
	p, err := structpb.NewStruct(params)
	if err != nil {
		return nil, err
	}
	return &pb.Frame{Id: id, Method: method, Params: p, Eof: true}, nil
}

func rpc(
	ctx context.Context,
	v api.VM,
	port int,
	method string,
	params map[string]any,
) (map[string]any, error) {
	c, err := Dial(ctx, v.VSockPath, port)
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	f, err := Request("host", method, params)
	if err != nil {
		return nil, err
	}
	if err = Write(c, f); err != nil {
		return nil, err
	}
	size := 0
	for {
		r, err := Read(c)
		if err != nil {
			return nil, err
		}
		if r.Id != f.Id {
			return nil, errors.New("unexpected response id")
		}
		if r.Error != "" {
			if method == "exec" {
				switch r.Params.AsMap()["error_code"] {
				case "result_unknown":
					return nil, api.Fail(
						api.Unknown,
						"guest exec result unknown; do not retry under a new key",
					)
				case "exec_in_progress":
					return nil, api.Fail(
						api.Conflict,
						"guest exec is in progress; retain the same exec_key",
					)
				case "exec_key_conflict":
					return nil, api.Fail(
						api.Conflict,
						"guest exec_key has different parameters",
					)
				case "exec_ledger_capacity_exhausted":
					return nil, api.Fail(
						api.Exhausted,
						"guest exec ledger admission capacity exhausted",
					)
				}
			}
			return nil, fmt.Errorf("guest %s: %s", method, r.Error)
		}
		size += len(r.Data)
		if size > 1<<20 {
			return nil, errors.New("guest control output exceeds 1 MiB")
		}
		if r.Eof {
			return r.Params.AsMap(), nil
		}
	}
}

func (c *Client) Info(ctx context.Context, v api.VM) error {
	p, err := rpc(ctx, v, 2048, "info", map[string]any{})
	if err != nil {
		return err
	}
	if p["portal"] != "product" || p["protocol"] != float64(1) {
		return errors.New("incompatible product agent")
	}
	return nil
}

func (c *Client) Activate(
	ctx context.Context,
	v api.VM,
	a sandboxd.Activation,
) error {
	env := map[string]any{}
	for k, v := range a.Env {
		env[k] = v
	}
	_, err := rpc(
		ctx,
		v,
		2046,
		"activate",
		map[string]any{
			"sandbox_id":     a.SandboxID,
			"hostname":       a.Hostname,
			"reset_identity": a.ResetIdentity,
			"env":            env,
			"network":        a.Network,
			"ca_pem":         a.CAPEM,
		},
	)
	return err
}

func (c *Client) PrepareTemplate(ctx context.Context, v api.VM) error {
	p, err := rpc(ctx, v, 2046, "prepare_template", map[string]any{})
	if err != nil {
		return err
	}
	if p["prepared"] != true || p["quiescent"] != true {
		return errors.New("guest template was not prepared")
	}
	return nil
}

func (c *Client) Exec(
	ctx context.Context,
	v api.VM,
	execKey, command string,
) error {
	if execKey == "" {
		return errors.New("guest warmup requires an explicit exec_key")
	}
	p, err := rpc(
		ctx,
		v,
		2048,
		"exec",
		map[string]any{"command": command, "exec_key": execKey},
	)
	if err != nil {
		if _, ok := errors.AsType[*api.Error](err); ok {
			return err
		}
		return api.Fail(
			api.Unknown,
			"guest exec completion unconfirmed; retain the same exec_key: %v",
			err,
		)
	}
	if p["result_state"] != "completed" {
		return api.Fail(
			api.Unknown,
			"guest did not confirm a durable keyed exec result",
		)
	}
	if _, ok := p["exit_code"].(float64); !ok {
		return api.Fail(
			api.Unknown,
			"guest keyed exec result omitted exit_code",
		)
	}
	if code := p["exit_code"].(float64); code != 0 {
		return fmt.Errorf("guest warmup failed: exit_code=%v", p["exit_code"])
	}
	return nil
}

func (c *Client) Connect(ctx context.Context, v api.VM) (net.Conn, error) {
	return Dial(ctx, v.VSockPath, 2048)
}

func (c *Client) Forward(
	ctx context.Context,
	v api.VM,
	port int,
) (net.Conn, error) {
	conn, err := c.Connect(ctx, v)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	f, err := Request("forward", "port_forward", map[string]any{"port": port})
	if err == nil {
		err = Write(conn, f)
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	r, err := Read(conn)
	if err != nil || r.Error != "" || r.Id != f.Id ||
		r.Params.AsMap()["ready"] != true {
		_ = conn.Close()
		return nil, errors.New("guest port forward failed")
	}
	return conn, nil
}

// BufferedConn preserves bytes already received with an HTTP Upgrade response.
type BufferedConn struct {
	net.Conn

	Reader *bufio.Reader
}

func (c *BufferedConn) Read(b []byte) (int, error) { return c.Reader.Read(b) }
func (c *BufferedConn) CloseWrite() error {
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return errors.New("connection does not support half-close")
}
