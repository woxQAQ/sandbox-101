// Package sdk provides the control API and end-to-end guest RPC client.
package sdk

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/woxQAQ/sandbox-engine/api"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
	"github.com/woxQAQ/sandboxd"
	"github.com/woxQAQ/sandboxd/agent"
)

type Client struct {
	Endpoint string
	Token    string
	HTTP     *http.Client
	TLS      *tls.Config
}

func New(endpoint, token string) *Client {
	return &Client{
		Endpoint: strings.TrimRight(endpoint, "/"),
		Token:    token,
		HTTP: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func (c *Client) call(
	ctx context.Context,
	method, path, key string,
	body, out any,
) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Endpoint+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e api.Error
		if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&e) != nil {
			return fmt.Errorf("sandboxd HTTP %d", resp.StatusCode)
		}
		return &e
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) Claim(
	ctx context.Context,
	key string,
	cmd sandboxd.Command,
) (sandboxd.Result, error) {
	var r sandboxd.Result
	err := c.call(ctx, "POST", "/v1/claim", key, cmd, &r)
	return r, err
}

func (c *Client) Change(
	ctx context.Context,
	id, key string,
	cmd sandboxd.Command,
) (sandboxd.Result, error) {
	var r sandboxd.Result
	err := c.call(
		ctx,
		"POST",
		"/v1/sandboxes/"+url.PathEscape(id)+"/"+url.PathEscape(cmd.Action),
		key,
		cmd,
		&r,
	)
	return r, err
}

func (c *Client) List(ctx context.Context) ([]sandboxd.Sandbox, error) {
	var r []sandboxd.Sandbox
	err := c.call(ctx, "GET", "/v1/sandboxes", "", nil, &r)
	return r, err
}

func (c *Client) Get(ctx context.Context, id string) (sandboxd.Sandbox, error) {
	var r sandboxd.Sandbox
	err := c.call(ctx, "GET", "/v1/sandboxes/"+url.PathEscape(id), "", nil, &r)
	return r, err
}

func (c *Client) DeleteCheckpoint(
	ctx context.Context,
	id, key string,
) (sandboxd.Result, error) {
	var r sandboxd.Result
	err := c.call(
		ctx,
		"DELETE",
		"/v1/checkpoints/"+url.PathEscape(id),
		key,
		nil,
		&r,
	)
	return r, err
}

func (c *Client) PutTemplate(
	ctx context.Context,
	key string,
	t sandboxd.Template,
	build bool,
) (sandboxd.Result, error) {
	action := "template_put"
	if build {
		action = "template_build"
	}
	var r sandboxd.Result
	err := c.call(
		ctx,
		"POST",
		"/v1/templates",
		key,
		sandboxd.Command{Action: action, Template: &t},
		&r,
	)
	return r, err
}

// RPCError reports a guest error. Code and ResultState retain keyed execution
// outcomes, including result_unknown; transport errors do not use this type.
type RPCError struct {
	Message     string
	Code        string
	ResultState string
}

func (e *RPCError) Error() string { return e.Message }

type Connection struct {
	net.Conn

	mu   sync.Mutex
	next uint64
}

func (c *Client) Connect(ctx context.Context, id string) (*Connection, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("HTTP(S) endpoint required")
	}
	address := u.Host
	if u.Port() == "" {
		port := "80"
		if u.Scheme == "https" {
			port = "443"
		}
		address = net.JoinHostPort(u.Hostname(), port)
	}
	var conn net.Conn
	if u.Scheme == "https" {
		config := c.TLS
		if config != nil {
			config = config.Clone()
		} else {
			config = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		if config.ServerName == "" {
			config.ServerName = u.Hostname()
		}
		conn, err = (&tls.Dialer{NetDialer: &net.Dialer{}, Config: config}).DialContext(
			ctx,
			"tcp",
			address,
		)
	} else {
		conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	u.Path = strings.TrimRight(
		u.Path,
		"/",
	) + "/v1/sandboxes/" + url.PathEscape(
		id,
	) + "/connect"
	req := &http.Request{
		Method: "GET",
		URL:    u,
		Host:   u.Host,
		Header: make(http.Header),
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "sandbox-v1")
	if err = req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if resp.StatusCode != 101 ||
		!strings.EqualFold(resp.Header.Get("Upgrade"), "sandbox-v1") {
		defer func() { _ = resp.Body.Close() }()
		defer func() { _ = conn.Close() }()
		var e api.Error
		if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&e) == nil {
			return nil, &e
		}
		return nil, fmt.Errorf("sandbox upgrade HTTP %d", resp.StatusCode)
	}
	return &Connection{Conn: &agent.BufferedConn{Conn: conn, Reader: br}}, nil
}

// Call serializes requests on a reusable stream. consume receives each bounded
// output frame before another is read, preserving downstream backpressure.
// Guest errors return the terminal parameters alongside an RPCError.
func (c *Connection) Call(
	ctx context.Context,
	method string,
	params map[string]any,
	consume func(*pb.Frame) error,
) (map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	id := fmt.Sprint(c.next)
	f, err := agent.Request(id, method, params)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	if err = agent.Write(c, f); err != nil {
		return nil, err
	}
	for {
		r, err := agent.Read(c)
		if err != nil {
			return nil, err
		}
		if r.Id != id {
			return nil, errors.New("unexpected RPC id")
		}
		if r.Error != "" {
			params := r.Params.AsMap()
			code, _ := params["error_code"].(string)
			state, _ := params["result_state"].(string)
			return params, &RPCError{
				Message:     r.Error,
				Code:        code,
				ResultState: state,
			}
		}
		if consume != nil {
			if err = consume(r); err != nil {
				_ = c.Close()
				return nil, err
			}
		}
		if r.Eof {
			return r.Params.AsMap(), nil
		}
	}
}

func (c *Connection) Exec(
	ctx context.Context,
	command string,
	stdout, stderr io.Writer,
) (int, error) {
	p, err := c.Call(
		ctx,
		"exec",
		map[string]any{"command": command},
		func(f *pb.Frame) error {
			if len(f.Data) == 0 {
				return nil
			}
			out := stdout
			if f.Params.AsMap()["stream"] == "stderr" {
				out = stderr
			}
			if out == nil {
				out = io.Discard
			}
			_, err := out.Write(f.Data)
			return err
		},
	)
	if err != nil {
		return -1, err
	}
	code, ok := p["exit_code"].(float64)
	if !ok {
		return -1, errors.New("guest omitted exit code")
	}
	return int(code), nil
}

// Frames exposes protocol primitives for fs streams, PTYs, sessions and watches.
// Callers using Frames must own the connection exclusively until the RPC ends.
func (c *Connection) Send(f *pb.Frame) error      { return agent.Write(c, f) }
func (c *Connection) Receive() (*pb.Frame, error) { return agent.Read(c) }
func (c *Connection) CloseWrite() error {
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return errors.New("connection does not support half-close")
}
