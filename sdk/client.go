// Package sdk provides Haven control and end-to-end guest RPC clients.
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

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/woxQAQ/haven/types"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
)

type Client struct {
	Endpoint string
	Token    string
	Tenant   string
	HTTP     *http.Client
	TLS      *tls.Config
}

func New(endpoint, token string) *Client {
	return &Client{
		Endpoint: strings.TrimRight(endpoint, "/"),
		Token:    token,
		HTTP: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func call[T any](ctx context.Context, c *Client, method, path, key string, body any) (T, error) {
	var result T
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return result, err
		}
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.Endpoint+path, reader)
	if err != nil {
		return result, err
	}
	request.Header.Set("Authorization", "Bearer "+c.Token)
	if c.Tenant != "" {
		request.Header.Set("X-Tenant-ID", c.Tenant)
	}
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := c.HTTP.Do(request)
	if err != nil {
		return result, err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure types.ApiError
		if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&failure) != nil {
			return result, fmt.Errorf("haven HTTP %d", response.StatusCode)
		}
		return result, fmt.Errorf("%s: %s", failure.Code, failure.Message)
	}
	err = json.NewDecoder(response.Body).Decode(&result)
	return result, err
}

func (c *Client) Claim(ctx context.Context, key string, command types.Command) (types.Result, error) {
	return call[types.Result](ctx, c, "POST", "/v1/claim", key, command)
}

func (c *Client) Change(ctx context.Context, id, key string, command types.Command) (types.Result, error) {
	return call[types.Result](ctx, c, "POST", "/v1/sandboxes/"+url.PathEscape(id)+"/"+url.PathEscape(command.Action), key, command)
}

func (c *Client) List(ctx context.Context) ([]types.Sandbox, error) {
	return call[[]types.Sandbox](ctx, c, "GET", "/v1/sandboxes", "", nil)
}

func (c *Client) Get(ctx context.Context, id string) (types.Sandbox, error) {
	return call[types.Sandbox](ctx, c, "GET", "/v1/sandboxes/"+url.PathEscape(id), "", nil)
}

func (c *Client) Operation(ctx context.Context, id string) (types.Operation, error) {
	return call[types.Operation](ctx, c, "GET", "/v1/operations/"+url.PathEscape(id), "", nil)
}

func (c *Client) DeleteCheckpoint(ctx context.Context, id, key string) (types.Result, error) {
	return call[types.Result](ctx, c, "DELETE", "/v1/checkpoints/"+url.PathEscape(id), key, nil)
}

func (c *Client) PutTemplate(ctx context.Context, key string, template types.Template, build bool) (types.Result, error) {
	action := "template_put"
	if build {
		action = "template_build"
	}
	return call[types.Result](ctx, c, "POST", "/v1/templates", key, types.Command{
		Action:   action,
		Template: &template,
	})
}

func (c *Client) PutTenant(ctx context.Context, tenant types.Tenant) (types.Tenant, error) {
	return call[types.Tenant](ctx, c, "POST", "/v1/tenants", "", tenant)
}

func (c *Client) Templates(ctx context.Context) ([]types.Template, error) {
	return call[[]types.Template](ctx, c, "GET", "/v1/templates", "", nil)
}

func (c *Client) Checkpoints(ctx context.Context) ([]types.Checkpoint, error) {
	return call[[]types.Checkpoint](ctx, c, "GET", "/v1/checkpoints", "", nil)
}

type Connection struct {
	net.Conn

	// The token owns the stream and RPC counter, including cancellation cleanup.
	once     sync.Once
	admit    chan struct{}
	next     uint64
	unusable bool
}

func (c *Connection) acquire(ctx context.Context) error {
	c.once.Do(func() {
		c.admit = make(chan struct{}, 1)
		c.admit <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.admit:
	}
	if c.unusable {
		c.release()
		return errors.New("connection was closed by a canceled call")
	}
	if err := ctx.Err(); err != nil {
		c.release()
		return err
	}
	return nil
}

func (c *Connection) release() {
	c.admit <- struct{}{}
}

func (c *Client) Connect(ctx context.Context, id string) (*Connection, error) {
	endpoint, err := url.Parse(c.Endpoint)
	if err != nil {
		return nil, err
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, errors.New("HTTP(S) endpoint required")
	}
	address := endpoint.Host
	if endpoint.Port() == "" {
		port := "80"
		if endpoint.Scheme == "https" {
			port = "443"
		}
		address = net.JoinHostPort(endpoint.Hostname(), port)
	}
	var conn net.Conn
	if endpoint.Scheme == "https" {
		config := c.TLS
		if config != nil {
			config = config.Clone()
		} else {
			config = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		}
		if config.ServerName == "" {
			config.ServerName = endpoint.Hostname()
		}
		conn, err = (&tls.Dialer{
			NetDialer: &net.Dialer{},
			Config:    config,
		}).DialContext(ctx, "tcp", address)
	} else {
		conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stop()
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/v1/sandboxes/" + url.PathEscape(id) + "/connect"
	request := &http.Request{
		Method: "GET",
		URL:    endpoint,
		Host:   endpoint.Host,
		Header: make(http.Header),
	}
	request.Header.Set("Authorization", "Bearer "+c.Token)
	if c.Tenant != "" {
		request.Header.Set("X-Tenant-ID", c.Tenant)
	}
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "sandbox-v1")
	if err = request.Write(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if response.StatusCode != 101 || !strings.EqualFold(response.Header.Get("Upgrade"), "sandbox-v1") {
		defer func() {
			_ = response.Body.Close()
		}()
		defer func() {
			_ = conn.Close()
		}()
		var failure types.ApiError
		if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&failure) == nil {
			return nil, fmt.Errorf("%s: %s", failure.Code, failure.Message)
		}
		return nil, fmt.Errorf("sandbox upgrade HTTP %d", response.StatusCode)
	}
	return &Connection{
		Conn: &bufferedConn{
			Conn:   conn,
			reader: reader,
		},
	}, nil
}

type bufferedConn struct {
	net.Conn

	reader *bufio.Reader
}

func (c *bufferedConn) Read(buffer []byte) (int, error) {
	return c.reader.Read(buffer)
}

func (c *bufferedConn) CloseWrite() error {
	if half, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite()
	}
	return errors.New("connection does not support half-close")
}

// Call serializes requests and consumes bounded frames with downstream backpressure.
// Guest errors retain their machine category and keyed execution outcome.
func (c *Connection) Call(ctx context.Context, method string, params map[string]any, consume func(*pb.Frame) error) (map[string]any, error) {
	if err := c.acquire(ctx); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = c.Close()
		close(done)
	})
	defer func() {
		if !stop() {
			<-done
			c.unusable = true
		}
		c.release()
	}()
	c.next++
	id := fmt.Sprint(c.next)
	parameters, err := structpb.NewStruct(params)
	if err != nil {
		return nil, err
	}
	if err = pb.WriteFrame(c, &pb.Frame{
		Id:     id,
		Method: method,
		Params: parameters,
		Eof:    true,
	}); err != nil {
		return nil, err
	}
	for {
		frame, err := pb.ReadFrame(c)
		if err != nil {
			return nil, err
		}
		if frame.Id != id {
			return nil, errors.New("unexpected RPC id")
		}
		if frame.Error != "" {
			values := frame.Params.AsMap()
			return values, errors.New(frame.Error)
		}
		if consume != nil {
			if err = consume(frame); err != nil {
				_ = c.Close()
				return nil, err
			}
		}
		if frame.Eof {
			return frame.Params.AsMap(), nil
		}
	}
}

func (c *Connection) Exec(ctx context.Context, command string, stdout, stderr io.Writer) (int, error) {
	params, err := c.Call(ctx, "exec", map[string]any{
		"command": command,
	}, func(frame *pb.Frame) error {
		if len(frame.Data) == 0 {
			return nil
		}
		output := stdout
		if frame.Params.AsMap()["stream"] == "stderr" {
			output = stderr
		}
		if output == nil {
			output = io.Discard
		}
		_, err := output.Write(frame.Data)
		return err
	})
	if err != nil {
		return -1, err
	}
	code, ok := params["exit_code"].(float64)
	if !ok {
		return -1, errors.New("guest omitted exit code")
	}
	return int(code), nil
}

// Send and Receive expose frames while the caller owns the connection exclusively.
func (c *Connection) Send(frame *pb.Frame) error {
	return pb.WriteFrame(c, frame)
}

func (c *Connection) Receive() (*pb.Frame, error) {
	return pb.ReadFrame(c)
}

func (c *Connection) CloseWrite() error {
	if half, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite()
	}
	return errors.New("connection does not support half-close")
}
