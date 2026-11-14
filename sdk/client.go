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

	"github.com/woxQAQ/haven/api"
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

func (c *Client) call(ctx context.Context, method, path, key string, body, out any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.Endpoint+path, reader)
	if err != nil {
		return err
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
		return err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure api.Error
		if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&failure) != nil {
			return fmt.Errorf("haven HTTP %d", response.StatusCode)
		}
		return &failure
	}
	return json.NewDecoder(response.Body).Decode(out)
}

func (c *Client) Claim(ctx context.Context, key string, command api.Command) (api.Result, error) {
	var result api.Result
	err := c.call(ctx, "POST", "/v1/claim", key, command, &result)
	return result, err
}

func (c *Client) Change(ctx context.Context, id, key string, command api.Command) (api.Result, error) {
	var result api.Result
	err := c.call(ctx, "POST", "/v1/sandboxes/"+url.PathEscape(id)+"/"+url.PathEscape(command.Action), key, command, &result)
	return result, err
}

func (c *Client) List(ctx context.Context) ([]api.Sandbox, error) {
	var result []api.Sandbox
	err := c.call(ctx, "GET", "/v1/sandboxes", "", nil, &result)
	return result, err
}

func (c *Client) Get(ctx context.Context, id string) (api.Sandbox, error) {
	var result api.Sandbox
	err := c.call(ctx, "GET", "/v1/sandboxes/"+url.PathEscape(id), "", nil, &result)
	return result, err
}

func (c *Client) Operation(ctx context.Context, id string) (api.Operation, error) {
	var result api.Operation
	err := c.call(ctx, "GET", "/v1/operations/"+url.PathEscape(id), "", nil, &result)
	return result, err
}

func (c *Client) DeleteCheckpoint(ctx context.Context, id, key string) (api.Result, error) {
	var result api.Result
	err := c.call(ctx, "DELETE", "/v1/checkpoints/"+url.PathEscape(id), key, nil, &result)
	return result, err
}

func (c *Client) PutTemplate(ctx context.Context, key string, template api.Template, build bool) (api.Result, error) {
	action := "template_put"
	if build {
		action = "template_build"
	}
	var result api.Result
	err := c.call(ctx, "POST", "/v1/templates", key, api.Command{
		Action:   action,
		Template: &template,
	}, &result)
	return result, err
}

func (c *Client) PutTenant(ctx context.Context, tenant api.Tenant) (api.Tenant, error) {
	var result api.Tenant
	err := c.call(ctx, "POST", "/v1/tenants", "", tenant, &result)
	return result, err
}

func (c *Client) Templates(ctx context.Context) ([]api.Template, error) {
	var result []api.Template
	err := c.call(ctx, "GET", "/v1/templates", "", nil, &result)
	return result, err
}

func (c *Client) Checkpoints(ctx context.Context) ([]api.Checkpoint, error) {
	var result []api.Checkpoint
	err := c.call(ctx, "GET", "/v1/checkpoints", "", nil, &result)
	return result, err
}

type RPCError struct {
	Message     string
	Code        string
	ResultState string
}

func (err *RPCError) Error() string {
	return err.Message
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
		var failure api.Error
		if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&failure) == nil {
			return nil, &failure
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
			code, _ := values["error_code"].(string)
			state, _ := values["result_state"].(string)
			return values, &RPCError{
				Message:     frame.Error,
				Code:        code,
				ResultState: state,
			}
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
