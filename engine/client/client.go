// Package client provides the public engine UDS HTTP client.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/woxQAQ/sandbox-engine/api"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type Client struct{ http *http.Client }

var _ api.Client = (*Client)(nil)

func New(socketPath string) *Client {
	return &Client{http: &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}, MaxIdleConns: 16, IdleConnTimeout: 30 * time.Second}, Timeout: 30 * time.Second}}
}
func (c *Client) Close() { c.http.CloseIdleConnections() }
func (c *Client) call(ctx context.Context, method, path string, input, out any) error {
	var body io.Reader
	if input != nil {
		b, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	r, err := http.NewRequestWithContext(ctx, method, "http://engine/v1/"+path, body)
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	d := json.NewDecoder(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var a api.Error
		if err = d.Decode(&a); err != nil {
			return fmt.Errorf("engine HTTP %d: %w", resp.StatusCode, err)
		}
		return &a
	}
	return d.Decode(out)
}
func (c *Client) Submit(ctx context.Context, r api.Request) (v api.Operation, e error) {
	e = c.call(ctx, "POST", "operations", r, &v)
	return
}
func (c *Client) GetOperation(ctx context.Context, id string) (v api.Operation, e error) {
	e = c.call(ctx, "GET", "operations/"+url.PathEscape(id), nil, &v)
	return
}
func (c *Client) FindOperation(ctx context.Context, key string) (v api.Operation, e error) {
	e = c.call(ctx, "GET", "operations?key="+url.QueryEscape(key), nil, &v)
	return
}
func (c *Client) GetVM(ctx context.Context, id string) (v api.VM, e error) {
	e = c.call(ctx, "GET", "vms/"+url.PathEscape(id), nil, &v)
	return
}
func (c *Client) ListVMs(ctx context.Context) (v []api.VM, e error) {
	e = c.call(ctx, "GET", "vms", nil, &v)
	return
}
func (c *Client) GetSnapshot(ctx context.Context, id string) (v api.Snapshot, e error) {
	e = c.call(ctx, "GET", "snapshots/"+url.PathEscape(id), nil, &v)
	return
}
func (c *Client) ListSnapshots(ctx context.Context) (v []api.Snapshot, e error) {
	e = c.call(ctx, "GET", "snapshots", nil, &v)
	return
}
