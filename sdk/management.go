package sdk

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/woxQAQ/haven/api"
)

// ManagementClient speaks only the local protocol. Socket possession and kernel
// peer credentials establish administrator authority at the daemon.
type ManagementClient struct {
	client *Client
}

func NewManagement(socket string) *ManagementClient {
	client := New("http://haven.local", "")
	client.HTTP.Transport = &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{
				Timeout: 5 * time.Second,
			}).DialContext(ctx, "unix", socket)
		},
	}
	return &ManagementClient{
		client: client,
	}
}

func (client *ManagementClient) Status(ctx context.Context) (api.NodeStatus, error) {
	var out api.NodeStatus
	err := client.client.call(ctx, "GET", "/v1/status", "", nil, &out)
	return out, err
}

func (client *ManagementClient) Manage(ctx context.Context, request api.ManagementRequest) (api.ManagementResponse, error) {
	var out api.ManagementResponse
	err := client.client.call(ctx, "POST", "/v1/manage", "", request, &out)
	return out, err
}

func (client *ManagementClient) Operation(ctx context.Context, id string) (api.ManagementOperation, error) {
	var out api.ManagementOperation
	err := client.client.call(ctx, "GET", "/v1/operations/"+url.PathEscape(id), "", nil, &out)
	return out, err
}

func (client *Client) Status(ctx context.Context) (api.NodeStatus, error) {
	var out api.NodeStatus
	err := client.call(ctx, "GET", "/v1/status", "", nil, &out)
	return out, err
}
