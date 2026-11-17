package sdk

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/woxQAQ/haven/types"
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

func (client *ManagementClient) Status(ctx context.Context) (types.NodeStatus, error) {
	return call[types.NodeStatus](ctx, client.client, controlRequest{
		Method: "GET",
		Path:   "/v1/status",
	})
}

func (client *ManagementClient) Manage(ctx context.Context, request types.ManagementRequest) (types.ManagementResponse, error) {
	return call[types.ManagementResponse](ctx, client.client, controlRequest{
		Method: "POST",
		Path:   "/v1/manage",
		Body:   request,
	})
}

func (client *ManagementClient) Operation(ctx context.Context, id string) (types.ManagementOperation, error) {
	return call[types.ManagementOperation](ctx, client.client, controlRequest{
		Method: "GET",
		Path:   "/v1/operations/" + url.PathEscape(id),
	})
}

func (client *Client) Status(ctx context.Context) (types.NodeStatus, error) {
	return call[types.NodeStatus](ctx, client, controlRequest{
		Method: "GET",
		Path:   "/v1/status",
	})
}
