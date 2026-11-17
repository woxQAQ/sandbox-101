package guest

import (
	"context"
	"net"

	"github.com/woxQAQ/haven/loom"
)

// Activation contains only guest-visible values. Host credentials stay in access.
type Activation struct {
	SandboxID     string
	Hostname      string
	ResetIdentity bool
	Env           map[string]string
	Network       string
	CAPEM         string
}

// Port exposes cooperation on the admitted resource's management socket.
// Replies do not establish resource ownership or authorize responsibility retirement.
type Port interface {
	Info(context.Context, loom.Resource) error
	Activate(context.Context, loom.Resource, Activation) error
	PrepareTemplate(context.Context, loom.Resource) error
	Exec(context.Context, loom.Resource, string, string) error
	Connect(context.Context, loom.Resource) (net.Conn, error)
	Forward(context.Context, loom.Resource, int) (net.Conn, error)
}
