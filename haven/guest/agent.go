package guest

import (
	"context"
	"net"

	"github.com/woxQAQ/haven/loom"
)

// Activation contains only guest-visible values. Host credentials stay in ephemeral.
type Activation struct {
	SandboxID     string
	Hostname      string
	ResetIdentity bool
	Env           map[string]string
	Network       string
	CAPEM         string
}

// Agent exposes cooperation with the guest agent on the admitted resource's
// management socket. Guest replies prove only that a specific agent answered a
// specific request on the candidate resource; they do not establish resource
// ownership, do not authorize responsibility retirement, and cannot replace the
// durable step receipts the host workflows verify.
type Agent interface {
	// Info verifies the Workload portal answers protocol version 1 on the
	// candidate resource. A mismatch means the resource cannot serve sandbox
	// traffic and must not be activated.
	Info(ctx context.Context, resource loom.Resource) error
	// Activate delivers sandbox identity, hostname, environment, network mode
	// and egress CA to the Admin portal. ResetIdentity asks the guest to
	// regenerate machine identity for a forked branch.
	Activate(ctx context.Context, resource loom.Resource, activation Activation) error
	// PrepareTemplate asks the Admin portal to quiesce for template capture.
	// The guest must explicitly confirm both prepared and quiescent state;
	// without that confirmation the capture must not proceed.
	PrepareTemplate(ctx context.Context, resource loom.Resource) error
	// Exec runs one keyed warmup command on the Workload portal. execKey is
	// the durable guest ledger identity of this execution: retries with the
	// same key join the original execution instead of starting a new one. A
	// nil error requires a confirmed durable completion with exit status zero;
	// a nonzero exit status is an ordinary failure, and any missing or
	// ambiguous completion proof maps to result-unknown rather than success.
	Exec(ctx context.Context, resource loom.Resource, execKey, command string) error
	// Connect opens a raw stream to the Workload portal of the candidate
	// resource. On success the caller owns the connection and must close it.
	Connect(ctx context.Context, resource loom.Resource) (net.Conn, error)
	// Forward opens a Workload stream and negotiates port forwarding to the
	// guest TCP port before returning. On success the stream is fully raw
	// duplex, the caller owns the connection and must close it.
	Forward(ctx context.Context, resource loom.Resource, port int) (net.Conn, error)
}
