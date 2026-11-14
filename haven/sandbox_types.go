package haven

import (
	"context"
	"net"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
)

type Principal struct {
	TenantID  string
	SandboxID string
	Root      bool
}

type (
	Injection struct {
		Hosts       []string `json:"hosts"`
		Header      string   `json:"header"`
		Placeholder string   `json:"placeholder,omitempty"`
	}
	EnvVar struct {
		Value  string     `json:"value"`
		Guest  *bool      `json:"guest,omitempty"`
		Inject *Injection `json:"inject,omitempty"`
	}
)

func (value EnvVar) isSecret() bool {
	return value.Guest != nil && !*value.Guest
}

// Command retains the sandbox API's intent fields. Only sanitized values may
// enter a durable request; a node-keyed MAC compares complete secret-bearing
// retries without retaining their values or a guessable credential digest.
type Command struct {
	ObservedGeneration string            `json:"observed_generation,omitempty"`
	Template           *Template         `json:"template,omitempty"`
	Action             string            `json:"action"`
	SandboxID          string            `json:"sandbox_id,omitempty"`
	CheckpointID       string            `json:"checkpoint_id,omitempty"`
	TemplateID         string            `json:"template_id,omitempty"`
	TTLSeconds         int64             `json:"ttl_seconds,omitempty"`
	OnExpire           string            `json:"on_expire,omitempty"`
	AutoRenew          bool              `json:"auto_renew,omitempty"`
	LeaseDeadline      time.Time         `json:"lease_deadline"`
	Env                map[string]EnvVar `json:"env,omitempty"`
	Policy             egress.Policy     `json:"policy"`
	Count              int               `json:"count,omitempty"`
}

type (
	Activation struct {
		SandboxID, Hostname string
		ResetIdentity       bool
		Env                 map[string]string
		Network, CAPEM      string
	}
	Guest interface {
		Info(context.Context, loom.Resource) error
		Activate(context.Context, loom.Resource, Activation) error
		PrepareTemplate(context.Context, loom.Resource) error
		Exec(context.Context, loom.Resource, string, string) error
		Connect(context.Context, loom.Resource) (net.Conn, error)
		Forward(context.Context, loom.Resource, int) (net.Conn, error)
	}
)

type Egress interface {
	Bind(context.Context, string, string, string, egress.Policy, map[string]egress.Secret) error
	Revoke(string) error
	Close() error
}

type sandboxSettings struct {
	Env         map[string]string    `json:"env"`
	Policy      egress.Policy        `json:"policy"`
	Secrets     map[string]Injection `json:"secrets,omitempty"`
	TTLSeconds  int64                `json:"ttl_seconds"`
	OnExpire    string               `json:"on_expire"`
	AutoRenew   bool                 `json:"auto_renew"`
	Deadline    time.Time            `json:"deadline"`
	PausedUntil time.Time            `json:"paused_until"`
	Generation  string               `json:"generation"`
	Instance    string               `json:"instance"`
	ActiveEpoch string               `json:"active_epoch,omitempty"`
}

type activationInput struct {
	PreviousSettings  *sandboxSettings `json:"previous_settings,omitempty"`
	Attempt           string           `json:"attempt,omitempty"`
	AllowFallback     bool             `json:"allow_fallback,omitempty"`
	RequestTenantID   string           `json:"request_tenant_id"`
	TenantID          string           `json:"tenant_id"`
	SandboxID         string           `json:"sandbox_id"`
	ResourceOperation string           `json:"resource_operation"`
	ResourceHash      string           `json:"resource_hash"`
	ExpectedInstance  string           `json:"expected_instance"`
	Settings          sandboxSettings  `json:"settings"`
	ResetIdentity     bool             `json:"reset_identity"`
}

type claimAdmission struct {
	Command  Command
	Settings sandboxSettings
	MAC      string
	Secrets  map[string]egress.Secret
}
