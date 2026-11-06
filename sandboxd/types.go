// Package sandboxd implements single-node sandbox orchestration over the engine API.
package sandboxd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

type Principal struct {
	TenantID  string
	SandboxID string
	Root      bool
}
type Tenant struct {
	ID            string `json:"id"`
	MaxSandboxes  int    `json:"max_sandboxes"`
	MaxStorageMiB int64  `json:"max_storage_mib"`
	Token         string `json:"token,omitempty"`
}
type Injection struct {
	Hosts       []string `json:"hosts"`
	Header      string   `json:"header"`
	Placeholder string   `json:"placeholder,omitempty"`
}
type EnvVar struct {
	Value  string     `json:"value"`
	Guest  *bool      `json:"guest,omitempty"`
	Inject *Injection `json:"inject,omitempty"`
}

func (v EnvVar) IsSecret() bool { return v.Guest != nil && !*v.Guest }

type Template struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	Image      string    `json:"image"`
	Shape      api.Shape `json:"shape"`
	SnapshotID string    `json:"snapshot_id,omitempty"`
	State      string    `json:"state"`
	TargetWarm int       `json:"target_warm"`
	Warmup     string    `json:"warmup,omitempty"`
}
type Sandbox struct {
	ID              string            `json:"id"`
	TenantID        string            `json:"tenant_id"`
	State           string            `json:"state"`
	VMID            string            `json:"vm_id,omitempty"`
	SnapshotID      string            `json:"snapshot_id,omitempty"`
	Instance        string            `json:"instance,omitempty"`
	TemplateID      string            `json:"template_id"`
	ProvisionSource string            `json:"provision_source,omitempty"`
	Shape           api.Shape         `json:"shape"`
	Deadline        time.Time         `json:"deadline"`
	PausedUntil     time.Time         `json:"paused_until"`
	TTLSeconds      int64             `json:"ttl_seconds"`
	OnExpire        string            `json:"on_expire"`
	AutoRenew       bool              `json:"auto_renew"`
	Env             map[string]string `json:"-"`
	Policy          egress.Policy     `json:"policy"`
	RequiresSecrets bool              `json:"requires_secrets"`
	ResumeAttempt   uint64            `json:"resume_attempt,omitempty"`
	Epoch           string            `json:"creation_epoch"`
	OperationID     string            `json:"operation_id,omitempty"`
	LastError       string            `json:"last_error,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
}
type Checkpoint struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	SandboxID  string    `json:"sandbox_id"`
	SnapshotID string    `json:"snapshot_id"`
	Shape      api.Shape `json:"shape"`
	CreatedAt  time.Time `json:"created_at"`
}
type Command struct {
	Action        string            `json:"action"`
	SandboxID     string            `json:"sandbox_id,omitempty"`
	CheckpointID  string            `json:"checkpoint_id,omitempty"`
	TemplateID    string            `json:"template_id,omitempty"`
	Template      *Template         `json:"template,omitempty"`
	TTLSeconds    int64             `json:"ttl_seconds,omitempty"`
	OnExpire      string            `json:"on_expire,omitempty"`
	AutoRenew     bool              `json:"auto_renew,omitempty"`
	LeaseDeadline time.Time         `json:"lease_deadline"`
	Env           map[string]EnvVar `json:"env,omitempty"`
	Policy        egress.Policy     `json:"policy"`
	Count         int               `json:"count,omitempty"`
}
type Result struct {
	OperationID string            `json:"operation_id"`
	Sandbox     *Sandbox          `json:"sandbox,omitempty"`
	Sandboxes   []Sandbox         `json:"sandboxes,omitempty"`
	Tokens      map[string]string `json:"tokens,omitempty"`
	Checkpoint  *Checkpoint       `json:"checkpoint,omitempty"`
	Template    *Template         `json:"template,omitempty"`
}
type Operation struct {
	ID             string     `json:"id"`
	Key            string     `json:"key"`
	TenantID       string     `json:"tenant_id"`
	ChargeTenantID string     `json:"charge_tenant_id,omitempty"`
	Hash           string     `json:"-"`
	Command        Command    `json:"command"`
	IDs            []string   `json:"resource_ids,omitempty"`
	Status         string     `json:"status"`
	Step           string     `json:"step"`
	SnapshotID     string     `json:"snapshot_id,omitempty"`
	OwnedSnapshot  bool       `json:"owned_snapshot,omitempty"`
	ReservedMiB    int64      `json:"reserved_mib,omitempty"`
	SourceInstance string     `json:"source_instance,omitempty"`
	Result         Result     `json:"result"`
	Error          *api.Error `json:"error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	FinishedAt     time.Time  `json:"finished_at"`
}
type Activation struct {
	SandboxID     string
	Hostname      string
	ResetIdentity bool
	Env           map[string]string
	Network       string
	CAPEM         string
}
type Agent interface {
	Info(context.Context, api.VM) error
	Activate(context.Context, api.VM, Activation) error
	PrepareTemplate(context.Context, api.VM) error
	Exec(context.Context, api.VM, string, string) error
	Connect(context.Context, api.VM) (net.Conn, error)
	Forward(context.Context, api.VM, int) (net.Conn, error)
}
type Egress interface {
	Bind(
		context.Context,
		string,
		string,
		string,
		egress.Policy,
		map[string]egress.Secret,
	) error
	Revoke(string) error
	Close() error
}
type Options struct {
	StateDir   string
	Initialize bool
	RootToken  string
	Engine     api.Client
	Agent      Agent
	Egress     Egress
	MaxTTL     time.Duration
	PausedTTL  time.Duration
	Interval   time.Duration
	MaxWarm    int
	CAPEM      string
}

func newID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b[:])
}

func stepKey(
	op Operation,
	step string,
) string {
	return "sbx-op:" + op.ID + ":step:" + step
}

func internalError(err error) *api.Error {
	if e, ok := errors.AsType[*api.Error](err); ok {
		return e
	}
	return &api.Error{Code: api.Internal, Message: fmt.Sprint(err)}
}
