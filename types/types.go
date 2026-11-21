// Package types defines Haven's wire data without host execution dependencies.
package types

import "time"

// Shape fixes the CPU, RAM and writable-disk capacity of a template or sandbox.
// Memory and disk sizes use MiB (1 << 20 bytes).
type Shape struct {
	VCPUs     int   `json:"vcpus"`
	MemoryMiB int64 `json:"memory_mib"`
	DiskMiB   int64 `json:"disk_mib"`
}

// Policy controls outbound access. Mode is none, proxy or direct.
type Policy struct {
	Mode           string   `json:"mode"`
	AllowedHosts   []string `json:"allowed_hosts,omitempty"`   // Proxy-only host allowlist.
	AllowedCIDRs   []string `json:"allowed_cidrs,omitempty"`   // Canonical IPv4 prefixes for direct mode.
	MaxConnections int      `json:"max_connections,omitempty"` // Direct-mode tracked flow limit.
}

// Injection supplies a host-only credential to matching proxy HTTP requests.
type Injection struct {
	Hosts       []string `json:"hosts"`
	Header      string   `json:"header"`
	Placeholder string   `json:"placeholder,omitempty"` // Harmless replacement exposed to the guest.
}

// EnvVar separates guest-visible environment values from host-only secrets.
type EnvVar struct {
	Value  string     `json:"value"`
	Guest  *bool      `json:"guest,omitempty"` // Nil or true exposes Value to the guest; false keeps it on the host.
	Inject *Injection `json:"inject,omitempty"`
}

// IsSecret reports whether the value is reserved for host-side injection.
func (value EnvVar) IsSecret() bool {
	return value.Guest != nil && !*value.Guest
}

// Template publishes a tenant-owned image and shape, optionally with a golden snapshot.
type Template struct {
	ID         string `json:"id"`
	TenantID   string `json:"tenant_id"` // Permanent owner; root publication must select it explicitly.
	Image      string `json:"image"`     // Published OCI manifest digest.
	Shape      Shape  `json:"shape"`
	SnapshotID string `json:"snapshot_id,omitempty"`
	State      string `json:"state"`
	TargetWarm int    `json:"target_warm"`      // Desired number of unclaimed warm instances.
	Warmup     string `json:"warmup,omitempty"` // Guest command executed during template build.
}

// Tenant defines the admission quotas and bearer credential for one owner.
type Tenant struct {
	ID            string `json:"id"`
	Token         string `json:"token,omitempty"` // Sensitive credential; returned when created or replaced.
	MaxSandboxes  int64  `json:"max_sandboxes"`
	MaxStorageMiB int64  `json:"max_storage_mib"` // Durable disk, capture and backing responsibility.
}

// Command is a business intent. Action selects the applicable fields; unused
// fields do not grant authority. Mutations require a stable Idempotency-Key.
type Command struct {
	Action        string            `json:"action"`
	SandboxID     string            `json:"sandbox_id,omitempty"`
	CheckpointID  string            `json:"checkpoint_id,omitempty"`
	TemplateID    string            `json:"template_id,omitempty"`
	Template      *Template         `json:"template,omitempty"`
	TTLSeconds    int64             `json:"ttl_seconds,omitempty"` // Zero selects the default 300-second lease.
	OnExpire      string            `json:"on_expire,omitempty"`   // pause or kill.
	AutoRenew     bool              `json:"auto_renew,omitempty"`  // Renew on connection admission, not continuously on an open stream.
	LeaseDeadline time.Time         `json:"lease_deadline"`        // Frozen expected deadline for expiry intents.
	Env           map[string]EnvVar `json:"env,omitempty"`         // Secret values are never persisted in operation views.
	Policy        Policy            `json:"policy"`
	Count         int               `json:"count,omitempty"` // Fork branches: 1–32; zero selects one.
}

// Sandbox is the tenant-visible state of a logical sandbox across VM generations.
type Sandbox struct {
	ID              string            `json:"id"`
	TenantID        string            `json:"tenant_id"`
	State           string            `json:"state"`
	VMID            string            `json:"vm_id,omitempty"`       // Logical resource identity, distinct from one execution instance.
	SnapshotID      string            `json:"snapshot_id,omitempty"` // Retained sleep snapshot while paused.
	Instance        string            `json:"instance,omitempty"`    // Exact VMM execution identity.
	TemplateID      string            `json:"template_id"`
	ProvisionSource string            `json:"provision_source,omitempty"` // cold, golden or warm.
	Shape           Shape             `json:"shape"`
	Deadline        time.Time         `json:"deadline"`     // Running lease deadline.
	PausedUntil     time.Time         `json:"paused_until"` // Retention deadline while paused.
	TTLSeconds      int64             `json:"ttl_seconds"`
	OnExpire        string            `json:"on_expire"`
	AutoRenew       bool              `json:"auto_renew"`
	Env             map[string]string `json:"-"` // Never included in public JSON.
	Policy          Policy            `json:"policy"`
	RequiresSecrets bool              `json:"requires_secrets"` // Explicit configure must restore host-only values after restart.
	ResumeAttempt   uint64            `json:"resume_attempt,omitempty"`
	Epoch           string            `json:"creation_epoch"` // Birth identity used by capabilities; stable across resume.
	OperationID     string            `json:"operation_id,omitempty"`
	LastError       string            `json:"last_error,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
}

// Checkpoint is an authorized business binding to a canonical resource snapshot.
type Checkpoint struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	SandboxID  string    `json:"sandbox_id"`
	SnapshotID string    `json:"snapshot_id"`
	Shape      Shape     `json:"shape"`
	CreatedAt  time.Time `json:"created_at"`
}

// Result is frozen when an operation commits its terminal delivery. Later
// sandbox configuration, renewal or deletion cannot rewrite this historical view.
type Result struct {
	OperationID string            `json:"operation_id"`
	Sandbox     *Sandbox          `json:"sandbox,omitempty"`
	Sandboxes   []Sandbox         `json:"sandboxes,omitempty"`
	Tokens      map[string]string `json:"tokens,omitempty"` // Sensitive bearer credentials keyed by sandbox ID.
	Checkpoint  *Checkpoint       `json:"checkpoint,omitempty"`
	Template    *Template         `json:"template,omitempty"`
}

// Operation describes an accepted, durable business intent and its frozen result.
// Canceling an HTTP wait does not cancel the accepted operation.
type Operation struct {
	ID             string    `json:"id"`
	Key            string    `json:"key"` // Original idempotency key within the authorization scope.
	TenantID       string    `json:"tenant_id"`
	ChargeTenantID string    `json:"charge_tenant_id,omitempty"` // Owner charged for the resulting resources.
	Command        Command   `json:"command"`                    // Sanitized accepted request.
	IDs            []string  `json:"resource_ids,omitempty"`
	Status         string    `json:"status"` // pending, running, succeeded or failed.
	Step           string    `json:"step"`   // Public progress label, not a resource cleanup proof.
	SnapshotID     string    `json:"snapshot_id,omitempty"`
	OwnedSnapshot  bool      `json:"owned_snapshot,omitempty"`
	ReservedMiB    int64     `json:"reserved_mib,omitempty"`
	SourceInstance string    `json:"source_instance,omitempty"`
	Result         Result    `json:"result"`
	Error          *ApiError `json:"error,omitempty"` // Added by the HTTP adapter; never stored in a publication.
	CreatedAt      time.Time `json:"created_at"`
	FinishedAt     time.Time `json:"finished_at"` // Zero until a terminal state commits.
}
