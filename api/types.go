// Package api defines the public Haven wire contracts without host execution dependencies.
package api

import "time"

type Shape struct {
	VCPUs     int   `json:"vcpus"`
	MemoryMiB int64 `json:"memory_mib"`
	DiskMiB   int64 `json:"disk_mib"`
}
type Policy struct {
	Mode           string   `json:"mode"`
	AllowedHosts   []string `json:"allowed_hosts,omitempty"`
	AllowedCIDRs   []string `json:"allowed_cidrs,omitempty"`
	MaxConnections int      `json:"max_connections,omitempty"`
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
type Template struct {
	ID         string `json:"id"`
	TenantID   string `json:"tenant_id"`
	Image      string `json:"image"`
	Shape      Shape  `json:"shape"`
	SnapshotID string `json:"snapshot_id,omitempty"`
	State      string `json:"state"`
	TargetWarm int    `json:"target_warm"`
	Warmup     string `json:"warmup,omitempty"`
}
type Tenant struct {
	ID            string `json:"id"`
	Token         string `json:"token,omitempty"`
	MaxSandboxes  int64  `json:"max_sandboxes"`
	MaxStorageMiB int64  `json:"max_storage_mib"`
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
	Policy        Policy            `json:"policy"`
	Count         int               `json:"count,omitempty"`
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
	Shape           Shape             `json:"shape"`
	Deadline        time.Time         `json:"deadline"`
	PausedUntil     time.Time         `json:"paused_until"`
	TTLSeconds      int64             `json:"ttl_seconds"`
	OnExpire        string            `json:"on_expire"`
	AutoRenew       bool              `json:"auto_renew"`
	Env             map[string]string `json:"-"`
	Policy          Policy            `json:"policy"`
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
	Shape      Shape     `json:"shape"`
	CreatedAt  time.Time `json:"created_at"`
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
	ID             string    `json:"id"`
	Key            string    `json:"key"`
	TenantID       string    `json:"tenant_id"`
	ChargeTenantID string    `json:"charge_tenant_id,omitempty"`
	Command        Command   `json:"command"`
	IDs            []string  `json:"resource_ids,omitempty"`
	Status         string    `json:"status"`
	Step           string    `json:"step"`
	SnapshotID     string    `json:"snapshot_id,omitempty"`
	OwnedSnapshot  bool      `json:"owned_snapshot,omitempty"`
	ReservedMiB    int64     `json:"reserved_mib,omitempty"`
	SourceInstance string    `json:"source_instance,omitempty"`
	Result         Result    `json:"result"`
	Error          *Error    `json:"error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	FinishedAt     time.Time `json:"finished_at"`
}
