// Package loom performs explicit host resource operations and verifies their
// effects. Durable intent, publication and recovery belong to the caller.
package loom

import "context"

type Shape struct {
	VCPUs     int   `json:"vcpus"`
	MemoryMiB int64 `json:"memory_mib"`
	DiskMiB   int64 `json:"disk_mib"`
}

type Config struct {
	Image  string            `json:"image"`
	Shape  Shape             `json:"shape"`
	Labels map[string]string `json:"labels,omitempty"`
}

// NetworkPolicy describes the versioned IPv4 rules for one explicit instance.
// Empty CIDRs deny all; hostname rules are unsupported. Connection limits have
// no implicit default and apply to tracked TCP, UDP and ICMP flows.
type NetworkPolicy struct {
	Version        int      `json:"version"`
	AllowedCIDRs   []string `json:"allowed_cidrs"`
	MaxConnections int      `json:"max_connections"`
	AllowedHosts   []string `json:"allowed_hosts,omitempty"`
}

// Resource fixes the object and paths allocated by the control plane before
// invoking an effect. It carries no catalog, store or publication authority.
type Resource struct {
	VMID         string `json:"vm_id"`
	Instance     string `json:"instance,omitempty"`
	Dir          string `json:"dir"`
	Disk         string `json:"disk"`
	RunDir       string `json:"run_dir,omitempty"`
	RestoreDir   string `json:"restore_dir,omitempty"`
	ImageDir     string `json:"image_dir"`
	Config       Config `json:"config"`
	NetworkToken string `json:"network_token"`
	Slot         int    `json:"slot"`
}

type Observation struct {
	Alive      bool
	Paused     bool
	Booted     bool
	Configured bool
	PID        int
	StartTime  uint64
	Network    bool
}

// Backend is the explicit host-effects boundary. Implementations must make
// effects inspectable under Resource.Instance and reject uncertain identities.
// Success verifies only the effect; it does not commit a control-plane step.
type Backend interface {
	Compatibility() string
	Check(context.Context, Config) error
	Prepare(context.Context, Resource) error
	Inspect(context.Context, Resource) (Observation, error)
	Launch(context.Context, Resource, bool) error
	Boot(context.Context, Resource, bool) error
	Agent(context.Context, Resource, bool) error
	// Stop proves this exact process before signaling; sibling inventory is
	// verified separately by Inspect before resource removal.
	Stop(context.Context, Resource) error
	Pause(context.Context, Resource) error
	Capture(context.Context, Resource, string) error
	Resume(context.Context, Resource) error
	Network(context.Context, Resource, bool) error
	Remove(context.Context, Resource) error
}

// NetworkPolicyBackend installs and verifies instance-bound kernel rules.
// Installation requires all interfaces down; nil revokes direct permission.
type NetworkPolicyBackend interface {
	InstallNetworkPolicy(context.Context, Resource, *NetworkPolicy) error
	VerifyNetworkPolicy(context.Context, Resource, *NetworkPolicy) error
}
