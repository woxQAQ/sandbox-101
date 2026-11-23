// Package loom performs explicit host resource operations and verifies their
// effects. Durable intent, publication and recovery belong to the caller.
package loom

import (
	"context"
	"maps"
)

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

// Equal compares the image, shape, and labels. Nil and empty label maps are equivalent.
func (c Config) Equal(other Config) bool {
	return c.Image == other.Image && c.Shape == other.Shape && maps.Equal(c.Labels, other.Labels)
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

// SnapshotCompatibility identifies the host format used to capture and restore VM state.
type SnapshotCompatibility interface {
	// Compatibility returns the host compatibility identifier recorded with each snapshot.
	Compatibility() string
}

// Backend is the explicit host-effects boundary. Implementations must make
// effects inspectable under Resource.Instance and reject uncertain identities.
// Success verifies only the effect; it does not commit a control-plane step.
type Backend interface {
	SnapshotCompatibility
	// ValidateHostConfig validates the requested VM configuration against this host.
	ValidateHostConfig(ctx context.Context, config Config) error
	// Prepare prepares the candidate's private execution files before launch.
	Prepare(ctx context.Context, resource Resource) error
	// Inspect observes the identified instance and reports uncertainty as an error.
	Inspect(ctx context.Context, resource Resource) (Observation, error)
	// Launch starts the VMM process for the candidate generation.
	Launch(ctx context.Context, resource Resource, restore bool) error
	// Boot boots the prepared VM or resumes its restored state.
	Boot(ctx context.Context, resource Resource, restore bool) error
	// Agent performs System portal initialization for the booted instance.
	Agent(ctx context.Context, resource Resource, restore bool) error
	// Stop proves this exact process before signaling; sibling inventory is
	// verified separately by Inspect before resource removal.
	Stop(ctx context.Context, resource Resource) error
	// Pause suspends guest execution before capture.
	Pause(ctx context.Context, resource Resource) error
	// Capture writes the paused VM state to the supplied directory.
	Capture(ctx context.Context, resource Resource, directory string) error
	// Resume continues guest execution after a pause.
	Resume(ctx context.Context, resource Resource) error
	// SetNetworkEnabled sets the host network link state for the identified instance.
	SetNetworkEnabled(ctx context.Context, resource Resource, enabled bool) error
	// Remove removes the stopped instance's runtime resources.
	Remove(ctx context.Context, resource Resource) error
}

// NetworkPolicyBackend installs and verifies instance-bound kernel rules.
// Installation requires all interfaces down; nil revokes direct permission.
type NetworkPolicyBackend interface {
	// InstallNetworkPolicy installs instance-bound rules with links down. A nil policy revokes permission.
	InstallNetworkPolicy(ctx context.Context, resource Resource, policy *NetworkPolicy) error
	// VerifyNetworkPolicy checks the live rules against the expected instance policy.
	VerifyNetworkPolicy(ctx context.Context, resource Resource, policy *NetworkPolicy) error
}
