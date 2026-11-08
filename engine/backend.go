// Package engine implements the single-host durable VM coordinator.
package engine

import (
	"context"

	"github.com/woxQAQ/sandbox-engine/api"
)

// Backend performs host effects. Implementations must make effects repeatable or
// inspectable under Resource.Instance; an uncertain identity must return an error.
// The daemon never uses a simulation backend implicitly.
type Backend interface {
	Compatibility() string
	Check(context.Context, api.Config) error
	Prepare(context.Context, Resource) error
	Inspect(context.Context, Resource) (Observation, error)
	Launch(context.Context, Resource, bool) error
	Boot(context.Context, Resource, bool) error
	Agent(context.Context, Resource, bool) error
	// Stop proves the supplied instance's identity before signaling it. It must
	// not require sibling instances to be absent; Inspect proves that inventory.
	Stop(context.Context, Resource) error
	Pause(context.Context, Resource) error
	Capture(context.Context, Resource, string) error
	Resume(context.Context, Resource) error
	Network(context.Context, Resource, bool) error
	Remove(context.Context, Resource) error
}

// NetworkPolicyBackend is required for direct activation. Install must require
// every owned interface down, atomically replace the namespace policy, and save
// an instance/namespace-bound receipt. Verify compares that receipt and actual
// kernel rules with the supplied persisted policy. Nil installs/verifies deny-all.
// Both methods execute under the engine's VM lifecycle lock, never a global lock.
type NetworkPolicyBackend interface {
	InstallNetworkPolicy(context.Context, Resource, *api.NetworkPolicy) error
	VerifyNetworkPolicy(context.Context, Resource, *api.NetworkPolicy) error
}

type Resource struct {
	VMID         string     `json:"vm_id"`
	Instance     string     `json:"instance,omitempty"`
	Dir          string     `json:"dir"`
	Disk         string     `json:"disk"`
	RunDir       string     `json:"run_dir,omitempty"`
	RestoreDir   string     `json:"restore_dir,omitempty"`
	ImageDir     string     `json:"image_dir"`
	Config       api.Config `json:"config"`
	NetworkToken string     `json:"network_token"`
	Slot         int        `json:"slot"`
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
