// Package engine implements the single-host durable VM coordinator.
package engine

import (
	"context"
	"encoding/json"

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

type Manifest struct {
	Format          int               `json:"format"`
	Backend         string            `json:"backend"`
	Architecture    string            `json:"architecture"`
	CPU             string            `json:"cpu"`
	Config          api.Config        `json:"config"`
	MemoryMode      string            `json:"memory_mode"`
	PublicationMode string            `json:"publication_mode"`
	Files           map[string]string `json:"files"`
	Image           json.RawMessage   `json:"image"`
}

type vmRecord struct {
	VM       api.VM   `json:"vm"`
	Resource Resource `json:"resource"`
}
type snapshotRecord struct {
	Snapshot api.Snapshot `json:"snapshot"`
	Dir      string       `json:"dir"`
}
type journal struct {
	CaptureBoundary *captureBoundary  `json:"capture_boundary,omitempty"`
	Materialized    bool              `json:"materialized,omitempty"`
	CaptureFiles    map[string]string `json:"capture_files,omitempty"`
	ArtifactsSynced bool              `json:"artifacts_synced,omitempty"`
	ResumeRequired  bool              `json:"resume_required,omitempty"`
	Captured        bool              `json:"captured,omitempty"`
	Resource        *Resource         `json:"resource,omitempty"`
	Old             *Resource         `json:"old,omitempty"`
	Lease           bool              `json:"lease,omitempty"`
	Manifest        *Manifest         `json:"manifest,omitempty"`
}
type opRecord struct {
	Operation api.Operation `json:"operation"`
	Journal   journal       `json:"journal"`
}
