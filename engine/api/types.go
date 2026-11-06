// Package api defines the engine's durable operation and resource contract.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Code string

const (
	NotFound       Code = "not_found"
	Conflict       Code = "conflict"
	Invalid        Code = "invalid"
	Exhausted      Code = "resource_exhausted"
	Incompatible   Code = "incompatible"
	Unknown        Code = "result_unknown"
	ManualRecovery Code = "manual_recovery_required"
	Internal       Code = "internal"
)

type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func Fail(
	c Code,
	format string,
	args ...any,
) error {
	return &Error{c, fmt.Sprintf(format, args...)}
}

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

// NetworkPolicy is the versioned IPv4 direct-egress contract. Version must be 1.
// AllowedCIDRs is an explicit allowlist (empty denies all). Prefixes must use
// canonical IPv4 CIDR notation; reserved/private/metadata destinations remain
// denied even inside a broader allowed prefix. No private-address exemptions are
// supported. Only TCP, UDP and ICMP egress and their tracked replies are allowed.
// MaxConnections must be 1..65535 and limits tracked flows per VM, including UDP
// and ICMP flows until conntrack expiry. AllowedHosts must be empty: direct mode
// cannot enforce DNS names, TLS identity, HTTP rules or credential injection.
// Lists retain their order in durable request identity; retries must use the
// same parameters. Zero MaxConnections has no implicit default.
type NetworkPolicy struct {
	Version        int      `json:"version"`
	AllowedCIDRs   []string `json:"allowed_cidrs"`
	MaxConnections int      `json:"max_connections"`
	AllowedHosts   []string `json:"allowed_hosts,omitempty"`
}

type Request struct {
	Key              string  `json:"key"`
	Action           string  `json:"action"`
	VMID             string  `json:"vm_id,omitempty"`
	SnapshotID       string  `json:"snapshot_id,omitempty"`
	ExpectedInstance string  `json:"expected_instance,omitempty"`
	Config           *Config `json:"config,omitempty"`
	NetworkEnabled   bool    `json:"network_enabled,omitempty"`
	// Policy is used only by network_policy. Nil revokes direct permission.
	// Both network_policy and network require a nonempty ExpectedInstance.
	Policy *NetworkPolicy `json:"policy,omitempty"`
}
type Operation struct {
	ID         string  `json:"id"`
	Key        string  `json:"key"`
	Request    Request `json:"request"`
	Status     string  `json:"status"` // pending, running, succeeded, failed
	Step       string  `json:"step,omitempty"`
	VMID       string  `json:"vm_id,omitempty"`
	SnapshotID string  `json:"snapshot_id,omitempty"`
	// ResultInstance fixes the candidate delivered by this successful start,
	// restore or clone. Historical terminal receipts are never enriched.
	ResultInstance string    `json:"result_instance,omitempty"`
	Error          *Error    `json:"error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
type VM struct {
	ID             string `json:"id"`
	Config         Config `json:"config"`
	Phase          string `json:"phase"`
	Instance       string `json:"instance,omitempty"`
	PID            int    `json:"pid,omitempty"`
	StartTime      uint64 `json:"start_time,omitempty"`
	AgentReady     bool   `json:"agent_ready"`
	NetworkEnabled bool   `json:"network_enabled"`
	// NetworkActual conservatively reports any owned interface up, including a
	// partially enabled path. Verified=false means current state is uncertain.
	NetworkActual bool `json:"network_actual"`
	// These record the last committed policy, not proof of current kernel state.
	// Activation and reconciliation independently verify the installed rules.
	NetworkPolicy         *NetworkPolicy `json:"network_policy,omitempty"`
	NetworkPolicyInstance string         `json:"network_policy_instance,omitempty"`
	Verified              bool           `json:"verified"`
	BlockedReason         string         `json:"blocked_reason,omitempty"`
	VSockPath             string         `json:"vsock_path,omitempty"`
	OperationID           string         `json:"operation_id,omitempty"`
	Deleting              bool           `json:"deleting"`
	CreatedAt             time.Time      `json:"created_at"`
}
type Snapshot struct {
	ID         string          `json:"id"`
	SourceVMID string          `json:"source_vm_id"`
	Config     Config          `json:"config"`
	Published  bool            `json:"published"`
	Deleting   bool            `json:"deleting"`
	CreatedAt  time.Time       `json:"created_at"`
	Manifest   json.RawMessage `json:"manifest,omitempty"`
}
type Client interface {
	Submit(context.Context, Request) (Operation, error)
	GetOperation(context.Context, string) (Operation, error)
	FindOperation(context.Context, string) (Operation, error)
	GetVM(context.Context, string) (VM, error)
	ListVMs(context.Context) ([]VM, error)
	GetSnapshot(context.Context, string) (Snapshot, error)
	ListSnapshots(context.Context) ([]Snapshot, error)
}

func Wait(ctx context.Context, c Client, r Request) (Operation, error) {
	op, err := c.Submit(ctx, r)
	if err != nil {
		return op, err
	}
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		switch op.Status {
		case "succeeded":
			return op, nil
		case "failed":
			if op.Error != nil {
				return op, op.Error
			}
			return op, Fail(Internal, "operation failed")
		}
		select {
		case <-ctx.Done():
			return op, ctx.Err()
		case <-t.C:
		}
		op, err = c.GetOperation(ctx, op.ID)
		if err != nil {
			return op, err
		}
	}
}
