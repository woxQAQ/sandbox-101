package application

import (
	"context"
	"os"

	"github.com/woxQAQ/haven/egress"
)

// Egress is the portal lifecycle supplied by application construction.
type Egress interface {
	// Bind activates the egress policy and in-memory credentials for the given target.
	Bind(ctx context.Context, target egress.BindingTarget, policy egress.Policy, secrets map[string]egress.Secret) error
	// Revoke retires the sandbox's active egress binding.
	Revoke(sandboxID string) error
	// Close shuts down active portals and releases their host resources.
	Close() error
}

// EgressCallbacks supplies only the host capabilities required by portal setup.
// Each callback is bound to its owning service before startup reconciliation.
type EgressCallbacks struct {
	Audit              func(context.Context, any) error
	PrepareProxySocket func(context.Context, egress.Binding, string, os.FileInfo) error
	RecoverStaleSocket func(egress.Binding, string, os.FileInfo) bool
	InstallDirect      func(context.Context, egress.Binding, egress.Policy) (func() error, error)
}
