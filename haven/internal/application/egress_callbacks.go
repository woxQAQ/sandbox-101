package application

import (
	"context"
	"os"

	"github.com/woxQAQ/haven/egress"
)

// Egress is the portal lifecycle supplied by application construction.
type Egress interface {
	Bind(context.Context, egress.BindingTarget, egress.Policy, map[string]egress.Secret) error
	Revoke(string) error
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
