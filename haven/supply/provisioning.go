package supply

import (
	"context"

	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

// BareTarget admits an undelivered allocation for build/refill. It cannot carry
// activation credentials, reuse a delivered sandbox, or choose an existing slot.
type BareTarget struct {
	NewSandboxID string
	Budget       *quota.SlotBudget
	Parent       machine.ResourceParent
}

// ProvisionRequest carries the frozen caller request and its preflight materials.
// The coordinator derives the request hash from Frozen; it is never supplied
// independently. Restore is present only for a clone allocation.
type ProvisionRequest struct {
	TenantID string
	Key      string
	Frozen   []byte
	Template TemplateRecord
	Image    host.ImageManifest
	Restore  *machine.SnapshotSource
}

// BareProvisioner retains the shared admission transaction and VM guard domain.
// Supply owns its template parent; the resource coordinator owns child execution.
type BareProvisioner interface {
	AdmitBare(context.Context, ProvisionRequest, BareTarget) (operation.Record, error)
	Continue(string) error
}
