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

// ProvisionRequest carries the accepted parameters and their preflight materials.
// Restore identifies the retained source of a clone allocation.
type ProvisionRequest struct {
	TenantID string
	Key      string
	Frozen   operation.Request
	Template TemplateRecord
	Image    host.ImageManifest
	Restore  *machine.SnapshotSource
}

// BareProvisioner retains the shared admission transaction and VM guard domain.
// Supply owns its template parent; the resource coordinator owns child execution.
type BareProvisioner interface {
	// AdmitBare admits the provision request for the parent's supply target.
	AdmitBare(ctx context.Context, request ProvisionRequest, target BareTarget) (operation.State, error)
	// Continue advances an already admitted provision operation.
	Continue(operationID string) error
}
