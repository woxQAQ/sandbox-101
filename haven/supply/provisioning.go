package supply

import (
	"context"

	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

// RawTarget admits an undelivered allocation for build/refill. It cannot carry
// activation credentials, reuse a delivered sandbox, or choose an existing slot.
type RawTarget struct {
	NewSandboxID string
	Budget       *quota.SlotBudget
	Parent       machine.ResourceParent
}

// RawProvisioner retains the shared admission transaction and VM guard domain.
// Supply owns its template parent; the resource coordinator owns child execution.
type RawProvisioner interface {
	AdmitRaw(context.Context, string, string, []byte, string, TemplateRecord, host.ImageManifest, *machine.Source, RawTarget) (operation.Record, error)
	Continue(string) error
}
