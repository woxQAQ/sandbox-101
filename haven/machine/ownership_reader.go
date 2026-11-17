package machine

import (
	"context"

	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/store"
)

// SandboxOwnershipReader reads the current canonical binding in the caller's
// snapshot. Consumers must also verify the original resource or capture proof.
// Implementations perform no writes, commits or runtime effects.
type SandboxOwnershipReader interface {
	ReadSandbox(context.Context, store.DBTX, string, string) (ownership.Sandbox, error)
}

// SupplyCleanupAuthority identifies the original collector participating in
// admission. Typed nil parents must return false rather than an empty identity.
type SupplyCleanupAuthority interface {
	SupplyCleanupOperation() (string, bool)
}
