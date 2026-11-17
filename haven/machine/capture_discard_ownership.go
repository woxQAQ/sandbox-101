package machine

import (
	"context"

	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/store"
)

// LifecycleParentAuthority exposes the original parent identity, without moving
// the business lifecycle input or its state transitions into machine.
type LifecycleParentAuthority interface {
	LifecycleOperation() string
}

// CaptureDiscardOwnershipReader verifies the original release, capture list,
// stop link and fence in the same snapshot used by physical retirement proofs.
type CaptureDiscardOwnershipReader interface {
	SandboxOwnershipReader
	VerifyCaptureDiscardParent(context.Context, store.DBTX, CaptureDiscardStep, ownership.Sandbox, bool) (bool, error)
}
