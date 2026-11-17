package machine

import (
	"context"

	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/store"
)

// StopParentAuthority exposes only the frozen lifecycle identity and whether its
// command permits stopping an activated sandbox. Admission still verifies parent
// intent and the child link in the same transaction.
type StopParentAuthority interface {
	StopLifecycle() (operationID string, allowRunning bool)
}

// StopOwnershipReader validates the original lifecycle, stop link and sandbox
// fence before returning the expected frozen business state. It never writes.
type StopOwnershipReader interface {
	SandboxOwnershipReader
	VerifyStopParent(context.Context, store.DBTX, StopStep, ResourceStep, ownership.Sandbox) (string, error)
}
