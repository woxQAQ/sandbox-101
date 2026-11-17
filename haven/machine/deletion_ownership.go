package machine

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

type ResumeDestinationProof struct {
	OperationID string
	TenantID    string
	SandboxID   string
}

// DeletionOwnershipReader checks business ownership in the caller's snapshot.
// Its methods never write or obtain a separate workflow lock.
type DeletionOwnershipReader interface {
	SandboxOwnershipReader
	CurrentSlotBudget(context.Context, store.DBTX, ownership.Sandbox, ResourceStep) (quota.SlotBudget, error)
	VerifyReleaseDestination(context.Context, *sql.Tx, ResourceStep) error
	ResumeDestination(context.Context, *sql.Tx, *QuotaDestination) (ResumeDestinationProof, error)
}

// DeletionParticipant joins the physical deletion transaction. It neither
// commits nor checkpoints, dispatches effects, or obtains workflow locks.
type DeletionParticipant interface {
	TerminateSandbox(context.Context, *sql.Tx, ownership.Sandbox, string, string) error
	CompleteRelease(context.Context, *sql.Tx, ResourceStep) error
	RestoreResumeSlot(context.Context, *sql.Tx, *QuotaDestination, string) error
	RestorePausedSandbox(context.Context, *sql.Tx, *QuotaDestination) error
}

// VMUsageReader supplies the allocation owner's retained VM usage count.
type VMUsageReader interface {
	VMUsers(context.Context, store.DBTX, string) (int64, error)
}

// ReleaseParentAuthority optionally selects a frozen slot destination before
// admission. The boolean distinguishes selection from leaving the source intact.
type ReleaseParentAuthority interface {
	ReleaseDestination(context.Context, *sql.Tx, ownership.Sandbox, ResourceStep) (*QuotaDestination, bool, error)
}
