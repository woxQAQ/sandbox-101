package machine

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
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

// VMGuards borrows the shared admission and per-VM lock domains.
type VMGuards interface {
	Admission() execution.Guard
	VM(string) execution.Guard
}

// ActivationProof carries only the original step identity and resource linkage.
// Settings and business transition decisions remain with sandbox.
type ActivationProof struct {
	OperationID       string
	Key               string
	Hash              string
	Status            string
	Phase             string
	ResourceOperation string
	ResourceHash      string
	ExpectedInstance  string
}

// ActivationProofReader validates the complete frozen activation before exposing
// its resource linkage. The borrowed DBTX is not statically read-only: readers
// must not write, commit, checkpoint, dispatch work, or capture a running App.
type ActivationProofReader interface {
	ReadActivationProof(context.Context, store.DBTX, string) (ActivationProof, error)
}

func (proofs ResourceProofs) Activation(ctx context.Context, query store.DBTX, id string) (ActivationProof, error) {
	if proofs.activations == nil {
		return ActivationProof{}, fmt.Errorf("%w: activation history reader is required", errs.ErrManualRecovery)
	}
	return proofs.activations.ReadActivationProof(ctx, query, id)
}

// SnapshotUsageReader reads supply's retained holders in the same snapshot as
// deletion proof. The two reads remain separate to retain failure ordering.
type SnapshotUsageReader interface {
	TemplateSnapshotUsers(context.Context, store.DBTX, string) (int64, error)
	PoolSnapshotUsers(context.Context, store.DBTX, string) (int64, error)
}

// ProvisionOwnershipReader checks the retained resource and sandbox intent in
// the caller's transaction. It never changes business or physical state.
type ProvisionOwnershipReader interface {
	VerifyProvisionIntent(ResourceProofs, context.Context, *sql.Tx, ResourceStep) (VMRecord, ownership.Sandbox, operation.Result, error)
}

// ProvisionParticipant joins physical progress without committing, checkpointing,
// dispatching effects, or obtaining a workflow lock.
type ProvisionParticipant interface {
	PublishInstance(context.Context, *sql.Tx, ownership.Sandbox, string) error
	BlockActivation(context.Context, *sql.Tx, string) error
}

type ProvisionBackend interface {
	StopBackend

	Check(context.Context, loom.Config) error
	Prepare(context.Context, loom.Resource) error
	Launch(context.Context, loom.Resource, bool) error
	Boot(context.Context, loom.Resource, bool) error
	Agent(context.Context, loom.Resource, bool) error
}

// StopParentAuthority exposes only the frozen lifecycle identity and whether its
// command permits stopping an activated sandbox. Admission still verifies parent
// intent and the child link in the same transaction.
type StopParentAuthority interface {
	StopLifecycle() (operationID string, allowRunning bool)
}

// StopParentProof compares the stop intent, immutable resource birth and current
// sandbox ownership in the caller's original transaction.
type StopParentProof struct {
	Step    StopStep
	Source  ResourceStep
	Sandbox ownership.Sandbox
}

// StopOwnershipReader validates the original lifecycle, stop link and sandbox
// fence before returning the expected frozen business state. It never writes.
type StopOwnershipReader interface {
	SandboxOwnershipReader
	VerifyStopParent(context.Context, store.DBTX, StopParentProof) (string, error)
}

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

// SandboxTermination keeps the authenticated tenant predicate separate from the
// serialized sandbox body while fixing the operation that takes termination.
type SandboxTermination struct {
	Sandbox     ownership.Sandbox
	OperationID string
	TenantID    string
}

// DeletionParticipant joins the physical deletion transaction. It neither
// commits nor checkpoints, dispatches effects, or obtains workflow locks.
type DeletionParticipant interface {
	TerminateSandbox(context.Context, *sql.Tx, SandboxTermination) error
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

// LifecycleParentAuthority exposes the original parent identity, without moving
// the business lifecycle input or its state transitions into machine.
type LifecycleParentAuthority interface {
	LifecycleOperation() string
}

// CaptureDiscardOwnershipReader verifies the original release, capture list,
// stop link and fence in the same snapshot used by physical retirement proofs.
type CaptureDiscardOwnershipReader interface {
	SandboxOwnershipReader
	VerifyCaptureDiscardParent(context.Context, store.DBTX, CaptureDiscardStep, ownership.Sandbox) (bool, error)
	VerifyCaptureDiscardLink(context.Context, store.DBTX, CaptureDiscardStep) error
}
