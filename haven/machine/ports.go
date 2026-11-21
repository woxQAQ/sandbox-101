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
	// ReadSandbox loads the current sandbox binding for this owner in the caller's snapshot.
	ReadSandbox(ctx context.Context, db store.DBTX, sandboxID, tenantID string) (ownership.Sandbox, error)
}

// SupplyCleanupAuthority identifies the original collector participating in
// admission. Typed nil parents must return false rather than an empty identity.
type SupplyCleanupAuthority interface {
	// SupplyCleanupOperation returns the cleanup operation identity when the parent supplies cleanup authority.
	SupplyCleanupOperation() (string, bool)
}

// VMGuards borrows the shared admission and per-VM lock domains.
type VMGuards interface {
	// ForStoreMutation returns the node-wide lock for durable admission, state publication and final store close.
	ForStoreMutation() execution.Guard
	// ForVMLifecycle returns the lock for host effects and completion checks on one logical VM.
	ForVMLifecycle(vmID string) execution.Guard
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
	// ReadActivationProof loads the frozen activation and verifies its resource linkage.
	ReadActivationProof(ctx context.Context, db store.DBTX, operationID string) (ActivationProof, error)
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
	// TemplateSnapshotUsers counts templates retaining the snapshot in the caller's database scope.
	TemplateSnapshotUsers(ctx context.Context, db store.DBTX, snapshotID string) (int64, error)
	// PoolSnapshotUsers counts pool entries retaining the snapshot in the caller's database scope.
	PoolSnapshotUsers(ctx context.Context, db store.DBTX, snapshotID string) (int64, error)
}

// ProvisionOwnershipReader checks the retained resource and sandbox intent in
// the caller's transaction. It never changes business or physical state.
type ProvisionOwnershipReader interface {
	// VerifyProvisionIntent checks the original provision request and current ownership before advancing the step.
	VerifyProvisionIntent(proofs ResourceProofs, ctx context.Context, tx *sql.Tx, step ResourceStep) (VMRecord, ownership.Sandbox, operation.Result, error)
}

// ProvisionParticipant joins physical progress without committing, checkpointing,
// dispatching effects, or obtaining a workflow lock.
type ProvisionParticipant interface {
	// PublishInstance records the provisioned instance on its sandbox in the resource transaction.
	PublishInstance(ctx context.Context, tx *sql.Tx, sandbox ownership.Sandbox, instance string) error
	// BlockActivation marks a pending activation blocked when provisioning fails.
	BlockActivation(ctx context.Context, tx *sql.Tx, operationID string) error
}

type ProvisionBackend interface {
	StopBackend

	// ValidateHostConfig validates the requested VM configuration against this host.
	ValidateHostConfig(ctx context.Context, config loom.Config) error
	// Prepare prepares the candidate's private execution files before launch.
	Prepare(ctx context.Context, resource loom.Resource) error
	// Launch starts the VMM process for the candidate generation.
	Launch(ctx context.Context, resource loom.Resource, restore bool) error
	// Boot boots the prepared VM or resumes its restored state.
	Boot(ctx context.Context, resource loom.Resource, restore bool) error
	// Agent performs System portal initialization for the booted instance.
	Agent(ctx context.Context, resource loom.Resource, restore bool) error
}

// StopParentAuthority exposes only the frozen lifecycle identity and whether its
// command permits stopping an activated sandbox. Admission still verifies parent
// intent and the child link in the same transaction.
type StopParentAuthority interface {
	// StopLifecycle identifies the parent lifecycle and whether its action authorizes stopping a running sandbox.
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
	// VerifyStopParent checks the original stop link and fence and returns the parent's frozen sandbox state.
	VerifyStopParent(ctx context.Context, db store.DBTX, proof StopParentProof) (string, error)
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
	// CurrentSlotBudget resolves the current slot responsibility for this provision source.
	CurrentSlotBudget(ctx context.Context, db store.DBTX, sandbox ownership.Sandbox, source ResourceStep) (quota.SlotBudget, error)
	// VerifyReleaseDestination checks the destination that assumes slot responsibility on release.
	VerifyReleaseDestination(ctx context.Context, tx *sql.Tx, source ResourceStep) error
	// ResumeDestination verifies the resume operation and owner named by the slot destination.
	ResumeDestination(ctx context.Context, tx *sql.Tx, destination *QuotaDestination) (ResumeDestinationProof, error)
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
	// TerminateSandbox records termination under the tenant and operation predicates.
	TerminateSandbox(ctx context.Context, tx *sql.Tx, termination SandboxTermination) error
	// CompleteRelease finishes the business release in the physical retirement transaction.
	CompleteRelease(ctx context.Context, tx *sql.Tx, source ResourceStep) error
	// RestoreResumeSlot returns the slot to its resume destination under the recorded holder.
	RestoreResumeSlot(ctx context.Context, tx *sql.Tx, destination *QuotaDestination, holderID string) error
	// RestorePausedSandbox restores the paused sandbox binding after an unsuccessful resume.
	RestorePausedSandbox(ctx context.Context, tx *sql.Tx, destination *QuotaDestination) error
}

// VMUsageReader supplies the allocation owner's retained VM usage count.
type VMUsageReader interface {
	// VMUsers counts supply references retaining the VM.
	VMUsers(ctx context.Context, db store.DBTX, vmID string) (int64, error)
}

// ReleaseParentAuthority optionally selects a frozen slot destination before
// admission. The boolean distinguishes selection from leaving the source intact.
type ReleaseParentAuthority interface {
	// ReleaseDestination selects the slot handoff authorized by the parent and reports whether one was selected.
	ReleaseDestination(ctx context.Context, tx *sql.Tx, sandbox ownership.Sandbox, source ResourceStep) (*QuotaDestination, bool, error)
}

// LifecycleParentAuthority exposes the original parent identity, without moving
// the business lifecycle input or its state transitions into machine.
type LifecycleParentAuthority interface {
	// LifecycleOperation returns the original lifecycle operation that owns the child intent.
	LifecycleOperation() string
}

// CaptureDiscardOwnershipReader verifies the original release, capture list,
// stop link and fence in the same snapshot used by physical retirement proofs.
type CaptureDiscardOwnershipReader interface {
	SandboxOwnershipReader
	// VerifyCaptureDiscardParent checks release authority and reports whether the sandbox remains in the frozen state.
	VerifyCaptureDiscardParent(ctx context.Context, db store.DBTX, step CaptureDiscardStep, sandbox ownership.Sandbox) (bool, error)
	// VerifyCaptureDiscardLink verifies the committed parent link to this failed-capture discard.
	VerifyCaptureDiscardLink(ctx context.Context, db store.DBTX, step CaptureDiscardStep) error
}
