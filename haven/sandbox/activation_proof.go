package sandbox

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
)

// Activation verifies historical resource custody and the current configuration
// fence in the caller's transaction before any activation progress is published.
func VerifyActivation(proofs machine.ResourceProofs, ctx context.Context, tx *sql.Tx, step ActivationStep) (machine.ResourceStep, ownership.Sandbox, error) {
	var record ownership.Sandbox
	source, err := machine.LoadResourceStep(ctx, tx, step.Input.ResourceOperation)
	if err != nil {
		return source, record, err
	}
	if source.Hash != step.Input.ResourceHash || source.Input.TenantID != step.Input.TenantID || source.Input.SandboxID != step.Input.SandboxID || source.Status != "succeeded" || source.State.Witness != step.Input.ExpectedInstance {
		return source, record, fmt.Errorf("activation original resource witness changed")
	}
	retiring, err := dbgen.New(tx).CountPendingCleanupResource(ctx, dbgen.CountPendingCleanupResourceParams{
		ResourceKind: "direct_policy",
		ResourceID:   source.Candidate,
	})
	if err != nil {
		return source, record, err
	}
	if retiring != 0 {
		return source, record, errs.ErrInterrupted
	}
	if err = proofs.VerifyGeneration(ctx, tx, source); err != nil {
		return source, record, err
	}
	record, err = LoadSandbox(ctx, tx, step.Input.SandboxID, step.Input.TenantID)
	if err != nil {
		return source, record, err
	}
	if record.SourceOperation != source.OperationID || record.VMID != source.Candidate || record.Instance != source.State.Witness || (record.State != "provisioning" && record.State != "running") {
		return source, record, fmt.Errorf("activation sandbox ownership changed")
	}
	resource, current, owner, err := machine.LoadVM(ctx, tx, source)
	if err != nil {
		return source, record, err
	}
	if owner != "" || current != source.State.Witness || resource.Deleting || !resource.Verified || !resource.AgentReady {
		return source, record, fmt.Errorf("activation current VM is not ready")
	}
	fence, err := LifecycleFence(ctx, tx, record.ID)
	if err != nil {
		return source, record, err
	}
	if fence != step.OperationID {
		return source, record, fmt.Errorf("%w: activation configuration was superseded", errs.ErrConflict)
	}
	settings, err := LoadSettings(ctx, tx, record.ID)
	if err != nil {
		return source, record, err
	}
	expectedSettings := step.Input.Settings
	if step.Input.PreviousSettings != nil && step.Phase == "admitted" {
		expectedSettings = *step.Input.PreviousSettings
	}
	if !reflect.DeepEqual(settings, expectedSettings) || step.Input.Settings.Generation != step.OperationID {
		return source, record, fmt.Errorf("activation desired settings changed")
	}
	identity, err := operation.New(tx).RequestIdentity(ctx, step.OperationID)
	if err != nil {
		return source, record, err
	}
	if operation.Terminal(identity.Status) || identity.ScopeKind != "tenant" || identity.ScopeID != step.Input.RequestTenantID || operation.Digest(identity.Request) != identity.Hash {
		return source, record, fmt.Errorf("activation parent identity changed")
	}
	candidate, expected, err := operation.New(tx).StepTarget(ctx, step.OperationID, step.Key, step.Hash)
	if err != nil {
		return source, record, err
	}
	if candidate != source.Candidate || expected != source.State.Witness {
		return source, record, fmt.Errorf("activation candidate differs from resource")
	}
	return source, record, nil
}
