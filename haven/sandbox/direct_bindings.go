package sandbox

import (
	"context"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// DirectBindings checks sandbox configuration and frozen activation authority
// before binding a caller to the machine's original network policy.
type DirectBindings struct {
	database store.DBTX
	policies *machine.DirectPolicies
}

func NewDirectBindings(database store.DBTX, policies *machine.DirectPolicies) *DirectBindings {
	return &DirectBindings{
		database: database,
		policies: policies,
	}
}

// InstallDirect verifies the policy already installed by the activation step.
// The returned cleanup retains that exact step and instance across retries.
func (db *DirectBindings) InstallDirect(ctx context.Context, binding egress.Binding, policy egress.Policy) (func() error, error) {
	expected, err := machine.DirectNetworkPolicy(policy)
	if err != nil {
		return nil, err
	}

	record, err := LoadSandbox(ctx, db.database, binding.SandboxID, binding.TenantID)
	if err != nil {
		return nil, err
	}
	policyRecord, err := machine.LoadPolicyWitness(ctx, db.database, record.VMID)
	if err != nil {
		return nil, err
	}
	operationID, instance := policyRecord.OperationID, policyRecord.Instance
	step, err := machine.LoadDirectRetirement(ctx, db.database, operationID)
	if err != nil {
		return nil, err
	}
	source := step.Input.Source
	if operation.Terminal(step.Status) || step.State.Phase != "armed" || record.SourceOperation != source.OperationID || record.Instance != instance || instance != source.State.Witness || binding.VSockPath != guest.Socket(source.Input.Resource) || policyRecord.PolicyHash != machine.InstalledPolicyHash(expected) || !reflect.DeepEqual(expected, step.Input.Policy) {
		return nil, fmt.Errorf("%w: direct binding differs from original activation instance", errs.ErrConflict)
	}
	settings, err := LoadSettings(ctx, db.database, record.ID)
	if err != nil {
		return nil, err
	}
	if len(settings.Secrets) != 0 || settings.Generation != operationID {
		return nil, fmt.Errorf("%w: direct binding configuration changed", errs.ErrConflict)
	}
	activation, err := LoadActivation(ctx, db.database, operationID)
	if err != nil {
		return nil, err
	}
	if activation.Hash != step.Input.ActivationHash || activation.Key != step.Input.ActivationKey {
		return nil, fmt.Errorf("%w: direct activation witness changed", errs.ErrManualRecovery)
	}
	return db.policies.Bind(ctx, step, expected)
}
