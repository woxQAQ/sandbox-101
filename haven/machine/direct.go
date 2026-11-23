package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/netip"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type DirectRetirementInput struct {
	Source         ResourceStep        `json:"source"`
	ActivationHash string              `json:"activation_hash"`
	ActivationKey  string              `json:"activation_key"`
	Policy         *loom.NetworkPolicy `json:"policy"`
}

type DirectRetirementState struct {
	Phase     string `json:"phase"`
	RetiredBy string `json:"retired_by,omitempty"`
}

type DirectRetirementStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       DirectRetirementInput
	State       DirectRetirementState
}

func LoadDirectRetirement(ctx context.Context, query store.DBTX, id string) (DirectRetirementStep, error) {
	step := DirectRetirementStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "direct_retirement",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(record.Input) != step.Hash {
		return step, fmt.Errorf("direct retirement intent changed")
	}
	if err = json.Unmarshal(record.Input, &step.Input); err != nil {
		return step, err
	}
	step.State = DirectRetirementState{
		Phase:     record.Phase.String,
		RetiredBy: record.RetiredBy.String,
	}
	if record.Action != "direct_retirement" || record.CandidateResourceID != step.Input.Source.Candidate || record.ExpectedInstance != step.Input.Source.State.Witness || step.Input.Policy == nil {
		return step, fmt.Errorf("direct retirement source identity changed")
	}
	return step, nil
}

func transitionDirect(ctx context.Context, tx *sql.Tx, step DirectRetirementStep, next DirectRetirementState, status string) error {
	return store.RequireOne(dbgen.New(tx).TransitionDirectRetirement(ctx, dbgen.TransitionDirectRetirementParams{
		OperationID:    step.OperationID,
		InputHash:      step.Hash,
		PreviousStatus: step.Status,
		PreviousPhase: sql.NullString{
			String: step.State.Phase,
			Valid:  true,
		},
		NextStatus: status,
		NextPhase: sql.NullString{
			String: next.Phase,
			Valid:  true,
		},
		RetiredBy: sql.NullString{
			String: next.RetiredBy,
			Valid:  next.RetiredBy != "",
		},
	}))
}

func DirectNetworkPolicy(policy egress.Policy) (*loom.NetworkPolicy, error) {
	if policy.Mode != "direct" || len(policy.AllowedHosts) != 0 {
		return nil, errs.InvalidInput("direct mode requires IPv4 CIDRs without hostname rules")
	}
	limit := policy.MaxConnections
	if limit == 0 {
		limit = 256
	}
	if limit < 1 || limit > 65535 || len(policy.AllowedCIDRs) > 256 {
		return nil, errs.InvalidInput("invalid direct connection budget")
	}
	seen := map[netip.Prefix]bool{}
	for _, raw := range policy.AllowedCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || !prefix.Addr().Is4() || prefix != prefix.Masked() || prefix.String() != raw || seen[prefix] {
			return nil, errs.InvalidInput("direct mode requires unique canonical IPv4 CIDRs")
		}
		seen[prefix] = true
	}
	result := &loom.NetworkPolicy{
		Version:        1,
		AllowedCIDRs:   append([]string(nil), policy.AllowedCIDRs...),
		MaxConnections: limit,
	}
	return result, host.ValidateNetworkPolicy(result)
}

// NetworkPolicyRecord retains the exact instance and activation that installed
// the policy. A current VM ID alone cannot authorize its removal.
type NetworkPolicyRecord struct {
	VMID        string
	Instance    string
	OperationID string
	PolicyHash  sql.NullString
}

// InstalledPolicyHash binds the host installation to the frozen retirement policy.
func InstalledPolicyHash(policy *loom.NetworkPolicy) sql.NullString {
	if policy == nil {
		return sql.NullString{}
	}
	return sql.NullString{
		String: operation.Digest(operation.Encode(policy)),
		Valid:  true,
	}
}

func policyOwner(ctx context.Context, query store.DBTX, id string) (instance, operationID string, err error) {
	row, err := dbgen.New(query).GetNetworkPolicy(ctx, id)
	return row.Instance, row.OperationID, err
}

func LoadPolicyWitness(ctx context.Context, query store.DBTX, id string) (NetworkPolicyRecord, error) {
	row, err := dbgen.New(query).GetNetworkPolicy(ctx, id)
	return NetworkPolicyRecord{
		VMID:        id,
		Instance:    row.Instance,
		OperationID: row.OperationID,
		PolicyHash:  row.PolicyHash,
	}, err
}

func RecordPolicyWitness(ctx context.Context, query store.DBTX, policy NetworkPolicyRecord) error {
	return store.RequireOne(dbgen.New(query).UpsertNetworkPolicy(ctx, dbgen.UpsertNetworkPolicyParams{
		VmID:        policy.VMID,
		Instance:    policy.Instance,
		OperationID: policy.OperationID,
		PolicyHash:  policy.PolicyHash,
	}))
}

func deletePolicyWitness(ctx context.Context, query store.DBTX, policy NetworkPolicyRecord) error {
	return store.RequireOne(dbgen.New(query).DeleteMatchingNetworkPolicy(ctx, dbgen.DeleteMatchingNetworkPolicyParams{
		VmID:        policy.VMID,
		Instance:    policy.Instance,
		OperationID: policy.OperationID,
		PolicyHash:  policy.PolicyHash,
	}))
}

// Bind verifies physical policy installation and binds cleanup to the supplied
// original retirement step. Sandbox validates its configuration before this call.
func (dp *DirectPolicies) Bind(ctx context.Context, step DirectRetirementStep, expected *loom.NetworkPolicy) (func() error, error) {
	backend, ok := dp.backend.(loom.NetworkPolicyBackend)
	if !ok {
		return nil, fmt.Errorf("%w: direct policy effects unavailable", errs.ErrIncompatible)
	}
	if err := backend.VerifyNetworkPolicy(ctx, step.Input.Source.Input.Resource, expected); err != nil {
		return nil, err
	}
	return func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return dp.retireDirect(cleanupCtx, step.OperationID)
	}, nil
}
