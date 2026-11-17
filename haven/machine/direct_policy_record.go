package machine

import (
	"context"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// NetworkPolicyRecord retains the exact instance and activation that installed
// the policy. A current VM ID alone cannot authorize its removal.
type NetworkPolicyRecord struct {
	VMID        string
	Instance    string
	OperationID string
	Body        []byte
}

func policyOwner(ctx context.Context, query store.DBTX, id string) (instance, operationID string, err error) {
	row, err := dbgen.New(query).GetNetworkPolicyOwner(ctx, id)
	return row.Instance, row.OperationID, err
}

func LoadPolicyWitness(ctx context.Context, query store.DBTX, id string) (NetworkPolicyRecord, error) {
	row, err := dbgen.New(query).GetNetworkPolicy(ctx, id)
	return NetworkPolicyRecord{
		VMID:        id,
		Instance:    row.Instance,
		OperationID: row.OperationID,
		Body:        row.Body,
	}, err
}

func RecordPolicyWitness(ctx context.Context, query store.DBTX, policy NetworkPolicyRecord) error {
	return store.RequireOne(dbgen.New(query).UpsertNetworkPolicy(ctx, dbgen.UpsertNetworkPolicyParams{
		VmID:        policy.VMID,
		Instance:    policy.Instance,
		OperationID: policy.OperationID,
		Body:        policy.Body,
	}))
}

func deletePolicyWitness(ctx context.Context, query store.DBTX, policy NetworkPolicyRecord) error {
	return store.RequireOne(dbgen.New(query).DeleteMatchingNetworkPolicy(ctx, dbgen.DeleteMatchingNetworkPolicyParams{
		VmID:        policy.VMID,
		Instance:    policy.Instance,
		OperationID: policy.OperationID,
		Body:        policy.Body,
	}))
}
