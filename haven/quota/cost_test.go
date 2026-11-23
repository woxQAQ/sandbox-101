package quota_test

import (
	"testing"

	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

func TestRetiredCostKeyCanBeChargedAgain(t *testing.T) {
	state := testfixture.OpenState(t)
	db := state.Database()
	ctx := t.Context()
	cost := quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID: "tenant",
			Kind:     "snapshot",
			Key:      "snapshot-id",
		},
		StorageMiB: 32,
	}
	ref := quota.CostReference{
		TenantID:   cost.TenantID,
		Kind:       cost.Kind,
		Key:        cost.Key,
		HolderKind: "active_use",
		HolderID:   "reader",
	}
	for _, id := range []string{"first", "second"} {
		if err := operation.New(db).Insert(ctx, operation.Admission{
			ID:        id,
			ScopeKind: "tenant",
			ScopeID:   cost.TenantID,
			Key:       id,
			Status:    "pending",
			Request: operation.Request{
				Action: "snapshot",
				Resource: &operation.ResourceRequest{
					TenantID: cost.TenantID,
					Key:      id,
				},
			},
		}); err != nil {
			t.Fatal(err)
		}
		cost.SourceOperation = id
		if err := quota.AddCost(ctx, db, cost); err != nil {
			t.Fatal(err)
		}
		if err := quota.AddCostReference(ctx, db, ref); err != nil {
			t.Fatal(err)
		}
		if err := quota.RetireMatchingCost(ctx, db, cost); err == nil {
			t.Fatal("retired a cost with an active holder")
		}
		usage, err := quota.TenantUsage(ctx, db, cost.TenantID)
		if err != nil || usage.StorageMiB != cost.StorageMiB {
			t.Fatalf("charged usage for %s: %+v, %v", id, usage, err)
		}
		if err := quota.RemoveCostReference(ctx, db, ref); err != nil {
			t.Fatal(err)
		}
		if err := quota.RetireMatchingCost(ctx, db, cost); err != nil {
			t.Fatal(err)
		}
		if err := quota.RetireMatchingCost(ctx, db, cost); err == nil {
			t.Fatal("retired the same charge twice")
		}
		if err := quota.AddCostReference(ctx, db, ref); err == nil {
			t.Fatal("attached a holder to a retired charge")
		}
	}
	var charges, holders int
	if err := db.QueryRowContext(ctx, `SELECT
		(SELECT count(DISTINCT source_operation_id) FROM quota_resources WHERE resource_key=? AND deleted_at IS NOT NULL),
		(SELECT count(*) FROM quota_refs WHERE resource_key=? AND deleted_at IS NOT NULL)`, cost.Key, ref.Key).Scan(&charges, &holders); err != nil || charges != 2 || holders != 2 {
		t.Fatalf("retained charge history: charges=%d holders=%d err=%v", charges, holders, err)
	}
}
