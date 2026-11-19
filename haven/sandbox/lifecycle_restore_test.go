package sandbox_test

import (
	"context"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/tests/fixtures"
	"github.com/woxQAQ/haven/types"
)

func TestHavenRestorePeakQuotaPreservesIntegerBoundary(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, request, prior := fixtures.TargetSetup(t)
	ctx := context.Background()
	var charged int64
	if err := service.State.Database().QueryRowContext(ctx, "SELECT SUM(storage_mib) FROM quota_resources WHERE tenant_id=?", request.TenantID).Scan(&charged); err != nil {
		t.Fatal(err)
	}
	// An independently retained charge places the tenant one unit above the
	// exact integer range of float64. Admission must retain that last unit.
	const total int64 = 1<<53 + 1
	if err := quota.New(service.State.Database()).AddCost(ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        request.TenantID,
			Kind:            "pending_storage",
			Key:             "fixture-peak-charge",
			SourceOperation: prior.OperationID,
		},
		StorageMiB: total - charged,
	}); err != nil {
		t.Fatal(err)
	}
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	tenant := types.Tenant{
		ID:            request.TenantID,
		Token:         "unused-quota-boundary-fixture",
		MaxSandboxes:  16,
		MaxStorageMiB: total + prior.Input.Resource.Config.Shape.DiskMiB - 1,
	}
	if _, err := service.Tenants.PutTenant(ctx, root, tenant); err != nil {
		t.Fatal(err)
	}
	accepted, err := service.Provisioning.AdmitRestore(ctx, request)
	if err == nil || !strings.Contains(err.Error(), "restore tenant peak quota exhausted") || accepted.ID != "" {
		t.Fatal("restore rounded away retained storage", accepted, err)
	}
	var operations, instances, versions int
	if err = service.State.Database().QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM operations WHERE scope_kind='tenant' AND scope_id=? AND idem_key=?),(SELECT COUNT(*) FROM instances WHERE vm_id=?),(SELECT COUNT(*) FROM resource_versions WHERE vm_id=?)", request.TenantID, request.Key, prior.Candidate, prior.Candidate).Scan(&operations, &instances, &versions); err != nil || operations != 0 || instances != 1 || versions != 1 {
		t.Fatal("rejected quota admission changed generation custody", operations, instances, versions, err)
	}
	tenant.MaxStorageMiB++
	if _, err = service.Tenants.PutTenant(ctx, root, tenant); err != nil {
		t.Fatal(err)
	}
	accepted, err = service.Provisioning.AdmitRestore(ctx, request)
	if err != nil {
		t.Fatal("exact quota boundary was rejected", err)
	}
	if step := fixtures.AwaitColdWorker(t, service, accepted.ID); step.Status != "succeeded" {
		t.Fatal("accepted restore failed", step.State)
	}
}
