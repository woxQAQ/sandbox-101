package sandbox_test

import (
	"context"
	"errors"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestHavenForkRequiresExactlyOneSource(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
	ctx := t.Context()
	principal := authorization.Principal{TenantID: "tenant"}
	claim := (testfixture.SandboxClaims{App: service}).Claim(t, "source", nil, egress.Policy{Mode: "none"})
	checkpoint, err := service.CommandAdmission.Execute(ctx, principal, "checkpoint", operation.Intent{
		Action:    "checkpoint",
		SandboxID: claim.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	type totals struct{ operations, slots, storage int64 }
	readTotals := func() totals {
		t.Helper()
		var got totals
		const query = `SELECT (SELECT COUNT(*) FROM operations), SUM(sandbox_slots), SUM(storage_mib) FROM quota_resources WHERE deleted_at IS NULL`
		if err := service.State.Database().QueryRowContext(ctx, query).Scan(&got.operations, &got.slots, &got.storage); err != nil {
			t.Fatal(err)
		}
		return got
	}
	before := readTotals()
	for _, command := range []operation.Intent{
		{Action: "fork"},
		{Action: "fork", SandboxID: claim.Result.SandboxID, CheckpointID: checkpoint.Result.CheckpointID},
	} {
		accepted, err := service.CommandAdmission.Submit(ctx, principal, "invalid-source", command)
		if !errors.Is(err, errs.ErrInvalid) || accepted.ID != "" {
			t.Fatalf("invalid fork sources yielded operation %q, error %v", accepted.ID, err)
		}
		if after := readTotals(); after != before {
			t.Fatalf("rejected fork changed operations or quota: before %+v, after %+v", before, after)
		}
	}
}

func TestHavenForkBatchAdmissionIsAtomic(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	source := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "source", nil, egress.Policy{
		Mode: "none",
	})
	if _, err := service.Tenants.PutTenant(context.Background(), authorization.Principal{
		Root: true,
	}, types.Tenant{
		ID:            "tenant",
		Token:         "quota-test-token",
		MaxSandboxes:  2,
		MaxStorageMiB: 4096,
	}); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM operations").Scan(&before); err != nil {
		t.Fatal(err)
	}
	acceptedOperation, err := service.Forks.AdmitFork(context.Background(), principal, "too-many", operation.Intent{
		Action:    "fork",
		SandboxID: source.Result.SandboxID,
		Count:     2,
	})
	if !errors.Is(err, errs.ErrExhausted) || acceptedOperation.ID != "" {
		t.Fatal(acceptedOperation, err)
	}
	var after, slots int
	if err := service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM operations),(SELECT SUM(sandbox_slots) FROM quota_resources WHERE deleted_at IS NULL)").Scan(&after, &slots); err != nil || after != before || slots != 1 || runtime.Count("capture") != 0 || runtime.Count("launch") != 1 {
		t.Fatal("rejected batch left intent or launched effects", after, before, slots, err)
	}
}
