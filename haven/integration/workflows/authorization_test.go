package workflows_test

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenSandboxOperationAuthorization(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, fixtures.RestorableBackend)
	tenant := authorization.Principal{
		TenantID: "tenant",
	}
	first := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "one", nil, egress.Policy{
		Mode: "none",
	})
	second := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "two", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	record, err := sandbox.NewCatalog(service.State.Database()).OwnedRecord(ctx, tenant, first.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := service.Authentication.Authenticate(ctx, service.Authentication.SandboxToken(record.TenantID, record.ID))
	if err != nil || scoped.SandboxID != record.ID {
		t.Fatal(scoped, err)
	}
	if _, err = service.Operations.Operation(ctx, scoped, first.ID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{
		second.ID,
		"missing-operation",
	} {
		if _, err = service.Operations.Operation(ctx, scoped, id); err == nil {
			t.Fatal("sandbox credential discovered foreign operation", id)
		}
		if _, err = service.Operations.Wait(ctx, scoped, id); err == nil {
			t.Fatal("sandbox credential polled foreign operation", id)
		}
	}
	renewal, err := service.Lifecycle.AdmitRenew(ctx, scoped, "renew", operation.Command{
		Action:     "renew",
		SandboxID:  record.ID,
		TTLSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Operations.Wait(ctx, scoped, renewal.ID); err != nil {
		t.Fatal("sandbox credential cannot poll its lease operation", err)
	}
	checkpoint, err := service.SandboxRequests.Execute(ctx, tenant, "checkpoint", operation.Command{
		Action:    "checkpoint",
		SandboxID: record.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := service.SandboxRequests.Execute(ctx, tenant, "fork", operation.Command{
		Action:       "fork",
		CheckpointID: checkpoint.Result.CheckpointID,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Operations.Operation(ctx, scoped, fork.ID); err == nil {
		t.Fatal("source sandbox credential read fork destination")
	}
	if _, err = service.Operations.Operation(ctx, authorization.Principal{
		TenantID: "other",
	}, first.ID); err == nil {
		t.Fatal("foreign tenant discovered operation")
	}
}
