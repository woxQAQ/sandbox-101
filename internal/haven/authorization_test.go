package haven

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
)

func TestHavenSandboxOperationAuthorization(t *testing.T) {
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	tenant := Principal{
		TenantID: "tenant",
	}
	first := claimSandbox(t, service, "one", nil, egress.Policy{
		Mode: "none",
	})
	second := claimSandbox(t, service, "two", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	record, err := ownedSandbox(ctx, service.store.db, tenant, first.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := service.Authenticate(ctx, service.sandboxToken(record))
	if err != nil || scoped.SandboxID != record.ID {
		t.Fatal(scoped, err)
	}
	if _, err = service.Operation(ctx, scoped, first.ID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{
		second.ID,
		"missing-operation",
	} {
		if _, err = service.Operation(ctx, scoped, id); err == nil {
			t.Fatal("sandbox credential discovered foreign operation", id)
		}
		if _, err = service.Wait(ctx, scoped, id); err == nil {
			t.Fatal("sandbox credential polled foreign operation", id)
		}
	}
	renewal, err := service.AdmitRenew(ctx, scoped, "renew", Command{
		Action:     "renew",
		SandboxID:  record.ID,
		TTLSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Wait(ctx, scoped, renewal.ID); err != nil {
		t.Fatal("sandbox credential cannot poll its lease operation", err)
	}
	checkpoint, err := service.Execute(ctx, tenant, "checkpoint", Command{
		Action:    "checkpoint",
		SandboxID: record.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := service.Execute(ctx, tenant, "fork", Command{
		Action:       "fork",
		CheckpointID: checkpoint.Result.CheckpointID,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Operation(ctx, scoped, fork.ID); err == nil {
		t.Fatal("source sandbox credential read fork destination")
	}
	if _, err = service.Operation(ctx, Principal{
		TenantID: "other",
	}, first.ID); err == nil {
		t.Fatal("foreign tenant discovered operation")
	}
}
