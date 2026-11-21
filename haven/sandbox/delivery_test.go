package sandbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/types"
)

func TestHavenClaimDelivery(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, guest, _ := testapp.SandboxFixture(t)
	acceptedOperation := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim", map[string]types.EnvVar{
		"VISIBLE": {
			Value: "guest-value",
		},
	}, egress.Policy{
		Mode: "none",
	})
	sandboxView, err := service.Catalog.Get(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, acceptedOperation.Result.SandboxID)
	if err != nil || sandboxView.State != "running" || sandboxView.Instance == "" || !time.Now().Before(sandboxView.Deadline) {
		t.Fatal(sandboxView, err)
	}
	if _, err = service.Dataplane.Ready(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, sandboxView.ID); err != nil {
		t.Fatal(err)
	}
	if runtime.Count("launch") != 1 || len(guest.Activations) != 1 || guest.Activations[0].Env["VISIBLE"] != "guest-value" {
		t.Fatal("claim bypassed or repeated guest activation")
	}
	conn, err := service.Dataplane.Connect(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, sandboxView.ID)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if _, err = service.Dataplane.Connect(context.Background(), authorization.Principal{
		TenantID: "other",
	}, sandboxView.ID); err == nil {
		t.Fatal("foreign workload admission")
	}
}
