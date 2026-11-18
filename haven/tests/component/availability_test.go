package component

import (
	"context"
	"errors"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenDrainPreservesReplayAndRunningAdmission(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	ctx := context.Background()
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	command := operation.Command{
		Action:     "claim",
		TemplateID: "cold",
		TTLSeconds: 60,
		Policy: egress.Policy{
			Mode: "none",
		},
	}
	original, err := service.SandboxRequests.Execute(ctx, principal, "before-drain", command)
	if err != nil || original.Status != "succeeded" {
		t.Fatal(original, err)
	}
	service.Availability.Drain()
	status, err := service.Management.Status(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	})
	if err != nil || !status.Draining {
		t.Fatal("drain decision was not visible to management", status, err)
	}
	replayed, err := service.SandboxRequests.Execute(ctx, principal, "before-drain", command)
	if err != nil || replayed.ID != original.ID || replayed.Status != original.Status {
		t.Fatal("drain changed an accepted operation", replayed, err)
	}
	if _, err = service.Sandboxes.AdmitClaim(ctx, principal, "after-drain", command); !errors.Is(err, errs.ErrExhausted) {
		t.Fatal("draining node admitted a new claim", err)
	}
	var admitted int
	if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM operations WHERE scope_kind='tenant' AND scope_id=? AND idem_key=?", principal.TenantID, "after-drain").Scan(&admitted); err != nil || admitted != 0 {
		t.Fatal("rejected claim left intent", admitted, err)
	}
	running, err := service.Dataplane.Ready(ctx, principal, original.Result.SandboxID)
	if err != nil || running.ID != original.Result.SandboxID || running.State != "running" {
		t.Fatal("drain revoked an existing running sandbox", running, err)
	}
}
