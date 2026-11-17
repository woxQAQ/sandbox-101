package component

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenResumeActivationHandoffRejectsSQL(t *testing.T) {
	parallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT,'private activation handoff refusal'",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, restorableBackend)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			claim := (sandboxClaims{
				App: service,
			}).claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			paused, err := service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
				Action:    "pause",
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil || before.State != "paused" || before.SnapshotID == "" {
				t.Fatal("fixture lacks paused snapshot", before, err)
			}
			const responsibility = `SELECT
			 (SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='sleep' AND holder_id=?),
			 (SELECT COUNT(*) FROM quota_refs WHERE tenant_id='tenant' AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?)`
			checkResponsibility := func(want int) {
				t.Helper()
				var references, charges int
				if err := service.State.Database().QueryRowContext(ctx, responsibility, before.SnapshotID, before.ID, before.SnapshotID, "sleep:"+before.ID).Scan(&references, &charges); err != nil || references != want || charges != want {
					t.Fatal("sleep snapshot custody and charges diverged", references, charges, want, err)
				}
			}
			checkResponsibility(1)
			if _, err = service.State.Database().Exec("CREATE TRIGGER reject_activation_completion BEFORE UPDATE OF status ON operation_steps WHEN OLD.action='activate' AND NEW.status='succeeded' BEGIN SELECT RAISE(" + rejection + "); END"); err != nil {
				t.Fatal(err)
			}
			command := operation.Command{
				Action:    "resume",
				SandboxID: before.ID,
			}
			failed, err := service.SandboxRequests.Execute(ctx, principal, "resume", command)
			if err == nil || failed.Status != "failed" {
				t.Fatal("activation succeeded without its final handoff", failed, err)
			}
			waitFixtureWorker(t, service, failed.ID)
			checkResponsibility(1)
			retained, err := service.Catalog.Get(ctx, principal, before.ID)
			if err != nil || retained.State != "paused" || retained.SnapshotID != before.SnapshotID {
				t.Fatal("failed activation lost recoverable paused source", retained, err)
			}
			if _, err = service.Dataplane.Ready(ctx, principal, before.ID); err == nil {
				t.Fatal("failed activation admitted data")
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER reject_activation_completion"); err != nil {
				t.Fatal(err)
			}
			repaired, err := service.SandboxRequests.Execute(ctx, principal, "repair", command)
			if err != nil || repaired.Status != "succeeded" {
				t.Fatal("explicit resume could not reuse retained sleep source", repaired, err)
			}
			checkResponsibility(0)
			if _, err = service.Dataplane.Ready(ctx, principal, before.ID); err != nil {
				t.Fatal("successful activation did not publish data readiness", err)
			}
			for _, original := range []operation.Record{
				paused,
				failed,
			} {
				unchanged, err := service.Operations.GetOperation(ctx, "tenant", original.ID)
				if err != nil || !reflect.DeepEqual(unchanged, original) {
					t.Fatal("repair rewrote an original terminal", original, unchanged, err)
				}
			}
		})
	}
}
