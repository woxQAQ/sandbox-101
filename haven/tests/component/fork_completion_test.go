package component

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/tests/fixtures"

	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

func TestHavenForkCompletionRejectsSQL(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, runtime, _, _ := testapp.SandboxFixtureWithBackend(t, nil, fixtures.RestorableBackend)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			source := (fixtures.SandboxClaims{
				App: service,
			}).Claim(t, "source", nil, egress.Policy{
				Mode: "none",
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			checkpoint, err := service.SandboxRequests.Execute(ctx, principal, "checkpoint", operation.Command{
				Action:    "checkpoint",
				SandboxID: source.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			raise := "RAISE(IGNORE)"
			if rejection == "ABORT" {
				raise = "RAISE(ABORT,'fork completion rejected')"
			}
			if _, err = service.State.Database().ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER reject_fork_completion BEFORE UPDATE OF status ON operation_steps WHEN OLD.step_key='fork_group' AND NEW.status='succeeded' BEGIN SELECT %s; END", raise)); err != nil {
				t.Fatal(err)
			}
			command := operation.Command{
				Action:       "fork",
				CheckpointID: checkpoint.Result.CheckpointID,
				Count:        2,
				Policy: egress.Policy{
					Mode: "none",
				},
			}
			accepted, err := service.Forks.AdmitFork(ctx, principal, "fork-held", command)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Operations.Wait(ctx, principal, accepted.ID); err == nil {
				t.Fatal("rejected group completion reported success")
			}
			group, err := sandbox.LoadFork(ctx, service.State.Database(), accepted.ID)
			if err != nil || fixtures.Terminal(group.Status) || group.State.Phase != "allocated" || len(group.State.Children) != 2 {
				t.Fatal("failed completion lost committed branch progress", group, err)
			}
			var leases, publications int
			if err = service.State.Database().QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM source_leases WHERE operation_id=? AND step_key='fork_group'),(SELECT COUNT(*) FROM operation_publications WHERE operation_id=?)", accepted.ID, accepted.ID).Scan(&leases, &publications); err != nil || leases != 1 || publications != 0 {
				t.Fatal("failed completion retired parent custody or published a result", leases, publications, err)
			}
			before, err := operation.LoadPublication(ctx, service.State.Database(), group.State.Children[0])
			if err != nil || before.Status != "succeeded" || before.Result.Sandbox == nil {
				t.Fatal("child publication was not retained", before, err)
			}
			launches, clones, captures := runtime.Count("launch"), runtime.Count("clone"), runtime.Count("capture")
			if _, err = service.State.Database().ExecContext(ctx, "DROP TRIGGER reject_fork_completion"); err != nil {
				t.Fatal(err)
			}
			replayed, err := service.SandboxRequests.Execute(ctx, principal, "fork-held", command)
			if err != nil || replayed.ID != accepted.ID || replayed.Status != "succeeded" {
				t.Fatal("repaired group did not complete original admission", replayed, err)
			}
			if runtime.Count("launch") != launches || runtime.Count("clone") != clones || runtime.Count("capture") != captures {
				t.Fatal("group replay repeated resource effects")
			}
			after, err := operation.LoadPublication(ctx, service.State.Database(), group.State.Children[0])
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("group replay changed original branch delivery", after, err)
			}
		})
	}
}
