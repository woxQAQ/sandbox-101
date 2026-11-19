package sandbox_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenLifecycleLinkRequiresWrite(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, mode := range []string{
		"ABORT,'denied'",
		"IGNORE",
	} {
		t.Run(mode, func(t *testing.T) {
			service, backend, _, _ := testapp.SandboxFixture(t)
			claim := (testfixture.SandboxClaims{
				App: service,
			}).Claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			if _, err := service.State.Database().Exec(fmt.Sprintf("CREATE TRIGGER deny_link_completion BEFORE UPDATE OF status ON operation_steps WHEN OLD.action='resource_link' AND NEW.status='succeeded' BEGIN SELECT RAISE(%s); END", mode)); err != nil {
				t.Fatal(err)
			}
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			command := operation.Command{
				Action:    "checkpoint",
				SandboxID: claim.Result.SandboxID,
			}
			accepted, err := service.SandboxRequests.Submit(context.Background(), principal, "checkpoint", command)
			if err != nil {
				t.Fatal(err)
			}
			testutil.WaitFixtureWorker(t, service, accepted.ID)
			if worker := service.Workers.State(accepted.ID); !worker.Completed || worker.Err == nil {
				t.Fatal("denied dependency completion was accepted", worker)
			}
			link, status, err := machine.LoadResourceLink(context.Background(), service.State.Database(), accepted.ID, "capture")
			if err != nil || status != "pending" {
				t.Fatal("denied write changed frozen dependency", link, status, err)
			}
			capture, err := machine.LoadCapture(context.Background(), service.State.Database(), link.ChildID)
			if err != nil || capture.Status != "succeeded" {
				t.Fatal("completed child was lost", capture, err)
			}
			if _, err = service.Catalog.GetCheckpoint(context.Background(), principal, accepted.Result.CheckpointID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("checkpoint published without dependency handoff", err)
			}
			var slots, storage int
			if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots, &storage); err != nil || slots != 1 || storage != 256 {
				t.Fatal("denied dependency lost original responsibility", slots, storage, err)
			}
			captures := backend.Count("capture")
			if _, err = service.State.Database().Exec("DROP TRIGGER deny_link_completion"); err != nil {
				t.Fatal(err)
			}
			replay, err := service.SandboxRequests.Submit(context.Background(), principal, "checkpoint", command)
			if err != nil || replay.ID != accepted.ID {
				t.Fatal("replay changed parent identity", replay, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			done, err := service.Operations.Wait(ctx, principal, accepted.ID)
			if err != nil || done.Status != "succeeded" || backend.Count("capture") != captures {
				t.Fatal("replay failed to reuse original child", done, err)
			}
			binding, err := service.Catalog.GetCheckpoint(ctx, principal, accepted.Result.CheckpointID)
			if err != nil || binding.SnapshotID != capture.Input.Snapshot.ID {
				t.Fatal("checkpoint rebound to another publication", binding, err)
			}
		})
	}
}
