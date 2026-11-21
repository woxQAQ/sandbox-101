package workflows_test

import (
	"context"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenInstanceRejectsLateCompletionAfterOwnershipChange(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, change := range []string{
		"vm-owner",
		"current-instance",
		"resource-version",
	} {
		t.Run(change, func(t *testing.T) {
			service, backend := testapp.ColdService(t, func(point string) error {
				if point == "step_agent_ready" {
					return errs.ErrInterrupted
				}
				return nil
			})
			claim := testfixture.ColdClaim(t, service, "late")
			before := testutil.AwaitColdWorker(t, service, claim.ID)
			var other string
			if err := service.State.Database().QueryRow("SELECT operation_id FROM template_intents WHERE template_id='cold'").Scan(&other); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "vm-owner":
				if _, err := service.State.Database().Exec("UPDATE vms SET operation_id=? WHERE id=?", other, before.Candidate); err != nil {
					t.Fatal(err)
				}
			case "current-instance":
				if _, err := service.State.Database().Exec("UPDATE vms SET current_instance=? WHERE id=?", operation.NewID("run-"), before.Candidate); err != nil {
					t.Fatal(err)
				}
			case "resource-version":
				if _, err := service.State.Database().Exec("UPDATE resource_versions SET body=json_set(body,'$.resource.instance',?) WHERE vm_id=?", operation.NewID("run-"), before.Candidate); err != nil {
					t.Fatal(err)
				}
			}
			agents := backend.Count("agent")
			service = testapp.ReopenCloneService(t, service, backend)
			after := testutil.AwaitColdWorker(t, service, claim.ID)
			if after.Status == "succeeded" || after.State.Witness != "" || after.Input.Resource.Instance != before.Input.Resource.Instance {
				t.Fatal("late worker completed another owner/instance", after.State)
			}
			if backend.Count("launch") != 1 || backend.Count("agent") != agents || backend.Count("stop") != 0 || backend.Count("remove") != 0 {
				t.Fatal("stale ownership caused another host effect")
			}
			result, err := service.Operations.GetOperation(context.Background(), "tenant", claim.ID)
			if err != nil || result.Result.ResourceReady {
				t.Fatal("current state backfilled a historical delivery proof", err)
			}
			coldResponsibility(t, service, claim, false)
		})
	}
}
