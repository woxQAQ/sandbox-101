package component

import (
	"context"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenInstanceRejectsLateCompletionAfterOwnershipChange(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, change := range []string{
		"vm-owner",
		"current-instance",
		"resource-version",
	} {
		t.Run(change, func(t *testing.T) {
			service, backend := fixtures.ColdService(t, func(point string) error {
				if point == "step_agent_ready" {
					return errs.ErrInterrupted
				}
				return nil
			})
			operation := fixtures.ColdClaim(t, service, "late")
			before := fixtures.AwaitColdWorker(t, service, operation.ID)
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
				if _, err := service.State.Database().Exec("UPDATE vms SET current_instance=? WHERE id=?", testfixture.NewID("run-"), before.Candidate); err != nil {
					t.Fatal(err)
				}
			case "resource-version":
				if _, err := service.State.Database().Exec("UPDATE resource_versions SET body=json_set(body,'$.resource.instance',?) WHERE vm_id=?", testfixture.NewID("run-"), before.Candidate); err != nil {
					t.Fatal(err)
				}
			}
			agents := backend.Count("agent")
			service = fixtures.ResumeColdService(t, service, backend)
			after := fixtures.AwaitColdWorker(t, service, operation.ID)
			if after.Status == "succeeded" || after.State.Witness != "" || after.Input.Resource.Instance != before.Input.Resource.Instance {
				t.Fatal("late worker completed another owner/instance", after.State)
			}
			if backend.Count("launch") != 1 || backend.Count("agent") != agents || backend.Count("stop") != 0 || backend.Count("remove") != 0 {
				t.Fatal("stale ownership caused another host effect")
			}
			result, err := service.Operations.GetOperation(context.Background(), "tenant", operation.ID)
			if err != nil || result.Result.ResourceReady {
				t.Fatal("current state backfilled a historical delivery proof", err)
			}
			fixtures.ColdResponsibility(t, service, operation, false)
		})
	}
}
