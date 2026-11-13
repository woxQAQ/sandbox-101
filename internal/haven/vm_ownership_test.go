package haven

import (
	"context"
	"testing"
)

func TestHavenInstanceRejectsLateCompletionAfterOwnershipChange(t *testing.T) {
	for _, change := range []string{"vm-owner", "current-instance", "resource-version"} {
		t.Run(change, func(t *testing.T) {
			service, backend := coldService(t, func(point string) error {
				if point == "step_agent_ready" {
					return ErrInterrupted
				}
				return nil
			})
			operation := coldClaim(t, service, "late")
			before := awaitColdWorker(t, service, operation.ID)
			var other string
			if err := service.store.db.QueryRow("SELECT operation_id FROM template_intents WHERE template_id='cold'").Scan(&other); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "vm-owner":
				if _, err := service.store.db.Exec("UPDATE vms SET operation_id=? WHERE id=?", other, before.Candidate); err != nil {
					t.Fatal(err)
				}
			case "current-instance":
				if _, err := service.store.db.Exec("UPDATE vms SET current_instance=? WHERE id=?", newID("run-"), before.Candidate); err != nil {
					t.Fatal(err)
				}
			case "resource-version":
				if _, err := service.store.db.Exec("UPDATE resource_versions SET body=json_set(body,'$.resource.instance',?) WHERE vm_id=?", newID("run-"), before.Candidate); err != nil {
					t.Fatal(err)
				}
			}
			agents := backend.count("agent")
			service = resumeColdService(t, service, backend)
			after := awaitColdWorker(t, service, operation.ID)
			if after.Status == "succeeded" || after.State.Witness != "" || after.Input.Resource.Instance != before.Input.Resource.Instance {
				t.Fatal("late worker completed another owner/instance", after.State)
			}
			if backend.count("launch") != 1 || backend.count("agent") != agents || backend.count("stop") != 0 || backend.count("remove") != 0 {
				t.Fatal("stale ownership caused another host effect")
			}
			result, err := service.GetOperation(context.Background(), "tenant", operation.ID)
			if err != nil || result.Result.ResourceReady {
				t.Fatal("current state backfilled a historical delivery proof", err)
			}
			coldResponsibility(t, service, operation, false)
		})
	}
}
