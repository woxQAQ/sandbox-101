package component

import (
	"testing"

	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenRestoreHandoffRequiresRunningStep(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend, source := fixtures.CloneFixture(t)
	// Change the step inside the attempted handoff, after its original proof was
	// loaded. The conditional write must roll back backing, lease and costs.
	if _, err := service.State.Database().Exec(`CREATE TRIGGER change_restore_status
AFTER INSERT ON backing BEGIN
 UPDATE operation_steps SET status='pending'
 WHERE operation_id=(SELECT operation_id FROM vms WHERE id=NEW.vm_id) AND step_key='resource';
END`); err != nil {
		t.Fatal(err)
	}
	accepted := fixtures.AdmitCloneFixture(t, service, source.Result.SnapshotID, "running-fence")
	step := fixtures.AwaitColdWorker(t, service, accepted.ID)
	if step.Status != "running" || step.State.BackingReady || step.State.Phase != "restore_materials_complete" {
		t.Fatalf("handoff crossed running fence or failed to roll back: status=%s state=%+v", step.Status, step.State)
	}
	fixtures.CloneLiability(t, service, step, false)
	if backend.Count("launch") != 1 {
		t.Fatal("candidate launched before source handoff")
	}
	if _, err := service.State.Database().Exec("DROP TRIGGER change_restore_status"); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	step = fixtures.AwaitColdWorker(t, service, accepted.ID)
	fixtures.CloneLiability(t, service, step, true)
	if backend.Count("launch") != 2 {
		t.Fatal("handoff retry repeated a launch")
	}
}
