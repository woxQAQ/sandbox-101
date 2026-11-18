package component

import (
	"testing"

	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenSnapshotCompletionRequiresRunningStep(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend := fixtures.ColdService(t, nil)
	// The private trigger changes the step after proof loading, inside the
	// publication transaction. Completion must still require a running step.
	if _, err := service.State.Database().Exec(`CREATE TRIGGER change_capture_status
AFTER UPDATE OF published ON snapshots WHEN NEW.published=1 BEGIN
 UPDATE operation_steps SET status='pending'
 WHERE operation_id=json_extract(NEW.body,'$.operation_id') AND step_key='capture';
END`); err != nil {
		t.Fatal(err)
	}
	accepted, _ := fixtures.CaptureReady(t, service, "running-fence")
	step := fixtures.AwaitCapture(t, service, accepted.ID)
	if step.Status != "running" || step.State.Phase != "publish_intent" {
		t.Fatalf("completion crossed running fence or failed to roll back: status=%s phase=%s", step.Status, step.State.Phase)
	}
	fixtures.CapturePublished(t, service, step, false)
	var checkpoints int
	if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM checkpoints").Scan(&checkpoints); err != nil || checkpoints != 0 {
		t.Fatal("checkpoint escaped rejected completion", checkpoints, err)
	}
	if _, err := service.State.Database().Exec("DROP TRIGGER change_capture_status"); err != nil {
		t.Fatal(err)
	}
	service = fixtures.ResumeColdService(t, service, backend)
	step = fixtures.AwaitCapture(t, service, accepted.ID)
	fixtures.CapturePublished(t, service, step, true)
	if backend.Count("capture") != 1 || backend.Count("launch") != 1 {
		t.Fatal("completion retry repeated a host effect")
	}
}
