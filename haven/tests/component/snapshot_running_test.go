package component

import "testing"

func TestHavenSnapshotCompletionRequiresRunningStep(t *testing.T) {
	parallelLifecycle(t)
	service, backend := coldService(t, nil)
	// The private trigger changes the step after proof loading, inside the
	// publication transaction. Completion must still require a running step.
	if _, err := service.State.Database().Exec(`CREATE TRIGGER change_capture_status
AFTER UPDATE OF published ON snapshots WHEN NEW.published=1 BEGIN
 UPDATE operation_steps SET status='pending'
 WHERE operation_id=json_extract(NEW.body,'$.operation_id') AND step_key='capture';
END`); err != nil {
		t.Fatal(err)
	}
	accepted, _ := captureReady(t, service, "running-fence")
	step := awaitCapture(t, service, accepted.ID)
	if step.Status != "running" || step.State.Phase != "publish_intent" {
		t.Fatalf("completion crossed running fence or failed to roll back: status=%s phase=%s", step.Status, step.State.Phase)
	}
	capturePublished(t, service, step, false)
	var checkpoints int
	if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM checkpoints").Scan(&checkpoints); err != nil || checkpoints != 0 {
		t.Fatal("checkpoint escaped rejected completion", checkpoints, err)
	}
	if _, err := service.State.Database().Exec("DROP TRIGGER change_capture_status"); err != nil {
		t.Fatal(err)
	}
	service = resumeColdService(t, service, backend)
	step = awaitCapture(t, service, accepted.ID)
	capturePublished(t, service, step, true)
	if backend.Count("capture") != 1 || backend.Count("launch") != 1 {
		t.Fatal("completion retry repeated a host effect")
	}
}
