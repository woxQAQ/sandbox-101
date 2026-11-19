package machine_test

import (
	"context"
	"testing"

	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenDiscardAdmissionAtomicity(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend, source := failedColdFixture(t)
	if _, err := service.State.Database().Exec("CREATE TRIGGER deny_discard_handoff BEFORE UPDATE ON cleanup_intents WHEN NEW.status='transferred' BEGIN SELECT RAISE(IGNORE); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Deletion.AdmitDiscard(context.Background(), discardRequest(source)); err == nil {
		t.Fatal("ignored handoff accepted discard")
	}
	sandboxView, err := sandbox.LoadSandbox(context.Background(), service.State.Database(), source.Input.SandboxID, source.Input.TenantID)
	if err != nil || sandboxView.State == "terminated" || sandboxView.OperationID != source.OperationID {
		t.Fatal("discard admission partially changed sandbox", err)
	}
	var owner string
	if err = service.State.Database().QueryRow("SELECT operation_id FROM vms WHERE id=?", source.Candidate).Scan(&owner); err != nil || owner != source.OperationID {
		t.Fatal("discard admission lost failed owner", owner, err)
	}
	var pending, step int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND status='blocked'),(SELECT COUNT(*) FROM operation_steps WHERE action='discard')", "blocked:"+source.OperationID).Scan(&pending, &step); err != nil || pending != 1 || step != 0 {
		t.Fatal("partial discard intent", pending, step, err)
	}
	if backend.Count("stop") != 0 || backend.Count("remove") != 0 {
		t.Fatal("host effect preceded discard admission")
	}
}
