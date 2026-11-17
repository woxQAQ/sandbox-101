package component

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/sandbox"
)

func TestHavenDiscardAdmissionAtomicity(t *testing.T) {
	parallelLifecycle(t)
	service, backend, source := failedColdFixture(t)
	if _, err := service.State.Database().Exec("CREATE TRIGGER deny_discard_handoff BEFORE UPDATE ON cleanup_intents WHEN NEW.status='transferred' BEGIN SELECT RAISE(IGNORE); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.VMDeletion.AdmitDiscard(context.Background(), discardRequest(source)); err == nil {
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

func TestHavenDiscardDoesNotMutateDevicesOnUnknownInspection(t *testing.T) {
	parallelLifecycle(t)
	service, backend, source := failedColdFixture(t)
	backend.Mu.Lock()
	backend.InspectError = errors.New("fixture unknown sibling")
	backend.Mu.Unlock()
	networks := backend.Count("network")
	discard, err := service.VMDeletion.AdmitDiscard(context.Background(), discardRequest(source))
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDiscard(t, service, discard.ID)
	if result.Status == "succeeded" || backend.Count("stop") != 1 || backend.Count("network") != networks || backend.Count("remove") != 0 {
		t.Fatal("unknown full inventory authorized a device mutation")
	}
	var held int
	if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?", source.OperationID).Scan(&held); err != nil || held != 64 {
		t.Fatal("unknown sibling retired liability", held, err)
	}
}

func TestHavenDiscardRetainsUnprovedRestoreDirectory(t *testing.T) {
	parallelLifecycle(t)
	service, backend, snapshot := cloneFixtureWithFault(t, func(point string) error {
		if point == "restore_prepare_intent" {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation := admitCloneFixture(t, service, snapshot.Result.SnapshotID, "unproved")
	pending := awaitColdWorker(t, service, operation.ID)
	if err := os.MkdirAll(pending.Input.Resource.RestoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(pending.Input.Resource.RestoreDir, "config.json")
	if err := os.WriteFile(unknown, []byte("unproved fixture"), 0o444); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	source := awaitColdWorker(t, service, operation.ID)
	if source.Status != "failed" || source.State.Materials != nil {
		t.Fatal("fixture unexpectedly established restore boundary", source.State)
	}
	discard, err := service.VMDeletion.AdmitDiscard(context.Background(), discardRequest(source))
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDiscard(t, service, discard.ID)
	if result.Status == "succeeded" {
		t.Fatal("current pathname created missing historical directory proof")
	}
	data, err := os.ReadFile(unknown)
	if err != nil || string(data) != "unproved fixture" {
		t.Fatal("unproved directory was reclaimed", err)
	}
	var held int
	if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?", source.OperationID).Scan(&held); err != nil || held != 64 {
		t.Fatal("unproved directory retired cost", held, err)
	}
}
