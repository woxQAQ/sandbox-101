package fixtures

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
)

func FailedColdFixture(t *testing.T) (*application.App, *testfixture.ColdRuntime, machine.ResourceStep) {
	t.Helper()
	return FailedColdFixtureWithFault(t, nil)
}

func FailedColdFixtureWithFault(t *testing.T, fault func(string) error) (*application.App, *testfixture.ColdRuntime, machine.ResourceStep) {
	t.Helper()
	service, backend := ColdService(t, fault)
	backend.FailBoot = true
	operation := ColdClaim(t, service, "failed-cold")
	source := AwaitColdWorker(t, service, operation.ID)
	if source.Status != "failed" || source.State.Witness != "" {
		t.Fatal("cold fixture did not fail", source.State)
	}
	backend.Mu.Lock()
	backend.FailBoot = false
	backend.Mu.Unlock()
	return service, backend, source
}

func FailedCloneFixture(t *testing.T, transferred bool) (*application.App, *RestorableRuntime, machine.ResourceStep) {
	t.Helper()
	service, backend, snapshot := CloneFixtureWithFault(t, func(point string) error {
		if !transferred && point == "restore_candidate_copied" {
			return errs.ErrInterrupted
		}
		return nil
	})
	if transferred {
		backend.Mu.Lock()
		backend.FailBoot = true
		backend.Mu.Unlock()
	}
	clone := AdmitCloneFixture(t, service, snapshot.Result.SnapshotID, "failed-clone")
	AwaitColdWorker(t, service, clone.ID)
	if !transferred {
		service = testapp.ReopenCloneService(t, service, backend)
	}
	source := AwaitColdWorker(t, service, clone.ID)
	if source.Status != "failed" || !source.State.RestoreBlocked || source.State.BackingReady != transferred {
		t.Fatal("clone fixture did not fix expected failure", source.State)
	}
	backend.Mu.Lock()
	backend.FailBoot = false
	backend.Mu.Unlock()
	return service, backend, source
}

func DiscardRequest(source machine.ResourceStep) machine.DiscardRequest {
	return machine.DiscardRequest{
		TenantID:          source.Input.TenantID,
		Key:               "discard",
		SandboxID:         source.Input.SandboxID,
		FailedOperationID: source.OperationID,
		ExpectedInstance:  source.Input.Resource.Instance,
	}
}

func AwaitDiscard(t *testing.T, service *application.App, id string) machine.DiscardStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, seen := worker.Active, worker.Completed
		step, err := machine.LoadDiscard(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || Terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatal("discard worker did not finish", step.State)
		}
		time.Sleep(time.Millisecond)
	}
}

type DiscardExpectation struct {
	Source   machine.ResourceStep
	Original []byte
}

func DiscardSucceeded(t *testing.T, service *application.App, discard machine.DiscardStep, expected DiscardExpectation) {
	source, original := expected.Source, expected.Original
	t.Helper()
	if discard.Status != "succeeded" {
		reason := service.Workers.State(discard.OperationID).Err
		t.Fatal("discard did not complete", discard.State, reason)
	}
	if _, err := os.Lstat(source.Input.Resource.Dir); !os.IsNotExist(err) {
		t.Fatal("discard reported success with candidate files retained", err)
	}
	after, err := service.Operations.GetOperation(context.Background(), source.Input.TenantID, source.OperationID)
	if err != nil || !bytes.Equal(testfixture.Encode(after), original) {
		t.Fatal("discard rewrote original public failure", err)
	}
	history, err := machine.LoadResourceStep(context.Background(), service.State.Database(), source.OperationID)
	if err != nil || !bytes.Equal(testfixture.Encode(history), testfixture.Encode(source)) {
		t.Fatal("discard upgraded or changed failed resource witness", err)
	}
	for _, check := range []struct {
		q   string
		arg any
	}{
		{"SELECT COUNT(*) FROM vms WHERE id=?", source.Candidate},
		{"SELECT COUNT(*) FROM quota_resources WHERE source_operation_id=?", source.OperationID},
		{"SELECT COUNT(*) FROM source_leases WHERE operation_id=?", source.OperationID},
		{"SELECT COUNT(*) FROM backing WHERE vm_id=?", source.Candidate},
	} {
		var count int
		if err = service.State.Database().QueryRow(check.q, check.arg).Scan(&count); err != nil || count != 0 {
			t.Fatal("discard did not retire proven candidate liability", check.q, count, err)
		}
	}
}
