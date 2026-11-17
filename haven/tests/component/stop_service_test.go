package component

import (
	"context"
	"testing"

	"github.com/woxQAQ/haven/machine"
)

func TestHavenStopCompletionAtomicity(t *testing.T) {
	parallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT,'private stop failure'",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, backend := coldService(t, nil)
			claim := coldClaim(t, service, "source")
			source := awaitColdWorker(t, service, claim.ID)
			if source.Status != "succeeded" {
				t.Fatal(source.State)
			}
			if _, err := service.State.Database().Exec("CREATE TRIGGER deny_stop_completion BEFORE UPDATE ON operation_steps WHEN OLD.step_key='stop' AND NEW.status='succeeded' BEGIN SELECT RAISE(" + rejection + "); END"); err != nil {
				t.Fatal(err)
			}
			request := machine.StopRequest{
				TenantID:         "tenant",
				Key:              "stop",
				SandboxID:        claim.Result.SandboxID,
				ExpectedInstance: source.State.Witness,
			}
			accepted, err := service.VMStops.AdmitStop(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			step := awaitStop(t, service, accepted.ID)
			if step.Status != "running" || step.Phase != "stop_intent" || service.Workers.State(accepted.ID).Err == nil {
				t.Fatal("failed stop completion lost retryable intent", step)
			}
			current, err := machine.LoadOwnership(context.Background(), service.State.Database(), source.Candidate)
			if err != nil || current.State != "running" || !current.Owned || current.Owner != accepted.ID || current.Instance != source.State.Witness {
				t.Fatal("failed completion partially released VM ownership", current, err)
			}
			observation, err := backend.Inspect(context.Background(), source.Input.Resource)
			if err != nil || observation.Alive || observation.Network {
				t.Fatal("fixture did not cross physical stop boundary", observation, err)
			}
			var storage, slots int
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib),SUM(sandbox_slots) FROM quota_resources").Scan(&storage, &slots); err != nil || storage != 64 || slots != 1 {
				t.Fatal("uncommitted stop lost resource responsibility", storage, slots, err)
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER deny_stop_completion"); err != nil {
				t.Fatal(err)
			}
			replay, err := service.VMStops.AdmitStop(context.Background(), request)
			if err != nil || replay.ID != accepted.ID {
				t.Fatal("stop replay changed immutable operation", replay, err)
			}
			done := awaitStop(t, service, accepted.ID)
			if done.Status != "succeeded" || done.Phase != "complete" || done.Hash != step.Hash || done.Input != step.Input {
				t.Fatal("stop replay did not complete original proof", done)
			}
			current, err = machine.LoadOwnership(context.Background(), service.State.Database(), source.Candidate)
			if err != nil || current.State != "stopped" || current.Owned || current.Instance != source.State.Witness {
				t.Fatal("confirmed stop did not release matching ownership", current, err)
			}
		})
	}
}
