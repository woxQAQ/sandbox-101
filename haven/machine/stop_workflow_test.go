package machine_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenStopCompletionAtomicity(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT,'private stop failure'",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, backend := testapp.ColdService(t, nil)
			claim := testfixture.ColdClaim(t, service, "source")
			source := testutil.AwaitColdWorker(t, service, claim.ID)
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
			accepted, err := service.Stops.AdmitStop(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			step := testutil.AwaitStop(t, service, accepted.ID)
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
			replay, err := service.Stops.AdmitStop(context.Background(), request)
			if err != nil || replay.ID != accepted.ID {
				t.Fatal("stop replay changed immutable operation", replay, err)
			}
			done := testutil.AwaitStop(t, service, accepted.ID)
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

func TestHavenStopRequiresOriginalDiskFunding(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, damage := range []string{
		"source",
		"amount",
		"holder",
	} {
		t.Run(damage, func(t *testing.T) {
			service, backend := testapp.ColdService(t, nil)
			claim := testfixture.ColdClaim(t, service, "source")
			history := testutil.AwaitColdWorker(t, service, claim.ID)
			other := testfixture.ColdClaim(t, service, "other")
			if history.Status != "succeeded" || testutil.AwaitColdWorker(t, service, other.ID).Status != "succeeded" {
				t.Fatal("fixture resources did not complete")
			}
			const readCurrent = "SELECT json_array(state,slot,directory,current_instance,generation_operation_id,resource_hash,verified,agent_ready,deleting,observation_hash) FROM vms WHERE id=? AND operation_id IS NULL"
			var before []byte
			if err := service.State.Database().QueryRow(readCurrent, claim.Result.VMID).Scan(&before); err != nil {
				t.Fatal(err)
			}
			var query string
			var changed, original any
			switch damage {
			case "source":
				query = "UPDATE quota_resources SET source_operation_id=? WHERE tenant_id='tenant' AND kind='vm_disk' AND resource_key=?"
				changed, original = other.ID, claim.ID
			case "amount":
				query = "UPDATE quota_resources SET storage_mib=? WHERE tenant_id='tenant' AND kind='vm_disk' AND resource_key=?"
				changed, original = history.Input.Resource.Config.Shape.DiskMiB+1, history.Input.Resource.Config.Shape.DiskMiB
			case "holder":
				query = "UPDATE quota_refs SET holder_id=? WHERE tenant_id='tenant' AND kind='vm_disk' AND resource_key=? AND holder_kind='active_use'"
				changed, original = "sandbox:"+other.Result.SandboxID, "sandbox:"+claim.Result.SandboxID
			}
			if err := testutil.RequireWrite(service.State.Database().Exec(query, changed, history.Input.DiskKey)); err != nil {
				t.Fatal(err)
			}
			var charged int64
			if err := service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources").Scan(&charged); err != nil {
				t.Fatal(err)
			}
			request := machine.StopRequest{
				TenantID:         "tenant",
				Key:              "stop",
				SandboxID:        claim.Result.SandboxID,
				ExpectedInstance: history.State.Witness,
			}
			accepted, err := service.Stops.AdmitStop(context.Background(), request)
			if err == nil || accepted.ID != "" || backend.Count("stop") != 0 {
				t.Fatal("unproved disk funding authorized stop", accepted, err)
			}
			var after []byte
			if err = service.State.Database().QueryRow(readCurrent, claim.Result.VMID).Scan(&after); err != nil || !bytes.Equal(before, after) {
				t.Fatal("rejected stop changed current custody", err)
			}
			var retained int64
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources").Scan(&retained); err != nil || retained != charged {
				t.Fatal("rejected stop retired responsibility", retained, charged, err)
			}
			frozen, err := machine.LoadResourceStep(context.Background(), service.State.Database(), claim.ID)
			if err != nil || !bytes.Equal(operation.Encode(frozen), operation.Encode(history)) {
				t.Fatal("rejected stop rewrote original witness", err)
			}
			if err = testutil.RequireWrite(service.State.Database().Exec(query, original, history.Input.DiskKey)); err != nil {
				t.Fatal(err)
			}
			accepted, err = service.Stops.AdmitStop(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if stopped := testutil.AwaitStop(t, service, accepted.ID); stopped.Status != "succeeded" || backend.Count("stop") != 1 {
				t.Fatal("restored original proof did not permit exactly one stop", stopped)
			}
		})
	}
}
