package component

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenStopRequiresOriginalDiskFunding(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, damage := range []string{
		"source",
		"amount",
		"holder",
	} {
		t.Run(damage, func(t *testing.T) {
			service, backend := fixtures.ColdService(t, nil)
			claim := fixtures.ColdClaim(t, service, "source")
			history := fixtures.AwaitColdWorker(t, service, claim.ID)
			other := fixtures.ColdClaim(t, service, "other")
			if history.Status != "succeeded" || fixtures.AwaitColdWorker(t, service, other.ID).Status != "succeeded" {
				t.Fatal("fixture resources did not complete")
			}
			var before []byte
			if err := service.State.Database().QueryRow("SELECT body FROM vms WHERE id=? AND operation_id IS NULL", claim.Result.VMID).Scan(&before); err != nil {
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
			if err := fixtures.RequireWrite(service.State.Database().Exec(query, changed, history.Input.DiskKey)); err != nil {
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
			if err = service.State.Database().QueryRow("SELECT body FROM vms WHERE id=? AND operation_id IS NULL", claim.Result.VMID).Scan(&after); err != nil || !bytes.Equal(before, after) {
				t.Fatal("rejected stop changed current custody", err)
			}
			var retained int64
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources").Scan(&retained); err != nil || retained != charged {
				t.Fatal("rejected stop retired responsibility", retained, charged, err)
			}
			frozen, err := machine.LoadResourceStep(context.Background(), service.State.Database(), claim.ID)
			if err != nil || !bytes.Equal(testfixture.Encode(frozen), testfixture.Encode(history)) {
				t.Fatal("rejected stop rewrote original witness", err)
			}
			if err = fixtures.RequireWrite(service.State.Database().Exec(query, original, history.Input.DiskKey)); err != nil {
				t.Fatal(err)
			}
			accepted, err = service.Stops.AdmitStop(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if stopped := fixtures.AwaitStop(t, service, accepted.ID); stopped.Status != "succeeded" || backend.Count("stop") != 1 {
				t.Fatal("restored original proof did not permit exactly one stop", stopped)
			}
		})
	}
}

func TestHavenReleaseRequiresOriginalGenerationRecords(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, damage := range []string{
		"version-owner",
		"instance-owner",
		"missing-version",
		"missing-instance",
	} {
		t.Run(damage, func(t *testing.T) {
			service, backend := fixtures.ColdService(t, func(point string) error {
				if point == "release_admitted" {
					return errs.ErrInterrupted
				}
				return nil
			})
			claim := fixtures.ColdClaim(t, service, "claim")
			history := fixtures.AwaitColdWorker(t, service, claim.ID)
			if history.Status != "succeeded" {
				t.Fatal(history.State)
			}
			release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
				TenantID:  "tenant",
				Key:       "release",
				SandboxID: claim.Result.SandboxID,
			})
			if !errors.Is(err, errs.ErrInterrupted) {
				t.Fatal("release did not stop after admission", err)
			}
			// Current ownership is valid for this release. Only the separately
			// retained birth proof is damaged, within this fixture's private DB.
			var currentBody []byte
			if err = service.State.Database().QueryRow("SELECT body FROM vms WHERE id=? AND current_instance=? AND operation_id=?", claim.Result.VMID, history.State.Witness, release.ID).Scan(&currentBody); err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "version-owner":
				err = fixtures.RequireWrite(service.State.Database().Exec("UPDATE resource_versions SET operation_id=? WHERE id=?", release.ID, claim.ID))
			case "instance-owner":
				err = fixtures.RequireWrite(service.State.Database().Exec("UPDATE instances SET operation_id=? WHERE vm_id=?", release.ID, claim.Result.VMID))
			case "missing-version":
				err = fixtures.RequireWrite(service.State.Database().Exec("DELETE FROM resource_versions WHERE id=?", claim.ID))
			case "missing-instance":
				err = fixtures.RequireWrite(service.State.Database().Exec("DELETE FROM instances WHERE vm_id=?", claim.Result.VMID))
			}
			if err != nil {
				t.Fatal(err)
			}
			service.Maintenance.Dispatch(release.ID)
			step := fixtures.AwaitColdWorker(t, service, release.ID)
			if step.Status == "succeeded" || service.Workers.State(release.ID).Err == nil || backend.Count("stop") != 0 || backend.Count("remove") != 0 {
				t.Fatal("current ownership replaced the original generation proof", step.State)
			}
			var after []byte
			if err = service.State.Database().QueryRow("SELECT body FROM vms WHERE id=? AND current_instance=? AND operation_id=?", claim.Result.VMID, history.State.Witness, release.ID).Scan(&after); err != nil || !bytes.Equal(after, currentBody) {
				t.Fatal("failed proof changed current VM custody", err)
			}
			var storage int64
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources").Scan(&storage); err != nil || storage != 64 {
				t.Fatal("unproved generation lost its storage responsibility", storage, err)
			}
			original, err := machine.LoadResourceStep(context.Background(), service.State.Database(), claim.ID)
			if err != nil || !bytes.Equal(testfixture.Encode(original), testfixture.Encode(history)) {
				t.Fatal("release rewrote the original terminal witness", err)
			}
		})
	}
}
