package machine_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

func TestHavenSnapshotReleaseRetainsPublishedSnapshot(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, point := range []string{
		"release_admitted",
		"release_unlinked",
		"release_completion_before_commit",
		"none",
	} {
		t.Run(point, func(t *testing.T) {
			service, backend := testapp.ColdService(t, func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			capture, request := testfixture.CaptureReady(t, service, "checkpoint")
			historical := testutil.AwaitCapture(t, service, capture.ID)
			testutil.CapturePublished(t, service, historical, true)
			historyBytes := operation.Encode(historical)
			release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
				TenantID:  request.TenantID,
				Key:       "release",
				SandboxID: request.SandboxID,
			})
			if err != nil && !errors.Is(err, errs.ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "release_admitted" {
				testutil.AwaitColdWorker(t, service, release.ID)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			deleted := testutil.AwaitColdWorker(t, service, release.ID)
			if deleted.Status != "succeeded" {
				reason := service.Workers.State(release.ID).Err
				t.Fatal("capture cleanup did not follow VM release", deleted.State, reason)
			}
			if len(deleted.Input.Captures) != 1 {
				t.Fatal("release did not freeze capture cleanup destination")
			}
			if _, err = os.Lstat(historical.Input.Source.Input.Resource.Dir); !os.IsNotExist(err) {
				t.Fatal("source VM files retained after successful release", err)
			}
			snapshot, err := machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), request.TenantID, capture.Result.SnapshotID)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Manifest == nil {
				t.Fatal("VM cleanup unpublished retained snapshot")
			}
			if err = loom.VerifyFiles(snapshot.Dir, snapshot.Manifest.Files); err != nil {
				t.Fatal("VM cleanup changed independent published bytes", err)
			}
			after, err := machine.LoadCapture(context.Background(), service.State.Database(), capture.ID)
			if err != nil || !bytes.Equal(operation.Encode(after), historyBytes) {
				t.Fatal("VM cleanup rewrote historical capture", err)
			}
			var storage, slots, done int
			if err = service.State.Database().QueryRow("SELECT (SELECT SUM(storage_mib) FROM quota_resources WHERE deleted_at IS NULL),(SELECT SUM(sandbox_slots) FROM quota_resources WHERE deleted_at IS NULL),(SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND status='done')", "capture:"+capture.ID).Scan(&storage, &slots, &done); err != nil || storage != 192 || slots != 0 || done != 1 {
				t.Fatal("VM cleanup lost snapshot charge or raw-output responsibility", storage, slots, done, err)
			}
			if backend.Count("capture") != 1 || backend.Count("launch") != 1 {
				t.Fatal("release woke or recaptured source")
			}
		})
	}
}

func TestHavenSnapshotReleaseHandoffAtomicity(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend := testapp.ColdService(t, nil)
	operation, request := testfixture.CaptureReady(t, service, "handoff")
	historical := testutil.AwaitCapture(t, service, operation.ID)
	testutil.CapturePublished(t, service, historical, true)
	if _, err := service.State.Database().Exec("CREATE TRIGGER deny_capture_handoff BEFORE UPDATE ON cleanup_intents WHEN NEW.status='transferred' BEGIN SELECT RAISE(IGNORE); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  request.TenantID,
		Key:       "release",
		SandboxID: request.SandboxID,
	}); err == nil {
		t.Fatal("ignored capture handoff accepted release")
	}
	sandboxView, err := sandbox.LoadSandbox(context.Background(), service.State.Database(), request.SandboxID, request.TenantID)
	if err != nil || sandboxView.State == "terminated" {
		t.Fatal("failed handoff partially revoked sandbox", err)
	}
	var refs, cleanup int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM resource_refs WHERE deleted_at IS NULL AND resource_kind='vm' AND holder_kind='capture' AND holder_id=?),(SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND status='pending')", operation.ID, "capture:"+operation.ID).Scan(&refs, &cleanup); err != nil || refs != 1 || cleanup != 1 {
		t.Fatal("failed handoff lost capture obligation", refs, cleanup, err)
	}
	if backend.Count("stop") != 0 || backend.Count("remove") != 0 {
		t.Fatal("host deletion preceded committed capture handoff")
	}
}

func TestHavenReleaseRequiresOriginalGenerationRecords(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, damage := range []string{
		"version-owner",
		"instance-owner",
		"missing-version",
		"missing-instance",
	} {
		t.Run(damage, func(t *testing.T) {
			service, backend := testapp.ColdService(t, func(point string) error {
				if point == "release_admitted" {
					return errs.ErrInterrupted
				}
				return nil
			})
			claim := testfixture.ColdClaim(t, service, "claim")
			history := testutil.AwaitColdWorker(t, service, claim.ID)
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
			const readCurrent = "SELECT json_array(state,slot,directory,generation_operation_id,resource_hash,verified,agent_ready,deleting,observation_hash) FROM vms WHERE id=? AND current_instance=? AND operation_id=?"
			var currentBody []byte
			if err = service.State.Database().QueryRow(readCurrent, claim.Result.VMID, history.State.Witness, release.ID).Scan(&currentBody); err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "version-owner":
				err = testutil.RequireWrite(service.State.Database().Exec("UPDATE resource_versions SET operation_id=? WHERE operation_id=?", release.ID, claim.ID))
			case "instance-owner":
				err = testutil.RequireWrite(service.State.Database().Exec("UPDATE instances SET operation_id=? WHERE vm_id=?", release.ID, claim.Result.VMID))
			case "missing-version":
				err = testutil.RequireWrite(service.State.Database().Exec("DELETE FROM resource_versions WHERE operation_id=?", claim.ID))
			case "missing-instance":
				err = testutil.RequireWrite(service.State.Database().Exec("DELETE FROM instances WHERE vm_id=?", claim.Result.VMID))
			}
			if err != nil {
				t.Fatal(err)
			}
			service.Reconcile.Dispatch(release.ID)
			step := testutil.AwaitColdWorker(t, service, release.ID)
			if step.Status == "succeeded" || service.Workers.State(release.ID).Err == nil || backend.Count("stop") != 0 || backend.Count("remove") != 0 {
				t.Fatal("current ownership replaced the original generation proof", step.State)
			}
			var after []byte
			if err = service.State.Database().QueryRow(readCurrent, claim.Result.VMID, history.State.Witness, release.ID).Scan(&after); err != nil || !bytes.Equal(after, currentBody) {
				t.Fatal("failed proof changed current VM custody", err)
			}
			var storage int64
			if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE deleted_at IS NULL").Scan(&storage); err != nil || storage != 64 {
				t.Fatal("unproved generation lost its storage responsibility", storage, err)
			}
			original, err := machine.LoadResourceStep(context.Background(), service.State.Database(), claim.ID)
			if err != nil || !bytes.Equal(operation.Encode(original), operation.Encode(history)) {
				t.Fatal("release rewrote the original terminal witness", err)
			}
		})
	}
}
