package component

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenSnapshotReleaseRetainsPublishedSnapshot(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, point := range []string{
		"release_admitted",
		"release_unlinked",
		"release_completion_before_commit",
		"none",
	} {
		t.Run(point, func(t *testing.T) {
			service, backend := fixtures.ColdService(t, func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			capture, request := fixtures.CaptureReady(t, service, "checkpoint")
			historical := fixtures.AwaitCapture(t, service, capture.ID)
			fixtures.CapturePublished(t, service, historical, true)
			historyBytes := testfixture.Encode(historical)
			release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
				TenantID:  request.TenantID,
				Key:       "release",
				SandboxID: request.SandboxID,
			})
			if err != nil && !errors.Is(err, errs.ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "release_admitted" {
				fixtures.AwaitColdWorker(t, service, release.ID)
			}
			service = fixtures.ResumeColdService(t, service, backend)
			deleted := fixtures.AwaitColdWorker(t, service, release.ID)
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
			if err != nil || !bytes.Equal(testfixture.Encode(after), historyBytes) {
				t.Fatal("VM cleanup rewrote historical capture", err)
			}
			var storage, slots, done int
			if err = service.State.Database().QueryRow("SELECT (SELECT SUM(storage_mib) FROM quota_resources),(SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND status='done')", "capture:"+capture.ID).Scan(&storage, &slots, &done); err != nil || storage != 192 || slots != 0 || done != 1 {
				t.Fatal("VM cleanup lost snapshot charge or raw-output responsibility", storage, slots, done, err)
			}
			if backend.Count("capture") != 1 || backend.Count("launch") != 1 {
				t.Fatal("release woke or recaptured source")
			}
		})
	}
}

func TestHavenSnapshotReleaseHandoffAtomicity(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend := fixtures.ColdService(t, nil)
	operation, request := fixtures.CaptureReady(t, service, "handoff")
	historical := fixtures.AwaitCapture(t, service, operation.ID)
	fixtures.CapturePublished(t, service, historical, true)
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
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND holder_kind='capture' AND holder_id=?),(SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND status='pending')", operation.ID, "capture:"+operation.ID).Scan(&refs, &cleanup); err != nil || refs != 1 || cleanup != 1 {
		t.Fatal("failed handoff lost capture obligation", refs, cleanup, err)
	}
	if backend.Count("stop") != 0 || backend.Count("remove") != 0 {
		t.Fatal("host deletion preceded committed capture handoff")
	}
}
