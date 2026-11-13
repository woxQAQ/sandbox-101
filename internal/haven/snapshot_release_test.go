package haven

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/woxQAQ/haven/loom"
)

func TestHavenSnapshotReleaseRetainsPublishedSnapshot(t *testing.T) {
	parallelLifecycle(t)
	for _, point := range []string{"release_admitted", "release_unlinked", "release_completion_before_commit", "none"} {
		t.Run(point, func(t *testing.T) {
			service, backend := coldService(t, nil)
			capture, request := captureReady(t, service, "checkpoint")
			historical := awaitCapture(t, service, capture.ID)
			capturePublished(t, service, historical, true)
			historyBytes := encode(historical)
			service.opts.Fault = func(candidate string) error {
				if candidate == point {
					return ErrInterrupted
				}
				return nil
			}
			release, err := service.AdmitRelease(context.Background(), ReleaseRequest{TenantID: request.TenantID, Key: "release", SandboxID: request.SandboxID})
			if err != nil && !errors.Is(err, ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "release_admitted" {
				awaitColdWorker(t, service, release.ID)
			}
			service = resumeColdService(t, service, backend)
			deleted := awaitColdWorker(t, service, release.ID)
			if deleted.Status != "succeeded" {
				service.mu.Lock()
				reason := service.lastErrors[release.ID]
				service.mu.Unlock()
				t.Fatal("capture cleanup did not follow VM release", deleted.State, reason)
			}
			if len(deleted.Input.Captures) != 1 {
				t.Fatal("release did not freeze capture cleanup destination")
			}
			if _, err = os.Lstat(historical.Input.Source.Input.Resource.Dir); !os.IsNotExist(err) {
				t.Fatal("source VM files retained after successful release", err)
			}
			snapshot, err := service.GetSnapshot(context.Background(), request.TenantID, capture.Result.SnapshotID)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Manifest == nil {
				t.Fatal("VM cleanup unpublished retained snapshot")
			}
			if err = loom.VerifyFiles(snapshot.Dir, snapshot.Manifest.Files); err != nil {
				t.Fatal("VM cleanup changed independent published bytes", err)
			}
			after, err := loadCapture(context.Background(), service.store.db, capture.ID)
			if err != nil || !bytes.Equal(encode(after), historyBytes) {
				t.Fatal("VM cleanup rewrote historical capture", err)
			}
			var storage, slots, done int
			if err = service.store.db.QueryRow("SELECT (SELECT SUM(storage_mib) FROM quota_resources),(SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND status='done')", "capture:"+capture.ID).Scan(&storage, &slots, &done); err != nil || storage != 192 || slots != 0 || done != 1 {
				t.Fatal("VM cleanup lost snapshot charge or raw-output responsibility", storage, slots, done, err)
			}
			if backend.count("capture") != 1 || backend.count("launch") != 1 {
				t.Fatal("release woke or recaptured source")
			}
		})
	}
}

func TestHavenSnapshotReleaseHandoffAtomicity(t *testing.T) {
	parallelLifecycle(t)
	service, backend := coldService(t, nil)
	operation, request := captureReady(t, service, "handoff")
	historical := awaitCapture(t, service, operation.ID)
	capturePublished(t, service, historical, true)
	if _, err := service.store.db.Exec("CREATE TRIGGER deny_capture_handoff BEFORE UPDATE ON cleanup_intents WHEN NEW.status='transferred' BEGIN SELECT RAISE(IGNORE); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.AdmitRelease(context.Background(), ReleaseRequest{TenantID: request.TenantID, Key: "release", SandboxID: request.SandboxID}); err == nil {
		t.Fatal("ignored capture handoff accepted release")
	}
	sandbox, err := loadSandbox(context.Background(), service.store.db, request.SandboxID, request.TenantID)
	if err != nil || sandbox.State == "terminated" {
		t.Fatal("failed handoff partially revoked sandbox", err)
	}
	var refs, cleanup int
	if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND holder_kind='capture' AND holder_id=?),(SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND status='pending')", operation.ID, "capture:"+operation.ID).Scan(&refs, &cleanup); err != nil || refs != 1 || cleanup != 1 {
		t.Fatal("failed handoff lost capture obligation", refs, cleanup, err)
	}
	if backend.count("stop") != 0 || backend.count("remove") != 0 {
		t.Fatal("host deletion preceded committed capture handoff")
	}
}
