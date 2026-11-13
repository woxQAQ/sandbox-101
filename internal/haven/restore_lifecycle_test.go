package haven

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestHavenRestoreReleaseRetiresOnlyClone(t *testing.T) {
	parallelLifecycle(t)
	for _, point := range []string{"release_admitted", "release_unlinked", "release_completion_before_commit"} {
		t.Run(point, func(t *testing.T) {
			service, backend, source := cloneFixture(t)
			clone := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
			historical := awaitColdWorker(t, service, clone.ID)
			cloneLiability(t, service, historical, true)
			frozen := encode(historical)
			service.opts.Fault = func(candidate string) error {
				if candidate == point {
					return ErrInterrupted
				}
				return nil
			}
			release, err := service.AdmitRelease(context.Background(), ReleaseRequest{TenantID: "tenant", Key: "release-clone", SandboxID: clone.Result.SandboxID})
			if err != nil && !errors.Is(err, ErrInterrupted) {
				t.Fatal(err)
			}
			if point != "release_admitted" {
				awaitColdWorker(t, service, release.ID)
			}
			service = reopenCloneService(t, service, backend)
			deleted := awaitColdWorker(t, service, release.ID)
			if deleted.Status != "succeeded" {
				service.mu.Lock()
				reason := service.lastErrors[release.ID]
				service.mu.Unlock()
				t.Fatal("clone cleanup did not converge", deleted.State, reason)
			}
			if _, err = os.Lstat(historical.Input.Resource.Dir); !os.IsNotExist(err) {
				t.Fatal("clone files retained after successful deletion", err)
			}
			var backing, held, sourceCost int
			if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM backing WHERE vm_id=?),(SELECT COUNT(*) FROM quota_resources WHERE source_operation_id=?),(SELECT SUM(storage_mib) FROM quota_resources WHERE kind='snapshot' AND resource_key=?)", clone.Result.VMID, clone.ID, source.Result.SnapshotID).Scan(&backing, &held, &sourceCost); err != nil || backing != 0 || held != 0 || sourceCost != 192 {
				t.Fatal("clone release retired source cost or retained clone debt", backing, held, sourceCost, err)
			}
			after, err := loadStep(context.Background(), service.store.db, clone.ID)
			if err != nil || !bytes.Equal(encode(after), frozen) {
				t.Fatal("cleanup rewrote original restore witness", err)
			}
			snapshot, err := service.GetSnapshot(context.Background(), "tenant", source.Result.SnapshotID)
			if err != nil || snapshot.Manifest == nil {
				t.Fatal("clone release retired source snapshot", err)
			}
		})
	}
}

func TestHavenRestoreLeaseAndIncompleteProof(t *testing.T) {
	parallelLifecycle(t)
	t.Run("source-lease-fk", func(t *testing.T) {
		service, backend, source := cloneFixture(t)
		service.opts.Fault = func(point string) error {
			if point == "admission_committed" {
				return ErrInterrupted
			}
			return nil
		}
		clone := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
		if _, err := service.store.db.Exec("DELETE FROM snapshots WHERE id=?", source.Result.SnapshotID); err == nil {
			t.Fatal("source catalog retired while leased")
		}
		if backend.count("launch") != 1 {
			t.Fatal("unexecuted clone already launched")
		}
		service = reopenCloneService(t, service, backend)
		cloneLiability(t, service, awaitColdWorker(t, service, clone.ID), true)
	})
	for _, damage := range []string{"disk", "memory-inode", "missing-proof"} {
		t.Run(damage, func(t *testing.T) {
			service, backend, source := cloneFixture(t)
			service.opts.Fault = func(point string) error {
				if point == "restore_resources_ready" {
					return ErrInterrupted
				}
				return nil
			}
			clone := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
			before := awaitColdWorker(t, service, clone.ID)
			switch damage {
			case "disk":
				if err := os.Truncate(before.Input.Resource.Disk, 0); err != nil {
					t.Fatal(err)
				}
			case "memory-inode":
				path := filepath.Join(before.Input.Resource.RestoreDir, "memory-ranges")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o444)
				if err != nil {
					t.Fatal(err)
				}
				truncateErr := file.Truncate(before.Input.Resource.Config.Shape.MemoryMiB << 20)
				if err = errors.Join(truncateErr, file.Close()); err != nil {
					t.Fatal(err)
				}
			case "missing-proof":
				state := before.State
				state.Materials = nil
				if _, err := service.store.db.Exec("UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='resource'", encode(state), clone.ID); err != nil {
					t.Fatal(err)
				}
			}
			service = reopenCloneService(t, service, backend)
			after := awaitColdWorker(t, service, clone.ID)
			if after.Status == "succeeded" || after.State.Witness != "" || backend.count("launch") != 1 {
				t.Fatal("damaged handoff became a runtime witness", after.State)
			}
			if !after.State.BackingReady {
				t.Fatal("failure reconstructed source lease instead of retaining backing")
			}
			var leases, backing, held int
			if err := service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM source_leases WHERE operation_id=?),(SELECT COUNT(*) FROM backing WHERE vm_id=?),(SELECT SUM(storage_mib) FROM quota_resources WHERE source_operation_id=?)", clone.ID, clone.Result.VMID, clone.ID).Scan(&leases, &backing, &held); err != nil || leases != 0 || backing != 1 || held != 64 {
				t.Fatal("damaged restore lost conservative debt", leases, backing, held, err)
			}
		})
	}
}

func TestHavenRestoreCloneCanBeCaptured(t *testing.T) {
	parallelLifecycle(t)
	service, backend, source := cloneFixture(t)
	clone := admitCloneFixture(t, service, source.Result.SnapshotID, "clone")
	ready := awaitColdWorker(t, service, clone.ID)
	cloneLiability(t, service, ready, true)
	capture, err := service.AdmitCapture(context.Background(), CaptureRequest{TenantID: "tenant", Key: "clone-capture", SandboxID: clone.Result.SandboxID, ExpectedInstance: ready.State.Witness})
	if err != nil {
		t.Fatal(err)
	}
	proof := awaitCapture(t, service, capture.ID)
	capturePublished(t, service, proof, true)
	if backend.count("launch") != 2 || backend.count("capture") != 2 {
		t.Fatal("resnapshot unexpectedly restarted a clone")
	}
}
