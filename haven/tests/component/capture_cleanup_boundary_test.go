package component

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestHavenPublishedRawCleanupRequiresTransfer(t *testing.T) {
	parallelLifecycle(t)
	for _, mode := range []string{
		"ABORT,'denied'",
		"IGNORE",
	} {
		t.Run(mode, func(t *testing.T) {
			service, _, capture, request := rawCleanupFixture(t, nil)
			if _, err := service.State.Database().Exec(fmt.Sprintf("CREATE TRIGGER deny_raw_transfer BEFORE UPDATE OF status ON cleanup_intents WHEN OLD.resource_kind='capture' AND NEW.status='transferred' BEGIN SELECT RAISE(%s); END", mode)); err != nil {
				t.Fatal(err)
			}
			accepted, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
			if err == nil || accepted.ID != "" {
				t.Fatal("raw cleanup accepted without responsibility transfer", accepted, err)
			}
			var status string
			if err = service.State.Database().QueryRow("SELECT status FROM cleanup_intents WHERE id=?", "capture:"+capture.OperationID).Scan(&status); err != nil || status != "pending" {
				t.Fatal("rejected handoff changed original responsibility", status, err)
			}
			var operations, owners, storage int
			if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM operations WHERE idem_key='raw'),(SELECT COUNT(*) FROM vms WHERE operation_id IS NOT NULL),(SELECT SUM(storage_mib) FROM quota_resources)").Scan(&operations, &owners, &storage); err != nil || operations != 0 || owners != 0 || storage != 256 {
				t.Fatal("rejected handoff leaked admission or changed custody", operations, owners, storage, err)
			}
			if _, err = os.Stat(capture.State.Boundary.Path); err != nil {
				t.Fatal("rejected handoff removed raw output", err)
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER deny_raw_transfer"); err != nil {
				t.Fatal(err)
			}
			accepted, err = service.CaptureCleanup.Admit(context.Background(), request, nil)
			if err != nil {
				t.Fatal(err)
			}
			if done := awaitRawCleanup(t, service, accepted.ID); done.Status != "succeeded" {
				t.Fatal(done)
			}
		})
	}
}

func TestHavenPublishedRawCleanupRetainsUnknownInstance(t *testing.T) {
	parallelLifecycle(t)
	service, backend, capture, request := rawCleanupFixture(t, nil)
	backend.Mu.Lock()
	backend.InspectError = errors.New("private fixture observation unavailable")
	backend.Mu.Unlock()
	accepted, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	awaitRetainedRaw(t, service, accepted.ID)
	if _, err = os.Stat(capture.State.Boundary.Path); err != nil {
		t.Fatal("unknown instance allowed raw removal", err)
	}
	var holders, storage int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM resource_refs WHERE holder_kind='capture'),(SELECT SUM(storage_mib) FROM quota_resources)").Scan(&holders, &storage); err != nil || holders != 2 || storage != 256 {
		t.Fatal("unknown instance lost original responsibility", holders, storage, err)
	}
	backend.Mu.Lock()
	backend.InspectError = nil
	backend.Mu.Unlock()
	replay, err := service.CaptureCleanup.Admit(context.Background(), request, nil)
	if err != nil || replay.ID != accepted.ID {
		t.Fatal("replay changed raw cleanup identity", replay, err)
	}
	waitFixtureWorker(t, service, accepted.ID)
	if done := awaitRawCleanup(t, service, accepted.ID); done.Status != "succeeded" {
		t.Fatal(done)
	}
}
