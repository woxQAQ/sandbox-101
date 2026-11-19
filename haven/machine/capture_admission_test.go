package machine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
)

func TestHavenCaptureConflictDoesNotDispatch(t *testing.T) {
	testutil.ParallelLifecycle(t)
	faults := &testfixture.SnapshotFaults{}
	faults.Set(func(point string) error {
		if point == "capture_admitted" {
			return errs.ErrInterrupted
		}
		return nil
	})
	service, backend := testapp.ColdService(t, faults.Check)
	accepted, request := testfixture.CaptureReady(t, service, "capture")
	original, err := machine.LoadCapture(context.Background(), service.State.Database(), accepted.ID)
	if err != nil {
		t.Fatal(err)
	}
	faults.Set(nil)
	conflicting := request
	conflicting.ExpectedInstance = testfixture.NewID("run-")
	_, err = service.Captures.AdmitCapture(context.Background(), conflicting)
	worker := service.Workers.State(accepted.ID)
	if !errors.Is(err, errs.ErrConflict) || worker.Active || worker.Completed || backend.Count("pause") != 0 || backend.Count("capture") != 0 {
		t.Fatal("conflicting capture request dispatched retained work", worker, err)
	}
	var storage int
	if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources").Scan(&storage); err != nil || storage != 256 {
		t.Fatal("conflicting request changed frozen responsibility", storage, err)
	}
	replay, err := service.Captures.AdmitCapture(context.Background(), request)
	if err != nil || replay.ID != accepted.ID {
		t.Fatal("matching capture replay changed operation", replay, err)
	}
	done := testutil.AwaitCapture(t, service, accepted.ID)
	if done.Status != "succeeded" || done.Hash != original.Hash || !reflect.DeepEqual(done.Input, original.Input) || backend.Count("capture") != 1 {
		t.Fatal("matching replay did not use original capture proof", done)
	}
	testutil.CapturePublished(t, service, done, true)
}
