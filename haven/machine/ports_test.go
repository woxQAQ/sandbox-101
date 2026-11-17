package machine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

func TestMissingCapabilityPortsRetainIntentAndResponsibility(t *testing.T) {
	ctx := context.Background()
	state := testfixture.OpenState(t)
	var err error
	lifetime := execution.NewLifetime(ctx)
	workers := execution.NewWorkers(lifetime)
	guards := execution.NewGuards()
	defer func() {
		if err := lifetime.Close(ctx, func() {}, func() error { return nil }); err != nil {
			t.Error(err)
		}
	}()
	payload := []byte(`{"action":"snapshot"}`)
	if err = operation.New(state.Database()).Insert(ctx, operation.Admission{
		ID: "retained", ScopeKind: "node", ScopeID: state.Identity().Identity,
		Key: "retained", Hash: operation.Digest(payload), Request: payload,
		Status: "pending", Body: operation.Encode(operation.Result{}),
	}); err != nil {
		t.Fatal(err)
	}
	if err = quota.New(state.Database()).Reserve(ctx, quota.Reservation{Owner: "snapshot:retained", Bytes: 4096}); err != nil {
		t.Fatal(err)
	}
	var epoch int64
	if err = state.Database().QueryRowContext(ctx, "SELECT epoch FROM store_metadata WHERE singleton=1").Scan(&epoch); err != nil {
		t.Fatal(err)
	}

	captures := machine.NewCaptures(state, guards, lifetime, workers, nil, 0, nil, nil)
	stops := machine.NewVMStops(state, guards, lifetime, workers, machine.ResourceProofs{}, nil, nil, nil)
	deletion := machine.NewVMDeletion(state, guards, lifetime, workers, machine.ResourceProofs{}, nil, nil, nil, nil, nil, nil)
	snapshots := machine.NewSnapshotDeletion(state, machine.SnapshotProofs{}, guards, lifetime, workers, nil, nil)
	provision := machine.NewProvisioning(state, ctx, guards.Admission(), machine.ResourceProofs{}, nil, nil, nil, nil, nil)
	for _, scenario := range []struct {
		name string
		run  func() error
	}{
		{"capture admission", func() error { _, err := captures.AdmitCapture(ctx, machine.CaptureRequest{}); return err }},
		{"capture recovery", func() error { _, err := captures.Resume("retained"); return err }},
		{"stop admission", func() error { _, err := stops.AdmitStop(ctx, machine.StopRequest{}); return err }},
		{"stop recovery", func() error { _, err := stops.Resume("retained"); return err }},
		{"release admission", func() error { _, err := deletion.AdmitRelease(ctx, machine.ReleaseRequest{}); return err }},
		{"release recovery", func() error { _, err := deletion.ResumeRelease("retained"); return err }},
		{"snapshot admission", func() error {
			_, err := snapshots.AdmitSnapshotDelete(ctx, machine.SnapshotDeleteRequest{})
			return err
		}},
		{"snapshot recovery", func() error { _, err := snapshots.Resume("retained"); return err }},
		{"provisioning", func() error { return provision.AdvanceLocked(machine.ResourceStep{OperationID: "retained"}) }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if err := scenario.run(); !errors.Is(err, errs.ErrManualRecovery) {
				t.Fatalf("missing port did not fail closed: %v", err)
			}
			var current int64
			if err := state.Database().QueryRowContext(ctx, "SELECT epoch FROM store_metadata WHERE singleton=1").Scan(&current); err != nil || current != epoch {
				t.Fatalf("missing port mutated durable state: %d/%d %v", current, epoch, err)
			}
			worker := workers.Snapshot("retained")
			if worker.Active || worker.Completed {
				t.Fatalf("missing port dispatched work: %+v", worker)
			}
		})
	}
	var status string
	if err = state.Database().QueryRowContext(ctx, "SELECT status FROM operations WHERE id='retained'").Scan(&status); err != nil || status != "pending" {
		t.Fatal(status, err)
	}
	capacity, err := quota.New(state.Database()).ReservedCapacity(ctx)
	if err != nil || capacity.Bytes != 4096 {
		t.Fatal("missing port retired responsibility", capacity, err)
	}
}
