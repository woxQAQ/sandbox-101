package sandboxd

import (
	"context"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestPausedCheckpointRetainsCaptureWithoutWaking(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	paused, err := s.Execute(
		context.Background(),
		p,
		"pause",
		Command{Action: "pause", SandboxID: id},
	)
	if err != nil {
		t.Fatal(err)
	}
	beforeClone, beforeCapture := f.count("clone"), f.count("snapshot")
	cp, err := s.Execute(
		context.Background(),
		p,
		"checkpoint",
		Command{Action: "checkpoint", SandboxID: id},
	)
	if err != nil {
		t.Fatal(err)
	}
	if cp.Checkpoint.SnapshotID != paused.Sandbox.SnapshotID ||
		f.count("clone") != beforeClone ||
		f.count("snapshot") != beforeCapture {
		t.Fatal("sleeping checkpoint woke or recaptured source")
	}
	if _, err = s.Execute(
		context.Background(),
		p,
		"release",
		Command{Action: "release", SandboxID: id},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = f.GetSnapshot(
		context.Background(),
		cp.Checkpoint.SnapshotID,
	); err != nil {
		t.Fatal("release removed checkpoint backing", err)
	}
	if _, err = s.Execute(
		context.Background(),
		p,
		"delete-cp",
		Command{Action: "delete_checkpoint", CheckpointID: cp.Checkpoint.ID},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = f.GetSnapshot(
		context.Background(),
		cp.Checkpoint.SnapshotID,
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("last reference did not retire snapshot", err)
	}
}

func TestRollbackReplayDoesNotStopRestoredInstance(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	cp, err := s.Execute(
		context.Background(),
		p,
		"checkpoint",
		Command{Action: "checkpoint", SandboxID: id},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Execute(
		context.Background(),
		p,
		"rollback",
		Command{
			Action:       "rollback",
			SandboxID:    id,
			CheckpointID: cp.Checkpoint.ID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	op, err := s.store.byKey(context.Background(), p.TenantID, "rollback")
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after activation committed but before the operation terminal record.
	op.Status = "running"
	if err = saveOp(s.ctx, s.store.db, op); err != nil {
		t.Fatal(err)
	}
	s.run(op)
	replay, err := s.Get(context.Background(), p, id)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Instance != result.Sandbox.Instance || f.count("stop") != 1 ||
		f.count("restore") != 1 {
		t.Fatal("recovery stopped or restored a second instance")
	}
	op, err = s.store.operation(context.Background(), op.ID)
	if err != nil || op.Status != "succeeded" {
		t.Fatalf("rollback replay failed: %+v %v", op, err)
	}
}
