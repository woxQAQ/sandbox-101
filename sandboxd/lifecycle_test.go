package sandboxd

import (
	"context"
	"testing"
)

func TestPauseFailurePreservesVMAndZeroWakeRelease(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	f.mu.Lock()
	f.failSnapshot = true
	f.mu.Unlock()
	_, err := s.Execute(
		context.Background(),
		p,
		"pause-fails",
		Command{Action: "pause", SandboxID: id},
	)
	if !isCode(err, ErrExhausted) {
		t.Fatalf("expected exhaustion: %v", err)
	}
	b, _ := s.Get(context.Background(), p, id)
	if b.State != "running" || f.count("delete") != 0 {
		t.Fatal("failed hibernate destroyed state")
	}
	f.mu.Lock()
	f.failSnapshot = false
	f.mu.Unlock()
	if _, err = s.Execute(
		context.Background(),
		p,
		"pause",
		Command{Action: "pause", SandboxID: id},
	); err != nil {
		t.Fatal(err)
	}
	b, _ = s.Get(context.Background(), p, id)
	if b.State != "paused" || b.VMID != "" || b.SnapshotID == "" {
		t.Fatal("pause did not free VM")
	}
	before := f.count("clone")
	if _, err = s.Execute(
		context.Background(),
		p,
		"release",
		Command{Action: "release", SandboxID: id},
	); err != nil {
		t.Fatal(err)
	}
	if f.count("clone") != before || f.count("delete_snapshot") != 1 {
		t.Fatal("paused release woke source or leaked snapshot")
	}
}
