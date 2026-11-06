package sandboxd

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestPausedCheckpointRetainsCaptureWithoutWaking(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	paused, err := s.Execute(context.Background(), p, "pause", Command{Action: "pause", SandboxID: id})
	if err != nil {
		t.Fatal(err)
	}
	beforeClone, beforeCapture := f.count("clone"), f.count("snapshot")
	cp, err := s.Execute(context.Background(), p, "checkpoint", Command{Action: "checkpoint", SandboxID: id})
	if err != nil {
		t.Fatal(err)
	}
	if cp.Checkpoint.SnapshotID != paused.Sandbox.SnapshotID || f.count("clone") != beforeClone || f.count("snapshot") != beforeCapture {
		t.Fatal("sleeping checkpoint woke or recaptured source")
	}
	if _, err = s.Execute(context.Background(), p, "release", Command{Action: "release", SandboxID: id}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.GetSnapshot(context.Background(), cp.Checkpoint.SnapshotID); err != nil {
		t.Fatal("release removed checkpoint backing", err)
	}
	if _, err = s.Execute(context.Background(), p, "delete-cp", Command{Action: "delete_checkpoint", CheckpointID: cp.Checkpoint.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.GetSnapshot(context.Background(), cp.Checkpoint.SnapshotID); !isCode(err, api.NotFound) {
		t.Fatal("last reference did not retire snapshot", err)
	}
}

func TestAutoResumeRetriesAfterActivationFailure(t *testing.T) {
	s, f, a, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	if _, err := s.Execute(context.Background(), p, "pause", Command{Action: "pause", SandboxID: id}); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	a.fail = true
	a.mu.Unlock()
	if _, err := s.DataReady(context.Background(), p, id); err == nil {
		t.Fatal("failed activation reported ready")
	}
	b, _ := s.Get(context.Background(), p, id)
	if b.State != "paused" || b.SnapshotID == "" || b.VMID != "" {
		t.Fatalf("failed wake lost source or leaked VM: %+v", b)
	}
	a.mu.Lock()
	a.fail = false
	a.mu.Unlock()
	if _, err := s.DataReady(context.Background(), p, id); err != nil {
		t.Fatal("new wake reused terminal failed intent", err)
	}
	if f.count("clone") != 2 {
		t.Fatal("expected one clone per resume attempt")
	}
}

func TestRollbackReplayDoesNotStopRestoredInstance(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	cp, err := s.Execute(context.Background(), p, "checkpoint", Command{Action: "checkpoint", SandboxID: id})
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Execute(context.Background(), p, "rollback", Command{Action: "rollback", SandboxID: id, CheckpointID: cp.Checkpoint.ID})
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
	if replay.Instance != result.Sandbox.Instance || f.count("stop") != 1 || f.count("restore") != 1 {
		t.Fatal("recovery stopped or restored a second instance")
	}
	op, err = s.store.operation(context.Background(), op.ID)
	if err != nil || op.Status != "succeeded" {
		t.Fatalf("rollback replay failed: %+v %v", op, err)
	}
}

func TestForkWithoutEnvironmentClearsSourceValues(t *testing.T) {
	s, _, a, p := fixture(t)
	r := claimTest(t, s, p, "claim", map[string]EnvVar{"SOURCE_ONLY": {Value: "old"}})
	branches, err := s.Execute(context.Background(), p, "fork", Command{Action: "fork", SandboxID: r.Sandbox.ID, Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	last := a.activations[len(a.activations)-1]
	if last.SandboxID != branches.Sandboxes[0].ID || len(last.Env) != 0 {
		t.Fatal("branch inherited source environment")
	}
}

func TestAuthenticatedForeignTenantCannotDiscoverObjects(t *testing.T) {
	s, _, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	if _, err := s.PutTenant(context.Background(), Principal{Root: true}, Tenant{ID: "bob", Token: "bob-token", MaxSandboxes: 5, MaxStorageMiB: 4096}); err != nil {
		t.Fatal(err)
	}
	request := func(path string) *httptest.ResponseRecorder {
		q := httptest.NewRequest(http.MethodGet, path, nil)
		q.Header.Set("Authorization", "Bearer bob-token")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, q)
		return w
	}
	foreign, missing := request("/v1/sandboxes/"+r.Sandbox.ID), request("/v1/sandboxes/missing")
	if foreign.Code != 404 || missing.Code != 404 || foreign.Body.String() != missing.Body.String() {
		t.Fatal("authenticated tenant can distinguish foreign object")
	}
}

type closeCallbackConn struct {
	net.Conn
	callback func()
}

func (c *closeCallbackConn) Close() error { c.callback(); return c.Conn.Close() }

func TestReleaseClosesTrackedConnectionWithoutDeadlock(t *testing.T) {
	s, _, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	a, b := net.Pipe()
	defer b.Close()
	done := s.track(r.Sandbox.ID, a)
	wrapped := &closeCallbackConn{Conn: a, callback: done}
	// Preview connections remove themselves from tracking while closing.
	done()
	s.track(r.Sandbox.ID, wrapped)
	finished := make(chan error, 1)
	go func() {
		_, err := s.Execute(context.Background(), p, "release", Command{Action: "release", SandboxID: r.Sandbox.ID})
		finished <- err
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("connection cleanup deadlocked")
	}
}
