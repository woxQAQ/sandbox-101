package sandboxd

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

func TestAutoResumeRetriesAfterActivationFailure(t *testing.T) {
	s, f, a, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	if _, err := s.Execute(
		context.Background(),
		p,
		"pause",
		Command{Action: "pause", SandboxID: id},
	); err != nil {
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
		_, err := s.Execute(
			context.Background(),
			p,
			"release",
			Command{Action: "release", SandboxID: r.Sandbox.ID},
		)
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

func TestConcurrentAutoResumeHasOneCloneAndPreservesIdentity(t *testing.T) {
	s, f, a, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	if _, err := s.Execute(
		context.Background(),
		p,
		"pause",
		Command{Action: "pause", SandboxID: id},
	); err != nil {
		t.Fatal(err)
	}
	before := f.count("clone")
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Go(
			func() { ; _, err := s.DataReady(context.Background(), p, id); errs <- err },
		)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if f.count("clone")-before != 1 {
		t.Fatal("concurrent wake cloned more than once")
	}
	a.mu.Lock()
	last := a.activations[len(a.activations)-1]
	a.mu.Unlock()
	if last.ResetIdentity {
		t.Fatal("resume reset guest identity")
	}
}
