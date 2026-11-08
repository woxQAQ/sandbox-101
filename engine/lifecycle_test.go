package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestStartupAdoptsLiveInstanceWithoutRelaunch(t *testing.T) {
	dir := t.TempDir()
	b := newTestBackend()
	var hit atomic.Bool
	e := openTest(t, dir, b, true, func(step string) error {
		if step == "launched" && hit.CompareAndSwap(false, true) {
			return ErrInterrupted
		}
		return nil
	})
	a := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	o, err := e.Submit(
		context.Background(),
		api.Request{Key: "start", Action: "start", VMID: a.VMID},
	)
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, o.ID, "launched")
	e.Close()
	e = openTest(t, dir, b, false, nil)
	defer e.Close()
	op := awaitStatus(t, e, o.ID)
	if op.Status != "succeeded" {
		t.Fatalf("adoption failed %+v", op)
	}
	b.mu.Lock()
	launches := b.launches
	b.mu.Unlock()
	if launches != 1 {
		t.Fatalf("recovery relaunched VMM %d times", launches)
	}
}

func TestAgentFailureStopsColdBoot(t *testing.T) {
	b := newTestBackend()
	b.agentError = errors.New("guest agent unavailable")
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "s", Action: "start", VMID: a.VMID},
	)
	if err != nil {
		t.Fatal(err)
	}
	op = awaitStatus(t, e, op.ID)
	if op.Status != "failed" {
		t.Fatal("agent failure accepted")
	}
	v, _ := e.GetVM(context.Background(), a.VMID)
	if v.Phase != "stopped" || v.PID != 0 || v.AgentReady {
		t.Fatalf("unverified cold boot still running %+v", v)
	}
}

func TestUnknownIdentityNeverStopsOrDeletes(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	wait(t, e, api.Request{Key: "s", Action: "start", VMID: a.VMID})
	l := e.vmLock(a.VMID)
	l.Lock()
	l.Unlock() //nolint:staticcheck // Synchronize with terminal publication before inspecting resources.
	b.mu.Lock()
	b.inspectError = api.Fail(api.ManualRecovery, "PID reused")
	b.mu.Unlock()
	_ = e.Reconcile(context.Background())
	v, _ := e.GetVM(context.Background(), a.VMID)
	if v.Verified || v.BlockedReason == "" {
		t.Fatal("unknown identity treated as confirmed")
	}
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "delete", Action: "delete", VMID: a.VMID},
	)
	if err != nil {
		t.Fatal(err)
	}
	final := awaitStatus(t, e, op.ID)
	if final.Status != "failed" {
		t.Fatal("unconfirmed resource deleted")
	}
	if _, err = e.GetVM(context.Background(), a.VMID); err != nil {
		t.Fatal("ownership record removed despite uncertainty")
	}
}
