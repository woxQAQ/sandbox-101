package engine

import (
	"context"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestReservationBeforePause(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	wait(t, e, api.Request{Key: "s", Action: "start", VMID: a.VMID})
	free, err := freeBytes(e.opts.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	e.opts.SafetyBytes = free
	if _, err = e.Submit(
		context.Background(),
		api.Request{Key: "full", Action: "snapshot", VMID: a.VMID},
	); err == nil {
		t.Fatal("insufficient storage admitted")
	}
	b.mu.Lock()
	captures, resumes := b.captures, b.resumes
	b.mu.Unlock()
	if captures != 0 || resumes != 0 {
		t.Fatal("reservation rejected after pausing")
	}
	e.opts.SafetyBytes = 1
	s := wait(
		t,
		e,
		api.Request{Key: "budgeted", Action: "snapshot", VMID: a.VMID},
	)
	var bytes int64
	if err = e.db.QueryRow("SELECT bytes FROM reservations WHERE owner=?", s.SnapshotID).
		Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	shape := config().Shape
	minimum := (2*shape.MemoryMiB + shape.DiskMiB + 160) << 20
	if bytes < minimum {
		t.Fatalf(
			"reservation %d cannot cover two RAM copies, disk and bounded JSON %d",
			bytes,
			minimum,
		)
	}
}
