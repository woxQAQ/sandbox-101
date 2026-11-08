package engine

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestCloneRejectsLegacyPublication(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	vm := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	wait(t, e, api.Request{Key: "start", Action: "start", VMID: vm.VMID})
	op := wait(
		t,
		e,
		api.Request{Key: "snapshot", Action: "snapshot", VMID: vm.VMID},
	)
	lock := e.vmLock(vm.VMID)
	lock.Lock()
	s, err := e.loadSnapshot(context.Background(), op.SnapshotID)
	lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err = json.Unmarshal(s.Snapshot.Manifest, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.PublicationMode = ""
	s.Snapshot.Manifest = encode(manifest)
	if err = e.saveSnapshot(s); err != nil {
		t.Fatal(err)
	}
	clone, err := e.Submit(
		context.Background(),
		api.Request{
			Key:        "legacy-clone",
			Action:     "clone",
			SnapshotID: op.SnapshotID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	final := awaitStatus(t, e, clone.ID)
	if final.Status != "failed" || final.Error == nil ||
		final.Error.Code != api.Incompatible {
		t.Fatalf("legacy publication trusted: %+v", final)
	}
	b.mu.Lock()
	launches := b.launches
	b.mu.Unlock()
	if launches != 1 {
		t.Fatal("legacy publication launched another VMM")
	}
	if err = verifyFiles(s.Dir, manifest.Files); err != nil {
		t.Fatal("legacy evidence altered", err)
	}
}

func TestCloneRecoveryAfterBackingTransferAndSnapshotDeletion(t *testing.T) {
	dir := t.TempDir()
	b := newTestBackend()
	var hit atomic.Bool
	e := openTest(t, dir, b, true, func(s string) error {
		if s == "restore_resources_ready" && hit.CompareAndSwap(false, true) {
			return ErrInterrupted
		}
		return nil
	})
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	wait(t, e, api.Request{Key: "boot", Action: "start", VMID: a.VMID})
	s := wait(t, e, api.Request{Key: "snap", Action: "snapshot", VMID: a.VMID})
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "clone", Action: "clone", SnapshotID: s.SnapshotID},
	)
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, op.ID, "restore_resources_ready")
	wait(
		t,
		e,
		api.Request{
			Key:        "del-snapshot",
			Action:     "delete_snapshot",
			SnapshotID: s.SnapshotID,
		},
	)
	e.Close()
	e = openTest(t, dir, b, false, nil)
	defer e.Close()
	final := awaitStatus(t, e, op.ID)
	if final.Status != "succeeded" {
		t.Fatalf("clone still depends on deleted source snapshot %+v", final)
	}
}

func TestRestoreUsesFreshInstanceAndDefaultNetworkBlock(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	wait(t, e, api.Request{Key: "s", Action: "start", VMID: a.VMID})
	before, _ := e.GetVM(context.Background(), a.VMID)
	s := wait(t, e, api.Request{Key: "snap", Action: "snapshot", VMID: a.VMID})
	wait(t, e, api.Request{Key: "off", Action: "stop", VMID: a.VMID})
	wait(
		t,
		e,
		api.Request{
			Key:        "restore",
			Action:     "restore",
			VMID:       a.VMID,
			SnapshotID: s.SnapshotID,
		},
	)
	after, _ := e.GetVM(context.Background(), a.VMID)
	if after.Instance == before.Instance || after.ID != before.ID ||
		after.NetworkActual ||
		after.NetworkEnabled ||
		!after.AgentReady {
		t.Fatalf("bad restore identity %+v", after)
	}
}
