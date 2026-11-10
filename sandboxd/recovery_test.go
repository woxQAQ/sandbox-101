package sandboxd

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func seedRecoveryVM(
	t *testing.T,
	s *Service,
	f *recoveryTestEngine,
	action, step, vmid string,
) Operation {
	t.Helper()
	o := Operation{
		ID:        newID("op_"),
		Key:       newID("test_"),
		TenantID:  "alice",
		Command:   Command{Action: action},
		Status:    "failed",
		CreatedAt: time.Now().UTC(),
	}
	o.hash = s.store.sign(o.ID)
	if action == "fork" {
		o.IDs = []string{strings.TrimPrefix(step, "fork-clone:")}
	}
	if action == "template_build" {
		o.Step = "warmup_inflight"
		o.Command.Template = &Template{
			ID:     "never-replay",
			Warmup: "unknown-guest-command",
		}
	}
	if err := saveOp(s.ctx, s.store.db, o); err != nil {
		t.Fatal(err)
	}
	requestAction := "clone"
	if action == "template_build" || action == "claim" {
		requestAction = "create"
	}
	key := stepKey(o, step)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops[key] = api.Operation{
		ID:      key,
		Key:     key,
		Request: api.Request{Key: key, Action: requestAction},
		Status:  "succeeded",
		VMID:    vmid,
	}
	f.vms[vmid] = api.VM{
		ID:         vmid,
		Instance:   "instance-" + vmid,
		Phase:      "running",
		Verified:   true,
		AgentReady: true,
		Config: api.Config{
			Labels: map[string]string{
				"sandboxd":     "build",
				"operation_id": "inherited-old-build",
			},
		},
	}
	return o
}

func TestRecoverResourcesCollectsOnlyProvenUnownedVMs(t *testing.T) {
	s, f, _, _ := recoveryFixture(t)
	for _, test := range []struct{ action, step, vm string }{
		{"template_build", "build-create", "orphan-build"},
		{"refill", "warm-clone", "orphan-refill"},
		{"claim", "provision", "orphan-provision"},
		{"fork", "fork-clone:branch", "orphan-fork"},
	} {
		seedRecoveryVM(t, s, f, test.action, test.step, test.vm)
	}
	seedRecoveryVM(t, s, f, "refill", "warm-clone", "warm")
	if _, err := s.store.db.ExecContext(
		s.ctx,
		"INSERT INTO warm(vm_id,template_id,snapshot_id,created_at) VALUES(?,?,?,?)",
		"warm",
		"python",
		"shared",
		1,
	); err != nil {
		t.Fatal(err)
	}
	seedRecoveryVM(t, s, f, "refill", "warm-clone", "claimed")
	live := Sandbox{
		ID:       "sbx_live",
		TenantID: "alice",
		State:    "running",
		VMID:     "claimed",
		Instance: "instance-claimed",
		Deadline: time.Now().Add(time.Hour),
		Policy:   egress.Policy{Mode: "none"},
	}
	if err := saveSandbox(s.ctx, s.store.db, live); err != nil {
		t.Fatal(err)
	}
	pending := seedRecoveryVM(t, s, f, "refill", "warm-clone", "pending")
	pending.Status = "running"
	if err := saveOp(s.ctx, s.store.db, pending); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.vms["label-only"] = api.VM{
		ID:       "label-only",
		Verified: true,
		Config:   api.Config{Labels: map[string]string{"sandboxd": "build"}},
	}
	f.mu.Unlock()
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"orphan-build", "orphan-refill", "orphan-provision", "orphan-fork"} {
		if _, err := f.GetVM(s.ctx, id); !isEngineCode(err, api.NotFound) {
			t.Fatalf("proven orphan %s was not collected: %v", id, err)
		}
	}
	for _, id := range []string{"warm", "claimed", "pending", "label-only"} {
		if _, err := f.GetVM(s.ctx, id); err != nil {
			t.Fatalf("protected or unproven VM %s was deleted: %v", id, err)
		}
	}
	if f.count("delete") != 4 || f.count("clone") != 0 ||
		f.count("create") != 0 {
		t.Fatalf(
			"recovery replayed an allocation or missed cleanup: %+v",
			f.calls,
		)
	}
}

func TestRecoverResourcesRetainsPendingOperationSourceSnapshot(t *testing.T) {
	s, f, _, _ := recoveryFixture(t)
	o := Operation{
		ID:       "capture-owner",
		Key:      "capture-owner",
		TenantID: "alice",
		Command:  Command{Action: "checkpoint"},
		Status:   "failed",
		hash:     s.store.sign("capture-owner"),
	}
	if err := saveOp(s.ctx, s.store.db, o); err != nil {
		t.Fatal(err)
	}
	key := stepKey(o, "checkpoint-capture")
	f.mu.Lock()
	f.snaps["source"] = api.Snapshot{ID: "source", Published: true}
	f.ops[key] = api.Operation{
		ID:         key,
		Key:        key,
		Status:     "succeeded",
		SnapshotID: "source",
		Request:    api.Request{Action: "snapshot"},
	}
	f.mu.Unlock()
	fork := Operation{
		ID:         "pending-fork",
		Key:        "pending-fork",
		TenantID:   "alice",
		Command:    Command{Action: "fork"},
		Status:     "pending",
		SnapshotID: "source",
		hash:       s.store.sign("pending-fork"),
	}
	if err := saveOp(s.ctx, s.store.db, fork); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if f.count("delete_snapshot") != 0 {
		t.Fatal(
			"snapshot referenced only by operations.snapshot_id was collected",
		)
	}
	fork.Status = "failed"
	if err := saveOp(s.ctx, s.store.db, fork); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if f.count("delete_snapshot") != 1 {
		t.Fatal(
			"snapshot did not become collectible when its final dependency ended",
		)
	}
}

func TestRecoverResourcesProtectsInFlightEngineDependencyOfTerminalSandboxOperation(
	t *testing.T,
) {
	s, f, _, _ := recoveryFixture(t)
	source := seedRecoveryVM(
		t,
		s,
		f,
		"template_build",
		"build-create",
		"capturing",
	)
	key := stepKey(source, "build-snapshot")
	f.mu.Lock()
	f.ops[key] = api.Operation{
		ID:      key,
		Key:     key,
		Status:  "running",
		VMID:    "capturing",
		Request: api.Request{Action: "snapshot", VMID: "capturing"},
	}
	f.mu.Unlock()
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if f.count("delete") != 0 {
		t.Fatal("terminal sandbox status hid an in-flight engine dependency")
	}
	f.mu.Lock()
	op := f.ops[key]
	op.Status = "failed"
	f.ops[key] = op
	f.mu.Unlock()
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if f.count("delete") != 1 {
		t.Fatal("orphan was not collected after its engine dependency finished")
	}
}

func TestRecoverResourcesOrphanDeletionRetriesHaveDistinctDurableKeys(
	t *testing.T,
) {
	s, f, _, _ := recoveryFixture(t)
	seedRecoveryVM(t, s, f, "template_build", "build-create", "first")
	seedRecoveryVM(t, s, f, "template_build", "build-create", "second")
	f.failDeletes = 2
	if err := s.recoverResources(); err == nil {
		t.Fatal("expected terminal delete failures")
	}
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	f.control.Lock()
	defer f.control.Unlock()
	for _, r := range f.requests {
		if r.Action != "delete" {
			continue
		}
		if seen[r.Key] {
			t.Fatalf("terminal cleanup key was reused for retry: %s", r.Key)
		}
		seen[r.Key] = true
	}
	if len(seen) != 4 {
		t.Fatalf(
			"expected two independent cleanup attempts per orphan, got %d",
			len(seen),
		)
	}
}

func TestRecoverResourcesQueryFailuresPreserveResources(t *testing.T) {
	for _, failure := range []string{"vm-inventory", "snapshot-inventory", "operation-proof"} {
		t.Run(failure, func(t *testing.T) {
			s, f, _, _ := recoveryFixture(t)
			seedRecoveryVM(t, s, f, "template_build", "build-create", "orphan")
			switch failure {
			case "vm-inventory":
				f.unavailable = true
			case "snapshot-inventory":
				f.snapshotError = errors.New("snapshot inventory unavailable")
			case "operation-proof":
				f.findError = errors.New("operation query unavailable")
			}
			if err := s.recoverResources(); err == nil {
				t.Fatal("query failure was treated as an empty inventory")
			}
			if f.count("delete") != 0 || f.count("delete_snapshot") != 0 {
				t.Fatal("query failure triggered cleanup")
			}
		})
	}
}

func TestRecoverResourcesOwnershipTransferIsSerializedWithCollection(
	t *testing.T,
) {
	s, f, _, _ := recoveryFixture(t)
	seedRecoveryVM(t, s, f, "refill", "warm-clone", "transfer")
	if _, err := s.store.db.ExecContext(
		s.ctx,
		"INSERT INTO warm(vm_id,template_id,snapshot_id,created_at) VALUES(?,?,?,?)",
		"transfer",
		"python",
		"shared",
		1,
	); err != nil {
		t.Fatal(err)
	}
	ownership := s.lockResourceOwnership()
	if _, err := s.store.db.ExecContext(
		s.ctx,
		"DELETE FROM warm WHERE vm_id=?",
		"transfer",
	); err != nil {
		ownership()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.recoverResources() }()
	select {
	case err := <-done:
		ownership()
		t.Fatalf("GC crossed an unfinished ownership transfer: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	b := Sandbox{
		ID:       "sbx_transfer",
		TenantID: "alice",
		State:    "running",
		VMID:     "transfer",
		Instance: "instance-transfer",
		Deadline: time.Now().Add(time.Hour),
	}
	err := saveSandbox(s.ctx, s.store.db, b)
	ownership()
	if err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if f.count("delete") != 0 {
		t.Fatal(
			"GC deleted the VM between warm pop and sandbox ownership publication",
		)
	}
	if _, err = f.GetVM(s.ctx, "transfer"); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverResourcesHoldsOwnershipUntilEngineDeletionCompletes(
	t *testing.T,
) {
	s, f, _, _ := recoveryFixture(t)
	seedRecoveryVM(t, s, f, "template_build", "build-create", "orphan")
	gate := make(chan struct{})
	f.deleteGate, f.deleteEntered = gate, make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- s.recoverResources() }()
	select {
	case <-f.deleteEntered:
	case <-time.After(time.Second):
		close(gate)
		t.Fatal("orphan deletion did not start")
	}
	acquired := make(chan struct{})
	go func() {
		unlock := s.lockResourceOwnership()
		close(acquired)
		unlock()
	}()
	select {
	case <-acquired:
		close(gate)
		t.Fatal("ownership transition overlapped engine deletion")
	case <-time.After(30 * time.Millisecond):
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("ownership lock was not released after deletion")
	}
}
