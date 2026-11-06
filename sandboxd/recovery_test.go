package sandboxd

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

type recoveryTestEngine struct {
	*fakeEngine

	control       sync.Mutex
	failDeletes   int
	snapshotError error
	findError     error
	requests      []api.Request
	deleteEntered chan struct{}
	deleteGate    <-chan struct{}
}

func (f *recoveryTestEngine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	f.control.Lock()
	f.requests = append(f.requests, r)
	fail := r.Action == "delete" && f.failDeletes > 0
	if fail {
		f.failDeletes--
	}
	entered, gate := f.deleteEntered, f.deleteGate
	f.control.Unlock()
	if r.Action == "delete" && entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return api.Operation{}, ctx.Err()
		case <-gate:
		}
	}
	if fail {
		f.mu.Lock()
		defer f.mu.Unlock()
		if old, ok := f.ops[r.Key]; ok {
			return old, nil
		}
		o := api.Operation{
			ID:      r.Key,
			Key:     r.Key,
			Request: r,
			VMID:    r.VMID,
			Status:  "failed",
			Error: &api.Error{
				Code:    api.Internal,
				Message: "temporary delete failure",
			},
		}
		f.ops[r.Key] = o
		f.calls[r.Action]++
		return o, nil
	}
	if r.Action == "stop" || r.Action == "restore" {
		f.mu.Lock()
		defer f.mu.Unlock()
		if old, ok := f.ops[r.Key]; ok {
			return old, nil
		}
		v, ok := f.vms[r.VMID]
		if !ok {
			return api.Operation{}, api.Fail(api.NotFound, "not found")
		}
		if r.ExpectedInstance != "" && v.Instance != r.ExpectedInstance {
			return api.Operation{}, api.Fail(api.Conflict, "instance mismatch")
		}
		f.calls[r.Action]++
		if r.Action == "stop" {
			v.Phase, v.AgentReady = "stopped", false
		} else {
			v.Phase, v.AgentReady, v.Instance = "running", true, newID("run_")
		}
		v.NetworkActual, v.NetworkEnabled = false, false
		f.vms[v.ID] = v
		o := api.Operation{
			ID:         r.Key,
			Key:        r.Key,
			Request:    r,
			VMID:       v.ID,
			Status:     "succeeded",
			SnapshotID: r.SnapshotID,
		}
		f.ops[r.Key] = o
		return o, nil
	}
	return f.fakeEngine.Submit(ctx, r)
}

func (f *recoveryTestEngine) ListSnapshots(
	ctx context.Context,
) ([]api.Snapshot, error) {
	f.control.Lock()
	err := f.snapshotError
	f.control.Unlock()
	if err != nil {
		return nil, err
	}
	return f.fakeEngine.ListSnapshots(ctx)
}

func (f *recoveryTestEngine) FindOperation(
	ctx context.Context,
	key string,
) (api.Operation, error) {
	f.control.Lock()
	err := f.findError
	f.control.Unlock()
	if err != nil {
		return api.Operation{}, err
	}
	return f.fakeEngine.FindOperation(ctx, key)
}

func recoveryFixture(
	t *testing.T,
) (*Service, *recoveryTestEngine, *fakeAgent, Principal) {
	t.Helper()
	s, engine, agent, p := fixture(t)
	// Keep the existing fixture, database and lifecycle implementation, but drive
	// reconciliation explicitly so fault injection has no timing dependency.
	s.cancel()
	s.wg.Wait()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	f := &recoveryTestEngine{fakeEngine: engine}
	s.opts.Engine = f
	return s, f, agent, p
}

func failedRecoveryOperation(
	t *testing.T,
	s *Service,
	action string,
	b *Sandbox,
) Operation {
	t.Helper()
	o := Operation{
		ID:       newID("op_"),
		Key:      newID("test_"),
		TenantID: b.TenantID,
		Command:  Command{Action: action, SandboxID: b.ID},
		IDs:      []string{b.ID},
		Status:   "failed",
		Error: &api.Error{
			Code:    api.Internal,
			Message: "interrupted cleanup",
		},
		CreatedAt: time.Now().UTC(),
	}
	o.Hash = s.store.sign(o.ID)
	if err := saveOp(s.ctx, s.store.db, o); err != nil {
		t.Fatal(err)
	}
	b.OperationID = o.ID
	if err := saveSandbox(s.ctx, s.store.db, *b); err != nil {
		t.Fatal(err)
	}
	return o
}

func recoverySandbox(t *testing.T, s *Service, id string) Sandbox {
	t.Helper()
	b, err := getSandbox(s.ctx, s.store.db, id)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRecoverResourcesFailedProvisionCleanupRetriesDurably(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "claim", nil).Sandbox
	vmid := b.VMID
	b.State = "provisioning"
	source := failedRecoveryOperation(t, s, "claim", &b)
	f.failDeletes = 1
	if err := s.recoverResources(); err == nil {
		t.Fatal("delete failure was hidden")
	}
	intents, err := s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	var retry resourceRecovery
	for _, r := range intents {
		if r.Kind == "sandbox" {
			retry = r
		}
	}
	if retry.Attempt != 1 || retry.Done {
		t.Fatalf(
			"terminal delete failure did not persist a new attempt: %+v",
			retry,
		)
	}
	opts := s.opts
	opts.Initialize = false
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if err = reopened.recoverResources(); err != nil {
		t.Fatal(err)
	}
	b = recoverySandbox(t, reopened, b.ID)
	if b.State != "terminated" || b.VMID != "" || b.OperationID != "" {
		t.Fatalf("provisioning cleanup did not converge: %+v", b)
	}
	if _, err = f.GetVM(
		context.Background(),
		vmid,
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatalf("failed provisioning VM remains: %v", err)
	}
	old, err := reopened.store.operation(context.Background(), source.ID)
	if err != nil || old.Status != "failed" {
		t.Fatalf("client operation was replayed or changed: %+v %v", old, err)
	}
}

func TestRecoverResourcesFailedHibernateRetainsVMBeforeCapture(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "claim", nil).Sandbox
	b.State = "pausing"
	failedRecoveryOperation(t, s, "pause", &b)
	f.failSnapshot = true
	if err := s.recoverResources(); !isCode(err, api.Exhausted) {
		t.Fatalf("expected failed capture: %v", err)
	}
	b = recoverySandbox(t, s, b.ID)
	if b.State != "running" || b.VMID == "" || f.count("delete") != 0 {
		t.Fatalf("capture failure discarded state: %+v", b)
	}
	if _, err := f.GetVM(s.ctx, b.VMID); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverResourcesHibernateDeleteRetryReusesPublishedCapture(
	t *testing.T,
) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "claim", nil).Sandbox
	b.State = "pausing"
	failedRecoveryOperation(t, s, "pause", &b)
	f.failDeletes = 1
	if err := s.recoverResources(); err == nil {
		t.Fatal("expected failed deletion")
	}
	b = recoverySandbox(t, s, b.ID)
	capture := b.SnapshotID
	if b.State != "pausing" || capture == "" || b.VMID == "" {
		t.Fatalf("capture was not retained before deletion: %+v", b)
	}
	// Delete can stop the VMM and then fail while removing disk resources.
	f.mu.Lock()
	stopped := f.vms[b.VMID]
	stopped.Phase, stopped.AgentReady = "stopped", false
	f.vms[b.VMID] = stopped
	f.mu.Unlock()
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	b = recoverySandbox(t, s, b.ID)
	if b.State != "paused" || b.VMID != "" || b.SnapshotID != capture ||
		f.count("snapshot") != 1 {
		t.Fatalf(
			"hibernate retry repeated capture or failed to converge: %+v",
			b,
		)
	}
}

func TestRecoverResourcesCompletesHibernateAfterVMAlreadyDeleted(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "claim", nil).Sandbox
	capture, err := s.engine(
		Operation{ID: "capture"},
		"capture",
		api.Request{
			Action:           "snapshot",
			VMID:             b.VMID,
			ExpectedInstance: b.Instance,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	b.State, b.SnapshotID = "pausing", capture.SnapshotID
	failedRecoveryOperation(t, s, "pause", &b)
	f.mu.Lock()
	delete(f.vms, b.VMID)
	f.mu.Unlock()
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	b = recoverySandbox(t, s, b.ID)
	if b.State != "paused" || b.VMID != "" || b.PausedUntil.IsZero() ||
		f.count("clone") != 0 {
		t.Fatalf("lost terminal hibernate commit was not recovered: %+v", b)
	}
}

func TestRecoverResourcesFailedResumeOnlyDeletesItsClone(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "claim", nil).Sandbox
	if _, err := s.Execute(
		s.ctx,
		p,
		"pause",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	b = recoverySandbox(t, s, b.ID)
	capture := b.SnapshotID
	b.State = "resuming"
	source := failedRecoveryOperation(t, s, "resume", &b)
	clone, err := s.engine(
		source,
		"resume-clone",
		api.Request{Action: "clone", SnapshotID: capture},
	)
	if err != nil {
		t.Fatal(err)
	}
	b.VMID = clone.VMID
	if err = saveSandbox(s.ctx, s.store.db, b); err != nil {
		t.Fatal(err)
	}
	before := f.count("clone")
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	b = recoverySandbox(t, s, b.ID)
	if b.State != "paused" || b.VMID != "" || b.SnapshotID != capture ||
		b.ResumeAttempt == 0 ||
		f.count("clone") != before {
		t.Fatalf("resume rollback failed: %+v", b)
	}
	if _, err = f.GetSnapshot(s.ctx, capture); err != nil {
		t.Fatalf("resume rollback deleted sleep capture: %v", err)
	}
}

func TestRecoverResourcesRollbackPreservesUnprovenRunningInstance(
	t *testing.T,
) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "claim", nil).Sandbox
	cp := Checkpoint{
		ID:         "cp",
		TenantID:   b.TenantID,
		SandboxID:  b.ID,
		SnapshotID: "checkpoint",
		Shape:      b.Shape,
	}
	f.snaps[cp.SnapshotID] = api.Snapshot{ID: cp.SnapshotID, Published: true}
	if _, err := s.store.db.ExecContext(
		s.ctx,
		"INSERT INTO checkpoints(id,tenant,body) VALUES(?,?,?)",
		cp.ID,
		cp.TenantID,
		encode(cp),
	); err != nil {
		t.Fatal(err)
	}
	b.State = "resuming"
	source := failedRecoveryOperation(t, s, "rollback", &b)
	source.Command.CheckpointID = cp.ID
	source.SourceInstance = b.Instance
	source.Step = "rollback_restore"
	if err := saveOp(s.ctx, s.store.db, source); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverResources(); !isCode(err, api.ManualRecovery) {
		t.Fatalf("unproven rollback should require recovery: %v", err)
	}
	if f.count("delete") != 0 || f.count("stop") != 0 ||
		f.count("restore") != 0 ||
		f.count("clone") != 0 {
		t.Fatal("rollback target was destroyed or restarted without proof")
	}
	if got := recoverySandbox(
		t,
		s,
		b.ID,
	); got.State != "resuming" ||
		got.VMID != b.VMID {
		t.Fatalf("unproven rollback changed ownership: %+v", got)
	}
}

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
	o.Hash = s.store.sign(o.ID)
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
		if _, err := f.GetVM(s.ctx, id); !isCode(err, api.NotFound) {
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
		Hash:     s.store.sign("capture-owner"),
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
		Hash:       s.store.sign("pending-fork"),
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

func TestRecoverResourcesProtectsInFlightEngineDependencyOfTerminalProductOperation(
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
		t.Fatal("terminal product status hid an in-flight engine dependency")
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

func TestRecoverResourcesUnknownResumeVMIsPreserved(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "claim", nil).Sandbox
	if _, err := s.Execute(
		s.ctx,
		p,
		"pause",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	b = recoverySandbox(t, s, b.ID)
	b.State, b.VMID, b.Instance = "resuming", "unproven", "unproven-instance"
	failedRecoveryOperation(t, s, "resume", &b)
	f.mu.Lock()
	f.vms[b.VMID] = api.VM{
		ID:         b.VMID,
		Instance:   b.Instance,
		Phase:      "running",
		Verified:   true,
		AgentReady: true,
	}
	f.mu.Unlock()
	before := f.count("delete")
	if err := s.recoverResources(); !isCode(err, api.ManualRecovery) {
		t.Fatalf(
			"resume without clone receipt should preserve unknown VM: %v",
			err,
		)
	}
	if f.count("delete") != before {
		t.Fatal("resume cleanup deleted an unproven VM")
	}
	got := recoverySandbox(t, s, b.ID)
	if got.State != "resuming" || got.SnapshotID != b.SnapshotID ||
		got.VMID != b.VMID {
		t.Fatalf("unknown resume resource was discarded: %+v", got)
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

func TestRecoverResourcesRollbackRetriesOnlyProvenStoppedSource(t *testing.T) {
	s, f, a, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "claim", nil).Sandbox
	capture, err := s.engine(
		Operation{ID: "checkpoint"},
		"capture",
		api.Request{
			Action:           "snapshot",
			VMID:             b.VMID,
			ExpectedInstance: b.Instance,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	cp := Checkpoint{
		ID:         "cp",
		TenantID:   b.TenantID,
		SandboxID:  b.ID,
		SnapshotID: capture.SnapshotID,
		Shape:      b.Shape,
	}
	if _, err = s.store.db.ExecContext(
		s.ctx,
		"INSERT INTO checkpoints(id,tenant,body) VALUES(?,?,?)",
		cp.ID,
		cp.TenantID,
		encode(cp),
	); err != nil {
		t.Fatal(err)
	}
	b.State = "resuming"
	source := failedRecoveryOperation(t, s, "rollback", &b)
	source.Command.CheckpointID, source.SourceInstance, source.Step = cp.ID, b.Instance, "rollback_restore"
	if err = saveOp(s.ctx, s.store.db, source); err != nil {
		t.Fatal(err)
	}
	if _, err = s.engine(
		source,
		"rollback-stop",
		api.Request{Action: "stop", VMID: b.VMID, ExpectedInstance: b.Instance},
	); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	b = recoverySandbox(t, s, b.ID)
	if b.State != "running" || b.Instance == source.SourceInstance ||
		f.count("stop") != 1 ||
		f.count("restore") != 1 {
		t.Fatalf(
			"stopped rollback did not converge without repeating stop: %+v",
			b,
		)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.activations[len(a.activations)-1].ResetIdentity {
		t.Fatal("rollback recovery reset the sandbox identity")
	}
}
