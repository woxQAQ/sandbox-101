package sandboxd

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func failedRecoveryOperation(
	t *testing.T,
	s *Service,
	action string,
	b *Sandbox,
) Operation {
	t.Helper()
	o := Operation{
		ID:             newID("op_"),
		Key:            newID("test_"),
		TenantID:       b.TenantID,
		ChargeTenantID: b.TenantID,
		Command:        Command{Action: action, SandboxID: b.ID},
		IDs:            []string{b.ID},
		Status:         "failed",
		Error: &Error{
			Code:    ErrInternal,
			Message: "interrupted cleanup",
		},
		CreatedAt: time.Now().UTC(),
	}
	o.hash = s.store.sign(o.ID)
	if action == "resume" {
		if err := reserveQuotaStep(
			s.ctx,
			s.store.db,
			o,
			"resume-clone",
			b.ID,
			b.Shape.DiskMiB,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveOp(s.ctx, s.store.db, o); err != nil {
		t.Fatal(err)
	}
	b.OperationID = o.ID
	if err := saveSandbox(s.ctx, s.store.db, *b); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestResumeCleanupFailureDoesNotReuseOldCapture(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	execute := func(key, action string) {
		t.Helper()
		_, err := s.Execute(
			context.Background(),
			p,
			key,
			Command{Action: action, SandboxID: b.ID},
		)
		if err != nil && action != "resume" {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	f.generation[b.VMID] = 1
	f.mu.Unlock()
	execute("pause1", "pause")
	f.control.Lock()
	f.failSnapshotDeletes = 100
	f.control.Unlock()
	execute("resume1", "resume")
	b = recoverySandbox(t, s, b.ID)
	if b.State != "running" {
		t.Fatal(b)
	}
	f.mu.Lock()
	f.generation[b.VMID] = 2
	f.mu.Unlock()
	execute("pause2", "pause")
	execute("resume2", "resume")
	b = recoverySandbox(t, s, b.ID)
	f.mu.Lock()
	generation := f.generation[b.VMID]
	f.mu.Unlock()
	if generation != 2 {
		t.Fatalf("second pause reused old capture: generation=%d", generation)
	}
}

func TestResumeHandoffIsAtomicAcrossRestart(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	if _, err := s.Execute(
		s.ctx,
		p,
		"pause",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	before := recoverySandbox(t, s, b.ID)
	if _, err := s.store.db.Exec(
		`CREATE TRIGGER reject_running BEFORE UPDATE ON sandboxes WHEN NEW.state='running' BEGIN SELECT RAISE(ABORT,'handoff interrupted'); END`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Execute(
		s.ctx,
		p,
		"resume-failed",
		Command{Action: "resume", SandboxID: b.ID},
	); err == nil {
		t.Fatal("transaction failure hidden")
	}
	if got := recoverySandbox(t, s, b.ID); got.SnapshotID != before.SnapshotID {
		t.Fatal("lost precommit reference", got)
	}
	intents, err := s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range intents {
		if r.Kind == "retired_snapshot" {
			t.Fatal("uncommitted handoff escaped", r)
		}
	}
	if _, err = s.store.db.Exec(`DROP TRIGGER reject_running`); err != nil {
		t.Fatal(err)
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
	if got := recoverySandbox(
		t,
		reopened,
		b.ID,
	); got.SnapshotID != before.SnapshotID {
		t.Fatal(got)
	}
	f.control.Lock()
	f.failSnapshotDeletes = 100
	f.control.Unlock()
	if _, err = reopened.Execute(
		reopened.ctx,
		p,
		"resume",
		Command{Action: "resume", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	if got := recoverySandbox(
		t,
		reopened,
		b.ID,
	); got.State != "running" ||
		got.SnapshotID != "" {
		t.Fatal(got)
	}
	opts = reopened.opts
	opts.Initialize = false
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { again.Close() })
	intents, err = again.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range intents {
		if r.Kind == "retired_snapshot" && r.Snapshot == before.SnapshotID &&
			!r.Done &&
			r.TenantID == p.TenantID &&
			r.MemoryMiB == 128 {
			found = true
		}
	}
	if !found {
		t.Fatal("postcommit charge missing", intents)
	}
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
	); !isEngineCode(
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
	if err := s.recoverResources(); !isCode(err, ErrExhausted) {
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
		Operation{ID: "capture", TenantID: p.TenantID},
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
	captured, err := s.Execute(
		s.ctx,
		p,
		"checkpoint",
		Command{Action: "checkpoint", SandboxID: b.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	cp := *captured.Checkpoint
	b.State = "resuming"
	source := failedRecoveryOperation(t, s, "rollback", &b)
	source.Command.CheckpointID = cp.ID
	source.SourceInstance = b.Instance
	source.Step = "rollback_restore"
	if err := saveOp(s.ctx, s.store.db, source); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverResources(); !isCode(err, ErrManualRecovery) {
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
	if err := s.recoverResources(); !isCode(err, ErrManualRecovery) {
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

func TestRecoverResourcesRollbackRetriesOnlyProvenStoppedSource(t *testing.T) {
	s, f, a, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "claim", nil).Sandbox
	capture, err := s.engine(
		Operation{ID: "checkpoint", TenantID: p.TenantID},
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
	if err = quotaSnapshot(
		s.ctx,
		s.store.db,
		Operation{ID: "checkpoint", ChargeTenantID: b.TenantID},
		cp.SnapshotID,
		cp.Shape,
		quotaRef{Kind: "active_use", ID: "checkpoint:" + cp.ID},
	); err != nil {
		t.Fatal(err)
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
