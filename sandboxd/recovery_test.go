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

type forkFailureAgent struct {
	*fakeAgent

	mu      sync.Mutex
	arrived int
	gate    chan struct{}
}

func (a *forkFailureAgent) Activate(
	ctx context.Context,
	vm api.VM,
	activation Activation,
) error {
	a.mu.Lock()
	a.arrived++
	n := a.arrived
	if n == 3 {
		close(a.gate)
	}
	a.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.gate:
	}
	if n == 3 {
		return errors.New("branch activation failed")
	}
	return a.fakeAgent.Activate(ctx, vm, activation)
}

func TestForkRollbackDeletesEveryBranch(t *testing.T) {
	s, f, a, p := recoveryFixture(t)
	source := claimTest(t, s, p, "source", nil)
	s.opts.Agent = &forkFailureAgent{fakeAgent: a, gate: make(chan struct{})}
	o, err := s.Submit(
		context.Background(),
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(context.Background(), p, o.ID); err == nil {
		t.Fatal("fork succeeded")
	}
	keys := map[string]bool{}
	for _, id := range o.IDs {
		b, err := getSandbox(context.Background(), s.store.db, id)
		if err != nil {
			t.Fatal(err)
		}
		if b.State != "terminated" || b.VMID != "" {
			t.Fatalf("branch leaked: %+v", b)
		}
	}
	f.control.Lock()
	for _, r := range f.requests {
		if r.Action == "delete" {
			if r.ExpectedInstance == "" {
				t.Fatal("missing instance")
			}
			if keys[r.Key] {
				t.Fatal("shared delete key")
			}
			keys[r.Key] = true
		}
	}
	f.control.Unlock()
	if len(keys) != 3 {
		t.Fatalf("deleted %d branches", len(keys))
	}
	if _, err = f.GetVM(context.Background(), source.Sandbox.VMID); err != nil {
		t.Fatal("source deleted", err)
	}
}

func TestForkRollbackRecoveryAfterDeleteFailure(t *testing.T) {
	s, f, a, p := recoveryFixture(t)
	source := claimTest(t, s, p, "source", nil)
	s.opts.Agent = &forkFailureAgent{fakeAgent: a, gate: make(chan struct{})}
	f.failDeletes = 1
	o, err := s.Submit(
		context.Background(),
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(context.Background(), p, o.ID); err == nil {
		t.Fatal("fork succeeded")
	}
	remaining := 0
	for _, id := range o.IDs {
		b := recoverySandbox(t, s, id)
		if b.State != "terminated" {
			t.Fatal(b)
		}
		if b.VMID != "" {
			remaining++
		}
	}
	if remaining != 1 {
		t.Fatalf("remaining=%d", remaining)
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
	for _, id := range o.IDs {
		b := recoverySandbox(t, reopened, id)
		if b.VMID != "" {
			t.Fatal("leaked", b)
		}
	}
	old, err := reopened.store.operation(context.Background(), o.ID)
	if err != nil || old.Status != "failed" {
		t.Fatal("terminal changed", old, err)
	}
	if _, err = f.GetVM(context.Background(), source.Sandbox.VMID); err != nil {
		t.Fatal(err)
	}
}

func TestForkRollbackPreservesUnknownInstance(t *testing.T) {
	s, f, a, p := recoveryFixture(t)
	source := claimTest(t, s, p, "source", nil)
	s.opts.Agent = &forkFailureAgent{fakeAgent: a, gate: make(chan struct{})}
	f.failDeletes = 1
	o, err := s.Submit(
		context.Background(),
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Wait(context.Background(), p, o.ID)
	var vmid string
	for _, id := range o.IDs {
		b := recoverySandbox(t, s, id)
		if b.VMID != "" {
			vmid = b.VMID
		}
	}
	if vmid == "" {
		t.Fatal("missing failed delete")
	}
	f.mu.Lock()
	vm := f.vms[vmid]
	vm.Instance = "foreign-instance"
	f.vms[vmid] = vm
	f.mu.Unlock()
	if err = s.recoverResources(); err == nil {
		t.Fatal("unknown instance silently accepted")
	}
	vm, err = f.GetVM(context.Background(), vmid)
	if err != nil || vm.Instance != "foreign-instance" {
		t.Fatal("foreign instance deleted", err)
	}
}

func TestForkRollbackLostDeleteResponseConvergesAfterRestart(t *testing.T) {
	s, f, a, p := recoveryFixture(t)
	source := claimTest(t, s, p, "source", nil)
	s.opts.Agent = &forkFailureAgent{fakeAgent: a, gate: make(chan struct{})}
	f.control.Lock()
	f.dropDeleteResponses = 1
	f.control.Unlock()
	o, err := s.Submit(
		s.ctx,
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(s.ctx, p, o.ID); err == nil {
		t.Fatal("fork succeeded")
	}
	var deletion api.Operation
	f.mu.Lock()
	for _, candidate := range f.ops {
		if candidate.Request.Action == "delete" &&
			candidate.Status == "running" {
			deletion = candidate
		}
	}
	f.mu.Unlock()
	if deletion.ID == "" {
		t.Fatal("missing in-flight deletion")
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal("in-flight delete should remain tracked", err)
	}
	f.control.Lock()
	f.findError = errors.New("engine receipt query unavailable")
	f.control.Unlock()
	if err = s.recoverResources(); err == nil {
		t.Fatal("unknown receipt query was hidden")
	}
	f.control.Lock()
	f.findError = nil
	f.control.Unlock()
	// Complete the accepted engine operation after sandboxd lost the response.
	f.mu.Lock()
	deletion.Status = "succeeded"
	f.ops[deletion.Key] = deletion
	delete(f.vms, deletion.VMID)
	f.mu.Unlock()
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
		t.Fatal("restart cleanup did not converge", err)
	}
	for _, id := range o.IDs {
		b := recoverySandbox(t, reopened, id)
		if b.State != "terminated" || b.VMID != "" || b.OperationID != "" {
			t.Fatal("stale fork cleanup remains", b)
		}
	}
	f.control.Lock()
	for _, request := range f.requests {
		if request.Action == "delete" && request.VMID == deletion.VMID &&
			request.Key != deletion.Key {
			f.control.Unlock()
			t.Fatal(
				"successful or unresolved deletion was replaced with a new key",
			)
		}
	}
	f.control.Unlock()
	old, err := reopened.store.operation(context.Background(), o.ID)
	if err != nil || old.Status != "failed" {
		t.Fatal("client terminal result changed", err)
	}
	if _, err = f.GetVM(context.Background(), source.Sandbox.VMID); err != nil {
		t.Fatal(err)
	}
}

func TestForkRollbackUnacceptedDeleteReusesOriginalKey(t *testing.T) {
	s, f, a, p := recoveryFixture(t)
	source := claimTest(t, s, p, "source", nil)
	s.opts.Agent = &forkFailureAgent{fakeAgent: a, gate: make(chan struct{})}
	f.control.Lock()
	f.dropDeleteBeforeAccept = 1
	f.control.Unlock()
	o, err := s.Submit(
		s.ctx,
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(s.ctx, p, o.ID); err == nil {
		t.Fatal("fork succeeded")
	}
	var pending Sandbox
	for _, id := range o.IDs {
		b := recoverySandbox(t, s, id)
		if b.VMID != "" {
			pending = b
		}
	}
	if pending.VMID == "" {
		t.Fatal("missing unaccepted delete")
	}
	originalKey := stepKey(o, releaseVMStep(pending.ID))
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if got := recoverySandbox(t, s, pending.ID); got.VMID != "" {
		t.Fatal("original delete replay did not converge", got)
	}
	f.control.Lock()
	count := 0
	for _, request := range f.requests {
		if request.Action == "delete" && request.VMID == pending.VMID {
			count++
			if request.Key != originalKey {
				f.control.Unlock()
				t.Fatal("unconfirmed delete changed original key")
			}
		}
	}
	f.control.Unlock()
	if count != 2 {
		t.Fatalf("original delete dispatch count=%d want=2", count)
	}
}

func TestForkRollbackUnknownInstanceSurvivesMaintenance(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	source := claimTest(t, s, p, "source", nil)
	f.control.Lock()
	f.failNetwork = true
	f.control.Unlock()
	o, err := s.Submit(
		s.ctx,
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(s.ctx, p, o.ID); err == nil {
		t.Fatal("fork succeeded")
	}
	f.control.Lock()
	f.failNetwork = false
	f.control.Unlock()
	for _, id := range o.IDs {
		b := recoverySandbox(t, s, id)
		if b.State != "terminated" || b.VMID == "" || b.Instance != "" {
			t.Fatal("missing unknown-instance fixture", b)
		}
	}
	// Exercise the same reconciliation, reaping and pool phases as maintenance,
	// including completion of any asynchronous reaper operations.
	for range 2 {
		s.reconcile()
		s.reap()
		s.pools()
		s.wg.Wait()
	}
	for _, id := range o.IDs {
		b := recoverySandbox(t, s, id)
		if b.VMID == "" {
			t.Fatal("maintenance deleted an unproven fork instance")
		}
		if _, err = f.GetVM(s.ctx, b.VMID); err != nil {
			t.Fatal("unproven VM disappeared", err)
		}
		if _, err = s.Execute(
			s.ctx,
			p,
			"explicit-release:"+id,
			Command{Action: "release", SandboxID: id},
		); !isCode(
			err,
			api.ManualRecovery,
		) {
			t.Fatal("shared release accepted unproven instance", err)
		}
	}
	if f.count("delete") != 0 {
		t.Fatal("unproven VM received delete")
	}
	old, err := s.store.operation(s.ctx, o.ID)
	if err != nil || old.Status != "failed" {
		t.Fatal("original terminal operation changed", err)
	}
	if _, err = f.GetVM(s.ctx, source.Sandbox.VMID); err != nil {
		t.Fatal(err)
	}
}

type recoveryTestEngine struct {
	*fakeEngine

	control                sync.Mutex
	failDeletes            int
	dropDeleteResponses    int
	dropDeleteBeforeAccept int
	failNetwork            bool
	failSnapshotDeletes    int
	snapshotDeleteUnknown  bool
	snapshotDeleteEntered  chan struct{}
	snapshotDeleteGate     <-chan struct{}
	snapshotEntered        chan struct{}
	snapshotGate           <-chan struct{}
	snapshotError          error
	findError              error
	requests               []api.Request
	deleteEntered          chan struct{}
	deleteGate             <-chan struct{}
}

func (f *recoveryTestEngine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	f.control.Lock()
	f.requests = append(f.requests, r)
	dropBeforeAccept := r.Action == "delete" && f.dropDeleteBeforeAccept > 0
	if dropBeforeAccept {
		f.dropDeleteBeforeAccept--
	}
	dropDelete := r.Action == "delete" && f.dropDeleteResponses > 0
	if dropDelete {
		f.dropDeleteResponses--
	}
	fail := r.Action == "delete" && f.failDeletes > 0
	if fail {
		f.failDeletes--
	}
	if r.Action == "delete_snapshot" && f.failSnapshotDeletes > 0 {
		fail = true
		f.failSnapshotDeletes--
	}
	entered, gate := f.deleteEntered, f.deleteGate
	unknown := f.snapshotDeleteUnknown
	failNetwork := f.failNetwork
	snapshotEntered := f.snapshotEntered
	snapshotGate := f.snapshotGate
	if r.Action == "delete_snapshot" {
		entered, gate = f.snapshotDeleteEntered, f.snapshotDeleteGate
	}
	f.control.Unlock()
	if r.Action == "network" && failNetwork {
		return api.Operation{}, api.Fail(
			api.Internal,
			"network request failed before receipt",
		)
	}
	if r.Action == "delete_snapshot" && unknown {
		return api.Operation{}, api.Fail(api.Unknown, "delete response unknown")
	}
	if (r.Action == "delete" || r.Action == "delete_snapshot") &&
		entered != nil {
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
	if dropBeforeAccept {
		return api.Operation{}, api.Fail(
			api.Unknown,
			"delete transport failed before acceptance",
		)
	}
	if dropDelete {
		f.mu.Lock()
		f.ops[r.Key] = api.Operation{
			ID:      r.Key,
			Key:     r.Key,
			Request: r,
			VMID:    r.VMID,
			Status:  "running",
		}
		f.mu.Unlock()
		return api.Operation{}, api.Fail(
			api.Unknown,
			"accepted delete response lost",
		)
	}
	if fail {
		f.mu.Lock()
		defer f.mu.Unlock()
		if old, ok := f.ops[r.Key]; ok {
			return old, nil
		}
		o := api.Operation{
			ID:         r.Key,
			Key:        r.Key,
			Request:    r,
			VMID:       r.VMID,
			SnapshotID: r.SnapshotID,
			Status:     "failed",
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
	o, err := f.fakeEngine.Submit(ctx, r)
	if r.Action == "snapshot" && snapshotEntered != nil {
		select {
		case snapshotEntered <- struct{}{}:
		default:
		}
		if snapshotGate != nil {
			select {
			case <-snapshotGate:
			case <-ctx.Done():
				return o, ctx.Err()
			}
		}
	}
	return o, err
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
		ID:             newID("op_"),
		Key:            newID("test_"),
		TenantID:       b.TenantID,
		ChargeTenantID: b.TenantID,
		Command:        Command{Action: action, SandboxID: b.ID},
		IDs:            []string{b.ID},
		Status:         "failed",
		Error: &api.Error{
			Code:    api.Internal,
			Message: "interrupted cleanup",
		},
		CreatedAt: time.Now().UTC(),
	}
	o.Hash = s.store.sign(o.ID)
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

func recoverySandbox(t *testing.T, s *Service, id string) Sandbox {
	t.Helper()
	b, err := getSandbox(s.ctx, s.store.db, id)
	if err != nil {
		t.Fatal(err)
	}
	return b
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

func TestRetiredSnapshotCleanupHonorsLiveReferences(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	for _, action := range []string{"pause", "checkpoint", "resume"} {
		if _, err := s.Execute(
			s.ctx,
			p,
			action,
			Command{Action: action, SandboxID: b.ID},
		); err != nil {
			t.Fatal(err)
		}
	}
	cps, err := s.Checkpoints(s.ctx, p)
	if err != nil || len(cps) != 1 {
		t.Fatal(cps, err)
	}
	cp := cps[0]
	if _, err = f.GetSnapshot(s.ctx, cp.SnapshotID); err != nil {
		t.Fatal("shared capture deleted", err)
	}
	if _, err = s.Execute(
		s.ctx,
		p,
		"delete-cp",
		Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
	); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.GetSnapshot(
		s.ctx,
		cp.SnapshotID,
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("unreferenced capture retained", err)
	}
	// Historical records require a receipt; arbitrary identifiers are not adopted.
	current := recoverySandbox(t, s, b.ID)
	current.SnapshotID = "unproven"
	if err = saveSandbox(s.ctx, s.store.db, current); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Execute(
		s.ctx,
		p,
		"unsafe-pause",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		api.ManualRecovery,
	) {
		t.Fatal("unproven historical capture accepted", err)
	}
}

func TestRetiredSnapshotCleanupRetriesAfterRestart(t *testing.T) {
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
	f.control.Lock()
	f.failSnapshotDeletes = 1
	f.control.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"resume",
		Command{Action: "resume", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	intents, err := s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	var retired resourceRecovery
	for _, r := range intents {
		if r.Kind == "retired_snapshot" {
			retired = r
		}
	}
	if retired.Attempt != 1 || retired.Done {
		t.Fatal("failed attempt not persisted", retired)
	}
	duplicate := retired
	duplicate.ID += "duplicate"
	duplicate.Attempt = 99
	if err = s.saveRecovery(duplicate); err != nil {
		t.Fatal(err)
	}
	f.control.Lock()
	f.snapshotDeleteUnknown = true
	f.control.Unlock()
	if err = s.recoverResources(); err == nil {
		t.Fatal("unknown deletion hidden")
	}
	intents, err = s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range intents {
		if r.ID == retired.ID && r.Attempt != 1 {
			t.Fatal("unknown response changed key", r)
		}
	}
	f.control.Lock()
	for _, request := range f.requests {
		if request.Action == "delete_snapshot" &&
			request.SnapshotID == retired.Snapshot &&
			!strings.Contains(request.Key, "recovery:"+retired.ID+":") {
			f.control.Unlock()
			t.Fatal("orphan collector bypassed retired deletion responsibility")
		}
	}
	f.snapshotDeleteUnknown = false
	f.control.Unlock()
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
	intents, err = reopened.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range intents {
		if r.ID == retired.ID && r.Done && r.Attempt == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("retry not completed", intents)
	}
}

func TestRetiredSnapshotCleanupDoesNotOverwriteNewPause(t *testing.T) {
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
	old := recoverySandbox(t, s, b.ID).SnapshotID
	f.control.Lock()
	f.failSnapshotDeletes = 1
	f.control.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"resume",
		Command{Action: "resume", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	captured := make(chan struct{}, 1)
	f.control.Lock()
	f.snapshotDeleteGate = gate
	f.snapshotDeleteEntered = entered
	captureGate := make(chan struct{})
	f.snapshotEntered = captured
	f.snapshotGate = captureGate
	f.control.Unlock()
	paused := make(chan error, 1)
	go func() {
		_, err := s.Execute(
			s.ctx,
			p,
			"pause-new",
			Command{Action: "pause", SandboxID: b.ID},
		)
		paused <- err
	}()
	select {
	case <-captured:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("new capture did not enter")
	}
	cleanup := make(chan error, 1)
	go func() { cleanup <- s.recoverResources() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(captureGate)
		close(gate)
		t.Fatal("cleanup did not enter")
	}
	close(captureGate)
	close(gate)
	if err := <-cleanup; err != nil {
		t.Fatal(err)
	}
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	current := recoverySandbox(t, s, b.ID)
	if current.State != "paused" || current.SnapshotID == "" ||
		current.SnapshotID == old ||
		current.VMID != "" {
		t.Fatal("cleanup overwrote pause", current)
	}
}

func TestRetiredSnapshotCleanupKeepsQuotaUntilReclaimed(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	if _, err := s.PutTenant(
		s.ctx,
		Principal{Root: true},
		Tenant{
			ID:            p.TenantID,
			Token:         "alice-token",
			MaxSandboxes:  50,
			MaxStorageMiB: 448,
		},
	); err != nil {
		t.Fatal(err)
	}
	f.control.Lock()
	f.failSnapshotDeletes = 100
	f.control.Unlock()
	for _, key := range []string{"one", "two"} {
		for _, action := range []string{"pause", "resume"} {
			if _, err := s.Execute(
				s.ctx,
				p,
				key+action,
				Command{Action: action, SandboxID: b.ID},
			); err != nil {
				t.Fatal(err)
			}
		}
	}
	snapshots := f.count("snapshot")
	if _, err := s.Execute(
		s.ctx,
		p,
		"over-quota",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		api.Exhausted,
	) {
		t.Fatal("cleanup charge lost", err)
	}
	if f.count("snapshot") != snapshots ||
		recoverySandbox(t, s, b.ID).State != "running" {
		t.Fatal("quota rejected after side effect")
	}
	f.control.Lock()
	f.failSnapshotDeletes = 0
	f.control.Unlock()
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Execute(
		s.ctx,
		p,
		"after-cleanup",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
}

func TestRetiredSnapshotHistoricalRunningCaptureAdmission(t *testing.T) {
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
	old := recoverySandbox(t, s, b.ID).SnapshotID
	f.control.Lock()
	f.failSnapshotDeletes = 100
	f.control.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"resume",
		Command{Action: "resume", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	b = recoverySandbox(t, s, b.ID)
	b.SnapshotID = old
	if err := saveSandbox(s.ctx, s.store.db, b); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutTenant(
		s.ctx,
		Principal{Root: true},
		Tenant{
			ID:            p.TenantID,
			Token:         "alice-token",
			MaxSandboxes:  50,
			MaxStorageMiB: 447,
		},
	); err != nil {
		t.Fatal(err)
	}
	count := f.count("snapshot")
	if _, err := s.Execute(
		s.ctx,
		p,
		"unsafe-admission",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		api.Exhausted,
	) {
		t.Fatal("historical capture skipped reservation", err)
	}
	ready, err := s.DataReady(s.ctx, p, b.ID)
	if err != nil || ready.SnapshotID != "" {
		t.Fatal("trusted historical handoff failed", ready, err)
	}
	if _, err := s.Execute(
		s.ctx,
		p,
		"after-handoff",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		api.Exhausted,
	) {
		t.Fatal("handoff released charge", err)
	}
	if f.count("snapshot") != count ||
		recoverySandbox(t, s, b.ID).State != "running" {
		t.Fatal("rejected capture changed running state")
	}
}

func TestRetiredSnapshotQuotaSurvivesRestartAndDeduplicates(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	if _, err := s.PutTenant(
		s.ctx,
		Principal{Root: true},
		Tenant{
			ID:            p.TenantID,
			Token:         "alice-token",
			MaxSandboxes:  50,
			MaxStorageMiB: 448,
		},
	); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"pause", "checkpoint", "resume"} {
		if _, err := s.Execute(
			s.ctx,
			p,
			action,
			Command{Action: action, SandboxID: b.ID},
		); err != nil {
			t.Fatal(err)
		}
	}
	intents, err := s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range intents {
		if r.Kind == "retired_snapshot" {
			r.ID += "duplicate"
			r.Attempt = 4
			if err = s.saveRecovery(r); err != nil {
				t.Fatal(err)
			}
		}
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
	// 64 active VM disk + 192 shared checkpoint/backing snapshot + 192 new
	// pause capture = 448. Cleanup and backing refs do not duplicate the SID.
	if _, err = reopened.Execute(
		reopened.ctx,
		p,
		"boundary",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal("shared charge duplicated", err)
	}
	f.control.Lock()
	f.failSnapshotDeletes = 100
	f.control.Unlock()
	if _, err = reopened.Execute(
		reopened.ctx,
		p,
		"resume-boundary",
		Command{Action: "resume", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.Execute(
		reopened.ctx,
		p,
		"over-boundary",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		api.Exhausted,
	) {
		t.Fatal("persistent charge missing", err)
	}
}

func TestSnapshotOwnershipSurvivesReplayAndRestart(t *testing.T) {
	for _, action := range []string{"template_build", "pause", "checkpoint", "fork"} {
		t.Run(action, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			cmd := Command{Action: action, SandboxID: b.ID, Count: 2}
			if action == "template_build" {
				cmd = Command{
					Action: action,
					Template: &Template{
						ID:    "built",
						Image: "sha256:" + strings.Repeat("1", 64),
						Shape: b.Shape,
					},
				}
			}
			entered := make(chan struct{}, 1)
			gate := make(chan struct{})
			f.control.Lock()
			f.snapshotEntered = entered
			f.snapshotGate = gate
			f.control.Unlock()
			o, err := s.Submit(s.ctx, p, "interrupted", cmd)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("capture did not complete")
			}
			s.cancel()
			s.wg.Wait()
			step := map[string]string{"template_build": "build-snapshot", "pause": "hibernate-snapshot", "checkpoint": "checkpoint-capture", "fork": "fork-snapshot"}[action]
			receipt, err := f.FindOperation(
				context.Background(),
				stepKey(o, step),
			)
			if err != nil {
				t.Fatal(err)
			}
			var count int
			if err = s.store.db.QueryRow("SELECT count(*) FROM snapshot_owners WHERE snapshot_id=?", receipt.SnapshotID).
				Scan(&count); err != nil ||
				count != 0 {
				t.Fatal("published before receipt handoff", count, err)
			}
			opts := s.opts
			opts.Initialize = false
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			f.control.Lock()
			f.snapshotEntered = nil
			f.snapshotGate = nil
			f.control.Unlock()
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { reopened.Close() })
			if _, err = reopened.Wait(
				context.Background(),
				p,
				o.ID,
			); err != nil {
				t.Fatal("replay failed", err)
			}
			var owner, source, kind string
			if err = reopened.store.db.QueryRow("SELECT tenant_id,source_operation_id,source_kind FROM snapshot_owners WHERE snapshot_id=?", receipt.SnapshotID).
				Scan(&owner, &source, &kind); err != nil ||
				owner != p.TenantID ||
				source != o.ID ||
				kind != "captured" {
				t.Fatal(owner, source, kind, err)
			}
			if action == "pause" {
				reused, err := reopened.Execute(
					reopened.ctx,
					p,
					"paused-checkpoint",
					Command{Action: "checkpoint", SandboxID: b.ID},
				)
				if err != nil ||
					reused.Checkpoint.SnapshotID != receipt.SnapshotID {
					t.Fatal("paused checkpoint did not reuse capture", err)
				}
			}
			if _, err = reopened.Execute(
				reopened.ctx,
				Principal{Root: true, TenantID: "other"},
				"transfer",
				Command{
					Action: "template_put",
					Template: &Template{
						ID:         "foreign",
						Shape:      b.Shape,
						SnapshotID: receipt.SnapshotID,
					},
				},
			); !isCode(
				err,
				api.NotFound,
			) {
				t.Fatal("replay provenance reassigned", err)
			}
			if err = reopened.store.db.QueryRow("SELECT count(*) FROM snapshot_owners WHERE snapshot_id=?", receipt.SnapshotID).
				Scan(&count); err != nil ||
				count != 1 {
				t.Fatal("duplicate ownership", count, err)
			}
		})
	}
}

func TestRootCaptureReceiptReplayKeepsSourceTenant(t *testing.T) {
	for _, action := range []string{"pause", "checkpoint"} {
		for _, requestTenant := range []string{"root", "bob"} {
			t.Run(action+"/"+requestTenant, func(t *testing.T) {
				s, f, _, alice := recoveryFixture(t)
				b := *claimTest(t, s, alice, "source", nil).Sandbox
				root := Principal{Root: true, TenantID: requestTenant}
				entered := make(chan struct{}, 1)
				gate := make(chan struct{})
				f.control.Lock()
				f.snapshotEntered = entered
				f.snapshotGate = gate
				f.control.Unlock()
				o, err := s.Submit(
					s.ctx,
					root,
					"root-capture",
					Command{Action: action, SandboxID: b.ID},
				)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("capture did not enter")
				}
				s.cancel()
				s.wg.Wait()
				step := "checkpoint-capture"
				if action == "pause" {
					step = "hibernate-snapshot"
				}
				receipt, err := f.FindOperation(
					context.Background(),
					stepKey(o, step),
				)
				if err != nil {
					t.Fatal(err)
				}
				if action == "pause" {
					// Terminal client failure is compensated from its original capture receipt.
					original, err := s.store.operation(
						context.Background(),
						o.ID,
					)
					if err != nil {
						t.Fatal(err)
					}
					original.Status = "failed"
					original.Error = &api.Error{
						Code:    api.Unknown,
						Message: "capture response lost",
					}
					if err = saveOp(
						context.Background(),
						s.store.db,
						original,
					); err != nil {
						t.Fatal(err)
					}
				}
				opts := s.opts
				opts.Initialize = false
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				f.control.Lock()
				f.snapshotEntered = nil
				f.snapshotGate = nil
				f.control.Unlock()
				reopened, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { reopened.Close() })
				if action == "pause" {
					if err = reopened.recoverResources(); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err = reopened.Wait(
						context.Background(),
						root,
						o.ID,
					); err != nil {
						t.Fatal(err)
					}
				}
				var owner string
				if err = reopened.store.db.QueryRow("SELECT tenant_id FROM snapshot_owners WHERE snapshot_id=?", receipt.SnapshotID).
					Scan(&owner); err != nil ||
					owner != alice.TenantID {
					t.Fatalf(
						"replayed root capture owner=%s err=%v",
						owner,
						err,
					)
				}
				if _, err = reopened.Execute(
					reopened.ctx,
					Principal{TenantID: "bob"},
					"foreign-import",
					Command{
						Action: "template_put",
						Template: &Template{
							ID:         "foreign",
							Shape:      b.Shape,
							SnapshotID: receipt.SnapshotID,
						},
					},
				); !isCode(
					err,
					api.NotFound,
				) {
					t.Fatal("Bob imported replayed Alice capture", err)
				}
				if action == "pause" {
					if _, err = reopened.Execute(
						reopened.ctx,
						alice,
						"alice-resume",
						Command{Action: "resume", SandboxID: b.ID},
					); err != nil {
						t.Fatal("Alice resume after root receipt recovery", err)
					}
				}
			})
		}
	}
}

func TestRootSnapshotGrantSurvivesRestart(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	shape := api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}
	f.mu.Lock()
	f.snaps["external"] = api.Snapshot{
		ID:        "external",
		Config:    api.Config{Shape: shape},
		Published: true,
	}
	f.mu.Unlock()
	cmd := Command{
		Action:   "template_put",
		Template: &Template{ID: "golden", Shape: shape, SnapshotID: "external"},
	}
	if _, err := s.Submit(
		s.ctx,
		p,
		"tenant-import",
		cmd,
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("tenant created grant", err)
	}
	unlock := s.lock("template:golden")
	o, err := s.Submit(
		s.ctx,
		Principal{Root: true, TenantID: p.TenantID},
		"root-import",
		cmd,
	)
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	s.cancel()
	unlock()
	s.wg.Wait()
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
	if _, err = reopened.Wait(context.Background(), p, o.ID); err != nil {
		t.Fatal(err)
	}
	result, err := reopened.Execute(
		reopened.ctx,
		p,
		"clone",
		Command{
			Action:     "claim",
			TemplateID: "golden",
			Policy:     egress.Policy{Mode: "none"},
		},
	)
	if err != nil || result.Sandbox == nil {
		t.Fatal("durable grant lost", err)
	}
	var kind, source string
	if err = reopened.store.db.QueryRow("SELECT source_kind,source_operation_id FROM snapshot_owners WHERE snapshot_id='external'").
		Scan(&kind, &source); err != nil ||
		kind != "external_import" ||
		source != o.ID {
		t.Fatal(kind, source, err)
	}
}

func TestUntrustedExistingTemplateCannotCloneSnapshot(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	template := Template{
		ID:         "untrusted",
		TenantID:   p.TenantID,
		State:      "ready",
		SnapshotID: "unowned",
		TargetWarm: 1,
		Shape:      api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
	}
	f.mu.Lock()
	f.snaps[template.SnapshotID] = api.Snapshot{
		ID:        template.SnapshotID,
		Config:    api.Config{Shape: template.Shape},
		Published: true,
	}
	f.mu.Unlock()
	if _, err := s.store.db.Exec(
		"INSERT INTO templates(id,tenant,body) VALUES(?,?,?)",
		template.ID,
		template.TenantID,
		encode(template),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Execute(
		s.ctx,
		p,
		"claim",
		Command{Action: "claim", TemplateID: template.ID},
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("untrusted template admitted", err)
	}
	s.pools()
	// A previously frozen internal refill is revalidated by the worker too.
	o := Operation{
		ID:       "untrusted-refill",
		Key:      "untrusted-refill",
		TenantID: p.TenantID,
		Command: Command{
			Action:     "refill",
			TemplateID: template.ID,
			Template:   &template,
		},
		Status: "pending",
	}
	o.Hash = s.store.sign(o.ID)
	if err := saveOp(s.ctx, s.store.db, o); err != nil {
		t.Fatal(err)
	}
	s.start(o)
	if _, err := s.Wait(s.ctx, p, o.ID); !isCode(err, api.NotFound) {
		t.Fatal("untrusted refill accepted", err)
	}
	if f.count("clone") != 0 {
		t.Fatal("unauthorized snapshot cloned")
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
