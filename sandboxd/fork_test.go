package sandboxd

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func TestForkWithoutEnvironmentClearsSourceValues(t *testing.T) {
	s, _, a, p := fixture(t)
	r := claimTest(
		t,
		s,
		p,
		"claim",
		map[string]EnvVar{"SOURCE_ONLY": {Value: "old"}},
	)
	branches, err := s.Execute(
		context.Background(),
		p,
		"fork",
		Command{Action: "fork", SandboxID: r.Sandbox.ID, Count: 1},
	)
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

func TestForkIdentityAndEnvironmentIsolation(t *testing.T) {
	s, _, a, p := fixture(t)
	source := claimTest(
		t,
		s,
		p,
		"claim",
		map[string]EnvVar{"OLD": {Value: "source-only"}},
	)
	r, err := s.Execute(
		context.Background(),
		p,
		"fork",
		Command{
			Action:    "fork",
			SandboxID: source.Sandbox.ID,
			Count:     2,
			Env:       map[string]EnvVar{"NEW": {Value: "branch"}},
			Policy:    egress.Policy{Mode: "none"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Sandboxes) != 2 || r.Sandboxes[0].ID == r.Sandboxes[1].ID ||
		r.Tokens[r.Sandboxes[0].ID] == r.Tokens[r.Sandboxes[1].ID] {
		t.Fatal("fork did not allocate independent branches")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, act := range a.activations[len(a.activations)-2:] {
		if !act.ResetIdentity || act.Env["OLD"] != "" ||
			act.Env["NEW"] != "branch" {
			t.Fatal("fork leaked source environment or identity")
		}
	}
}
