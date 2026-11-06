package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

type testPolicyState struct {
	Instance string
	Policy   *api.NetworkPolicy
	Valid    bool
}

type policyTestBackend struct {
	*testBackend
	policyMu     sync.Mutex
	policies     map[string]testPolicyState
	installs     int
	gateVM       string
	gate         chan struct{}
	disableError error
}

func newPolicyTestBackend() *policyTestBackend {
	return &policyTestBackend{testBackend: newTestBackend(), policies: map[string]testPolicyState{}}
}
func (b *policyTestBackend) InstallNetworkPolicy(ctx context.Context, r Resource, p *api.NetworkPolicy) error {
	if r.VMID == b.gateVM && b.gate != nil {
		select {
		case <-b.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	obs, err := b.Inspect(ctx, r)
	if err != nil {
		return err
	}
	if obs.Network {
		return api.Fail(api.Conflict, "interfaces on")
	}
	if err = validateNetworkPolicy(p); err != nil {
		return err
	}
	b.policyMu.Lock()
	defer b.policyMu.Unlock()
	old := b.policies[r.VMID]
	if old.Valid && old.Instance == r.Instance && reflect.DeepEqual(old.Policy, p) {
		return nil
	}
	var copy *api.NetworkPolicy
	if err = json.Unmarshal(encode(p), &copy); err != nil {
		return err
	}
	b.policies[r.VMID] = testPolicyState{Instance: r.Instance, Policy: copy, Valid: true}
	b.installs++
	return nil
}
func (b *policyTestBackend) VerifyNetworkPolicy(_ context.Context, r Resource, p *api.NetworkPolicy) error {
	b.policyMu.Lock()
	defer b.policyMu.Unlock()
	s := b.policies[r.VMID]
	if !s.Valid || s.Instance != r.Instance || !reflect.DeepEqual(s.Policy, p) {
		return api.Fail(api.Unknown, "policy reality mismatch")
	}
	return nil
}
func (b *policyTestBackend) Network(ctx context.Context, r Resource, enable bool) error {
	if !enable {
		b.policyMu.Lock()
		err := b.disableError
		b.policyMu.Unlock()
		if err != nil {
			return err
		}
	}
	if enable {
		b.policyMu.Lock()
		s := b.policies[r.VMID]
		b.policyMu.Unlock()
		if s.Policy == nil {
			return api.Fail(api.Conflict, "no policy")
		}
		if err := b.VerifyNetworkPolicy(ctx, r, s.Policy); err != nil {
			return err
		}
	}
	return b.testBackend.Network(ctx, r, enable)
}
func openPolicyTest(t *testing.T, dir string, b *policyTestBackend, init bool, fault func(string) error) *Engine {
	t.Helper()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	e, err := Open(Options{StateDir: dir, Init: init, Backend: b, SafetyBytes: 1, MaxMemoryMiB: 8192, MaxVCPUs: 64, ReconcileInterval: time.Hour, Fault: fault})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func startPolicyVM(t *testing.T, e *Engine, key string) api.VM {
	t.Helper()
	create := wait(t, e, api.Request{Key: key + ":create", Action: "create", Config: config()})
	wait(t, e, api.Request{Key: key + ":start", Action: "start", VMID: create.VMID})
	vm, err := e.GetVM(context.Background(), create.VMID)
	if err != nil {
		t.Fatal(err)
	}
	return vm
}
func directPolicy() *api.NetworkPolicy {
	return &api.NetworkPolicy{Version: 1, AllowedCIDRs: []string{"1.1.1.1/32"}, MaxConnections: 256}
}
func policyRequest(vm api.VM, key string, p *api.NetworkPolicy) api.Request {
	return api.Request{Key: key, Action: "network_policy", VMID: vm.ID, ExpectedInstance: vm.Instance, Policy: p}
}
func networkRequest(vm api.VM, key string, on bool) api.Request {
	return api.Request{Key: key, Action: "network", VMID: vm.ID, ExpectedInstance: vm.Instance, NetworkEnabled: on}
}
func requirePolicyFailure(t *testing.T, e *Engine, request api.Request, code api.Code) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := api.Wait(ctx, e, request)
	var a *api.Error
	if !errors.As(err, &a) || a.Code != code {
		t.Fatalf("wanted %s, got %v", code, err)
	}
}

func TestNetworkPolicyValidation(t *testing.T) {
	for _, cidr := range []string{"0.0.0.0/0", "1.1.1.1/32", "8.8.0.0/16"} {
		p := directPolicy()
		p.AllowedCIDRs = []string{cidr}
		if err := validateNetworkPolicy(p); err != nil {
			t.Errorf("valid %s: %v", cidr, err)
		}
	}
	for _, cidr := range []string{
		"10.0.0.0/8", "127.0.0.1/32", "100.100.100.200/32", "169.254.169.254/32",
		"168.63.129.16/32", "192.0.0.192/32", "198.18.0.0/15", "224.0.0.1/32",
		"255.255.255.255/32", "192.0.2.0/24", "::/0", "::ffff:1.1.1.1/128",
		"1.1.1.1/24", "01.1.1.1/32", "1.1.1.1", "example.com", "1.1.1.1/32; accept", "1.1.1.1/32\nflush ruleset",
	} {
		p := directPolicy()
		p.AllowedCIDRs = []string{cidr}
		if err := validateNetworkPolicy(p); err == nil {
			t.Errorf("accepted invalid %q", cidr)
		}
	}
	for _, budget := range []int{-1, 0, 65536} {
		p := directPolicy()
		p.MaxConnections = budget
		if validateNetworkPolicy(p) == nil {
			t.Errorf("accepted budget %d", budget)
		}
	}
	p := directPolicy()
	p.AllowedHosts = []string{"example.com"}
	if validateNetworkPolicy(p) == nil {
		t.Fatal("broadened a hostname policy into CIDRs")
	}
	p = directPolicy()
	p.AllowedCIDRs = append(p.AllowedCIDRs, p.AllowedCIDRs[0])
	if validateNetworkPolicy(p) == nil {
		t.Fatal("accepted duplicate CIDRs")
	}
}

func TestNetworkPolicyActivationReplacementAndRevocation(t *testing.T) {
	b := newPolicyTestBackend()
	e := openPolicyTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	vm := startPolicyVM(t, e, "vm")
	requirePolicyFailure(t, e, networkRequest(vm, "baseline-on", true), api.Conflict)
	p := directPolicy()
	install := policyRequest(vm, "policy", p)
	original := wait(t, e, install)
	cached, err := e.Submit(context.Background(), install)
	if err != nil || cached.ID != original.ID || cached.Status != "succeeded" {
		t.Fatalf("policy idempotency: %+v %v", cached, err)
	}
	changed := *p
	changed.MaxConnections = 16
	conflicting := install
	conflicting.Policy = &changed
	requirePolicyFailure(t, e, conflicting, api.Conflict)
	wait(t, e, policyRequest(vm, "identical-new-key", p))
	b.policyMu.Lock()
	installs := b.installs
	b.policyMu.Unlock()
	if installs != 1 {
		t.Fatalf("identical installation changed kernel policy: %d", installs)
	}
	wait(t, e, networkRequest(vm, "on", true))
	requirePolicyFailure(t, e, policyRequest(vm, "replace-on", &changed), api.Conflict)
	requirePolicyFailure(t, e, policyRequest(vm, "revoke-on", nil), api.Conflict)
	actual, _ := e.GetVM(context.Background(), vm.ID)
	if !actual.NetworkActual || !actual.NetworkEnabled || actual.NetworkPolicy.MaxConnections != 256 {
		t.Fatalf("failed replacement changed active policy: %+v", actual)
	}
	wait(t, e, networkRequest(vm, "off", false))
	wait(t, e, policyRequest(vm, "replace-off", &changed))
	wait(t, e, networkRequest(vm, "on-new-policy", true))
	wait(t, e, networkRequest(vm, "off-for-revoke", false))
	revoke := policyRequest(vm, "revoke", nil)
	wait(t, e, revoke)
	wait(t, e, revoke)
	wait(t, e, policyRequest(vm, "revoke-again", nil))
	requirePolicyFailure(t, e, networkRequest(vm, "on-revoked", true), api.Conflict)
}

func TestNetworkPolicyRestartVerifiesRulesAndFailsClosed(t *testing.T) {
	for _, corrupt := range []string{"none", "missing", "rules", "instance", "policy", "inspection"} {
		t.Run(corrupt, func(t *testing.T) {
			dir := t.TempDir()
			b := newPolicyTestBackend()
			e := openPolicyTest(t, dir, b, true, nil)
			vm := startPolicyVM(t, e, "vm")
			wait(t, e, policyRequest(vm, "policy", directPolicy()))
			wait(t, e, networkRequest(vm, "on", true))
			e.Close()
			b.policyMu.Lock()
			s := b.policies[vm.ID]
			switch corrupt {
			case "missing":
				s = testPolicyState{}
			case "rules":
				s.Valid = false
			case "instance":
				s.Instance = "old-instance"
			case "policy":
				s.Policy.MaxConnections++
			}
			b.policies[vm.ID] = s
			b.policyMu.Unlock()
			if corrupt == "inspection" {
				b.mu.Lock()
				b.inspectError = errors.New("CH unreachable")
				b.mu.Unlock()
			}
			e = openPolicyTest(t, dir, b, false, nil)
			defer e.Close()
			actual, _ := e.GetVM(context.Background(), vm.ID)
			if corrupt == "none" {
				if !actual.NetworkActual || !actual.Verified || actual.Instance != vm.Instance {
					t.Fatalf("verified running policy lost during reopen: %+v", actual)
				}
			} else {
				if actual.NetworkActual || actual.NetworkEnabled || actual.Verified || actual.BlockedReason == "" {
					t.Fatalf("unknown policy not closed: %+v", actual)
				}
				b.mu.Lock()
				up := b.states[vm.Instance].Network
				b.mu.Unlock()
				if up {
					t.Fatal("only metadata closed; effective network remains on")
				}
			}
		})
	}
}

func TestNetworkPolicyShutdownFailureRemainsVisible(t *testing.T) {
	b := newPolicyTestBackend()
	e := openPolicyTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	vm := startPolicyVM(t, e, "vm")
	wait(t, e, policyRequest(vm, "policy", directPolicy()))
	wait(t, e, networkRequest(vm, "on", true))
	b.policyMu.Lock()
	s := b.policies[vm.ID]
	s.Valid = false
	b.policies[vm.ID] = s
	b.disableError = errors.New("link shutdown failed")
	b.policyMu.Unlock()
	// Model an external link activation before metadata caught up.
	lock := e.vmLock(vm.ID)
	lock.Lock()
	v, err := e.loadVM(context.Background(), vm.ID)
	if err != nil {
		t.Fatal(err)
	}
	v.VM.NetworkActual = false
	err = e.saveVM(v)
	lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	actual, _ := e.GetVM(context.Background(), vm.ID)
	if !actual.NetworkActual || actual.NetworkEnabled || actual.Verified || actual.BlockedReason == "" {
		t.Fatalf("shutdown failure concealed active interfaces: %+v", actual)
	}
	b.policyMu.Lock()
	b.disableError = nil
	b.policyMu.Unlock()
	if err = e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	actual, _ = e.GetVM(context.Background(), vm.ID)
	if actual.NetworkActual {
		t.Fatal("reconciliation did not retry shutdown")
	}
}

func TestNetworkPolicyInstallCrashRecovery(t *testing.T) {
	for _, boundary := range []string{"network_policy_intent", "network_policy_installed", "network_policy_committed"} {
		t.Run(boundary, func(t *testing.T) {
			dir := t.TempDir()
			b := newPolicyTestBackend()
			e := openPolicyTest(t, dir, b, true, func(step string) error {
				if step == boundary {
					return ErrInterrupted
				}
				return nil
			})
			vm := startPolicyVM(t, e, "vm")
			op, err := e.Submit(context.Background(), policyRequest(vm, "policy", directPolicy()))
			if err != nil {
				t.Fatal(err)
			}
			awaitStep(t, e, op.ID, boundary)
			e.Close()
			e = openPolicyTest(t, dir, b, false, nil)
			defer e.Close()
			if done := awaitStatus(t, e, op.ID); done.Status != "succeeded" {
				t.Fatalf("policy recovery failed: %+v", done)
			}
			actual, _ := e.GetVM(context.Background(), vm.ID)
			if actual.NetworkActual || actual.NetworkEnabled {
				t.Fatal("installation recovery activated links")
			}
			wait(t, e, networkRequest(vm, "on", true))
		})
	}
}

func TestNetworkPolicyActivationCrashRecovery(t *testing.T) {
	for _, boundary := range []string{"network_intent", "network_applied"} {
		for _, staleRules := range []bool{false, true} {
			name := boundary
			if staleRules {
				name += "-stale"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				b := newPolicyTestBackend()
				var armed atomic.Bool
				e := openPolicyTest(t, dir, b, true, func(step string) error {
					if step == boundary && armed.Load() {
						return ErrInterrupted
					}
					return nil
				})
				vm := startPolicyVM(t, e, "vm")
				wait(t, e, policyRequest(vm, "policy", directPolicy()))
				armed.Store(true)
				op, err := e.Submit(context.Background(), networkRequest(vm, "on", true))
				if err != nil {
					t.Fatal(err)
				}
				awaitStep(t, e, op.ID, boundary)
				e.Close()
				if staleRules {
					b.policyMu.Lock()
					s := b.policies[vm.ID]
					s.Valid = false
					b.policies[vm.ID] = s
					b.policyMu.Unlock()
				}
				e = openPolicyTest(t, dir, b, false, nil)
				defer e.Close()
				done := awaitStatus(t, e, op.ID)
				actual, _ := e.GetVM(context.Background(), vm.ID)
				if staleRules {
					if done.Status != "failed" || actual.NetworkActual || actual.NetworkEnabled {
						t.Fatalf("resumed activation with stale rules: %+v %+v", done, actual)
					}
				} else if done.Status != "succeeded" || !actual.NetworkActual {
					t.Fatalf("verified activation did not resume: %+v %+v", done, actual)
				}
			})
		}
	}
}

func TestNetworkPolicyInstancePreconditions(t *testing.T) {
	b := newPolicyTestBackend()
	e := openPolicyTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	vm := startPolicyVM(t, e, "vm")
	missing := policyRequest(vm, "missing", directPolicy())
	missing.ExpectedInstance = ""
	requirePolicyFailure(t, e, missing, api.Invalid)
	missing = networkRequest(vm, "missing-off", false)
	missing.ExpectedInstance = ""
	requirePolicyFailure(t, e, missing, api.Invalid)
	wait(t, e, policyRequest(vm, "old-policy", directPolicy()))
	wait(t, e, networkRequest(vm, "old-on", true))
	wait(t, e, api.Request{Key: "stop", Action: "stop", VMID: vm.ID, ExpectedInstance: vm.Instance})
	wait(t, e, api.Request{Key: "restart", Action: "start", VMID: vm.ID, ExpectedInstance: vm.Instance})
	current, _ := e.GetVM(context.Background(), vm.ID)
	if current.Instance == vm.Instance || current.NetworkEnabled || current.NetworkActual || current.NetworkPolicy != nil {
		t.Fatalf("fresh instance inherited old permission: %+v", current)
	}
	requirePolicyFailure(t, e, networkRequest(vm, "stale-on", true), api.Conflict)
	requirePolicyFailure(t, e, networkRequest(vm, "stale-off", false), api.Conflict)
	requirePolicyFailure(t, e, policyRequest(vm, "stale-policy", directPolicy()), api.Conflict)
	requirePolicyFailure(t, e, policyRequest(vm, "stale-revoke", nil), api.Conflict)
	requirePolicyFailure(t, e, networkRequest(current, "new-on-without-policy", true), api.Conflict)
	wait(t, e, policyRequest(current, "current-policy", directPolicy()))
	wait(t, e, networkRequest(current, "current-on", true))
	// Replaying a completed old request must not reapply effects to this instance.
	wait(t, e, policyRequest(vm, "old-policy", directPolicy()))
	actual, _ := e.GetVM(context.Background(), vm.ID)
	if !actual.NetworkActual || actual.Instance != current.Instance {
		t.Fatal("cached old operation altered current instance")
	}
}

func TestNetworkPolicyStaleExecutionAfterAcceptance(t *testing.T) {
	dir := t.TempDir()
	b := newPolicyTestBackend()
	e := openPolicyTest(t, dir, b, true, func(step string) error {
		if step == "network_policy_intent" {
			return ErrInterrupted
		}
		return nil
	})
	vm := startPolicyVM(t, e, "vm")
	op, err := e.Submit(context.Background(), policyRequest(vm, "policy", directPolicy()))
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, op.ID, "network_policy_intent")
	// Model a newer persisted instance seen when an old operation is replayed.
	lock := e.vmLock(vm.ID)
	lock.Lock()
	r, err := e.loadVM(context.Background(), vm.ID)
	if err == nil {
		r.VM.Instance = "newer-instance"
		r.Resource.Instance = r.VM.Instance
		err = e.saveVM(r)
	}
	lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	e = openPolicyTest(t, dir, b, false, nil)
	defer e.Close()
	done := awaitStatus(t, e, op.ID)
	if done.Error == nil || done.Error.Code != api.Conflict {
		t.Fatalf("stale execution accepted: %+v", done)
	}
	b.policyMu.Lock()
	defer b.policyMu.Unlock()
	if b.installs != 0 {
		t.Fatal("stale operation installed policy")
	}
}

func TestNetworkPolicyDoesNotSerializeOtherVMs(t *testing.T) {
	b := newPolicyTestBackend()
	e := openPolicyTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	a := startPolicyVM(t, e, "a")
	snapshot := wait(t, e, api.Request{Key: "snapshot", Action: "snapshot", VMID: a.ID})
	b.gateVM = a.ID
	b.gate = make(chan struct{})
	op, err := e.Submit(context.Background(), policyRequest(a, "a-policy", directPolicy()))
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, op.ID, "network_policy_intent")
	cloned := wait(t, e, api.Request{Key: "clone", Action: "clone", SnapshotID: snapshot.SnapshotID})
	other, err := e.GetVM(context.Background(), cloned.VMID)
	if err != nil {
		t.Fatal(err)
	}
	if other.NetworkPolicy != nil || other.NetworkEnabled || other.NetworkActual {
		t.Fatal("clone inherited policy or enabled network")
	}
	wait(t, e, policyRequest(other, "b-policy", directPolicy()))
	wait(t, e, networkRequest(other, "b-on", true))
	close(b.gate)
	if done := awaitStatus(t, e, op.ID); done.Status != "succeeded" {
		t.Fatalf("blocked policy failed: %+v", done)
	}
}
