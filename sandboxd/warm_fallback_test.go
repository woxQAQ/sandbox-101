package sandboxd

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

type rejectWarmAgent struct{ fakeAgent }

func (a *rejectWarmAgent) Activate(ctx context.Context, vm api.VM, activation Activation) error {
	if vm.ID == "warm-candidate" {
		return errors.New("warm guest no longer responds")
	}
	return a.fakeAgent.Activate(ctx, vm, activation)
}

func warmFallbackFixture(t *testing.T, golden bool) (*Service, *fakeEngine, Principal, Template) {
	t.Helper()
	engine := newFakeEngine()
	template := Template{ID: "python", TenantID: "alice", Image: "sha256:" + strings.Repeat("1", 64), Shape: api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}, TargetWarm: 1}
	config := api.Config{Image: template.Image, Shape: template.Shape}
	if golden {
		template.SnapshotID = "golden"
		engine.snaps[template.SnapshotID] = api.Snapshot{ID: template.SnapshotID, Config: config, Published: true}
	}
	engine.vms["warm-candidate"] = api.VM{ID: "warm-candidate", Config: config, Phase: "running", Verified: true, AgentReady: true, Instance: "warm-instance", VSockPath: "/tmp/warm/vsock"}
	st, err := openStore(t.TempDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Exercise durable lifecycle replay without background pool refills changing
	// the engine allocation counts being asserted by these tests.
	service := &Service{
		opts:  Options{RootToken: "root-secret", Engine: engine, Agent: &rejectWarmAgent{}, Egress: &fakeEgress{}, MaxTTL: time.Hour, MaxWarm: 32},
		store: st, ctx: ctx, cancel: cancel, epoch: "test-daemon",
		locks: map[string]*sync.Mutex{}, active: map[string]chan struct{}{},
		secrets: map[string]map[string]egress.Secret{}, opSecrets: map[string]map[string]egress.Secret{},
		bindings: map[string]string{}, connections: map[string]map[net.Conn]struct{}{},
	}
	t.Cleanup(func() { service.Close() })
	principal := Principal{TenantID: "alice"}
	if _, err = service.PutTenant(context.Background(), Principal{Root: true}, Tenant{ID: "alice", Token: "alice-token", MaxSandboxes: 5, MaxStorageMiB: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.store.db.ExecContext(service.ctx, "INSERT INTO warm(vm_id,template_id,snapshot_id,created_at) VALUES(?,?,?,?)", "warm-candidate", template.ID, template.SnapshotID, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Execute(context.Background(), principal, "template", Command{Action: "template_put", Template: &template}); err != nil {
		t.Fatal(err)
	}
	return service, engine, principal, template
}

func TestRejectedWarmFallsBackAndKeepsDeterministicReplay(t *testing.T) {
	for _, golden := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "golden"}[golden], func(t *testing.T) {
			s, engine, principal, _ := warmFallbackFixture(t, golden)
			result := claimTest(t, s, principal, "claim", nil)
			if result.Sandbox.ProvisionSource == "warm" || result.Sandbox.State != "running" || result.Sandbox.VMID == "warm-candidate" {
				t.Fatalf("rejected warm instance was delivered: %+v", result.Sandbox)
			}
			if _, err := engine.GetVM(context.Background(), "warm-candidate"); !isCode(err, api.NotFound) {
				t.Fatal("rejected warm instance was retained", err)
			}
			if engine.count("delete") != 1 {
				t.Fatal("warm instance was not deleted exactly once")
			}
			if golden && (engine.count("clone") != 1 || engine.count("create") != 0) {
				t.Fatal("golden fallback did not use one clone")
			}
			if !golden && (engine.count("create") != 1 || engine.count("start") != 1) {
				t.Fatal("cold fallback did not use one create/start")
			}
			replay := claimTest(t, s, principal, "claim", nil)
			if replay.Sandbox.VMID != result.Sandbox.VMID || engine.count("delete") != 1 {
				t.Fatal("claim retry supplied another VM")
			}
		})
	}
}

func TestWarmFallbackRecoversDeletionBeforeReferenceCommit(t *testing.T) {
	s, engine, principal, template := warmFallbackFixture(t, true)
	b := Sandbox{ID: "recovering", TenantID: principal.TenantID, State: "provisioning", TemplateID: template.ID, Shape: template.Shape, TTLSeconds: 300, Policy: egress.Policy{Mode: "none"}}
	if err := s.saveSandboxReference(b); err != nil {
		t.Fatal(err)
	}
	if err := s.claimWarm(template, &b); err != nil {
		t.Fatal(err)
	}
	b.ProvisionSource = "warm-rejected"
	b.Instance = "warm-instance"
	if err := s.saveSandboxReference(b); err != nil {
		t.Fatal(err)
	}
	o := Operation{ID: "recovering-claim", IDs: []string{b.ID}, Command: Command{Action: "claim", Template: &template}}
	if _, err := s.engine(o, "warm-reject:"+b.ID, api.Request{Action: "delete", VMID: b.VMID, ExpectedInstance: b.Instance}); err != nil {
		t.Fatal(err)
	}
	// The durable sandbox still names the rejected VM, as after a crash between
	// confirmed engine deletion and the product reference commit.
	if err := s.claim(&o); err != nil {
		t.Fatal(err)
	}
	if o.Result.Sandbox.ProvisionSource != "golden" || engine.count("clone") != 1 {
		t.Fatal("replay failed to finish the original fallback")
	}
	if err := s.claim(&o); err != nil || engine.count("clone") != 1 {
		t.Fatal("replay duplicated the replacement", err)
	}
}

func TestWarmFallbackPreservesChangedInstance(t *testing.T) {
	s, engine, _, template := warmFallbackFixture(t, true)
	b := Sandbox{ID: "review", VMID: "warm-candidate", Instance: "old-instance", ProvisionSource: "warm-rejected"}
	if err := s.discardWarm(Operation{ID: "review"}, &b, template); !isCode(err, api.Conflict) {
		t.Fatalf("changed warm instance was not rejected: %v", err)
	}
	if engine.count("delete") != 0 || b.VMID == "" {
		t.Fatal("changed instance was destroyed or forgotten")
	}
}
