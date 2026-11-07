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

type rejectWarmAgent struct {
	fakeAgent

	vm string
}

func (a *rejectWarmAgent) Activate(
	ctx context.Context,
	vm api.VM,
	activation Activation,
) error {
	if vm.ID == a.vm {
		return errors.New("warm guest no longer responds")
	}
	return a.fakeAgent.Activate(ctx, vm, activation)
}

func warmFallbackFixture(
	t *testing.T,
	golden bool,
) (*Service, *fakeEngine, Principal, Template) {
	t.Helper()
	engine := newFakeEngine()
	template := Template{
		ID:         "python",
		TenantID:   "alice",
		Image:      "sha256:" + strings.Repeat("1", 64),
		Shape:      api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
		TargetWarm: 1,
	}
	config := api.Config{Image: template.Image, Shape: template.Shape}
	if golden {
		template.SnapshotID = "golden"
		engine.snaps[template.SnapshotID] = api.Snapshot{
			ID:        template.SnapshotID,
			Config:    config,
			Published: true,
		}
	}
	var created, started api.Operation
	var err error
	if !golden {
		created, err = api.Wait(
			context.Background(),
			engine,
			api.Request{
				Key: stepKey(
					Operation{ID: "fixture-warm"},
					"fixture-create",
				),
				Action: "create",
				Config: &config,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		started, err = api.Wait(
			context.Background(),
			engine,
			api.Request{
				Key:    stepKey(Operation{ID: "fixture-warm"}, "fixture-start"),
				Action: "start",
				VMID:   created.VMID,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	state := t.TempDir()
	st, err := openStore(state, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Exercise durable lifecycle replay without background pool refills changing
	// the engine allocation counts being asserted by these tests.
	service := &Service{
		opts: Options{
			StateDir:  state,
			RootToken: "root-secret",
			Engine:    engine,
			Agent:     &rejectWarmAgent{vm: created.VMID},
			Egress:    &fakeEgress{},
			MaxTTL:    time.Hour,
			MaxWarm:   32,
		},
		store:       st,
		ctx:         ctx,
		cancel:      cancel,
		epoch:       "test-daemon",
		locks:       map[string]*sync.Mutex{},
		active:      map[string]chan struct{}{},
		secrets:     map[string]map[string]egress.Secret{},
		opSecrets:   map[string]map[string]egress.Secret{},
		bindings:    map[string]string{},
		connections: map[string]map[net.Conn]struct{}{},
	}
	t.Cleanup(func() { service.Close() })
	principal := Principal{TenantID: "alice"}
	if _, err = service.PutTenant(
		context.Background(),
		Principal{Root: true},
		Tenant{
			ID:            "alice",
			Token:         "alice-token",
			MaxSandboxes:  5,
			MaxStorageMiB: 1 << 20,
		},
	); err != nil {
		t.Fatal(err)
	}
	importer := Principal{Root: true, TenantID: principal.TenantID}
	if _, err = service.Execute(
		context.Background(),
		importer,
		"template",
		Command{Action: "template_put", Template: &template},
	); err != nil {
		t.Fatal(err)
	}
	if golden {
		service.pools()
		service.wg.Wait()
		if err = service.store.db.QueryRow(`SELECT vm_id FROM warm WHERE template_id=?`, template.ID).
			Scan(&engine.warmCandidate); err != nil {
			t.Fatal(err)
		}
	} else {
		seedTemplateQuotaWarm(t, service, template, created, started)
		engine.warmCandidate = created.VMID
	}
	service.opts.Agent.(*rejectWarmAgent).vm = engine.warmCandidate
	engine.mu.Lock()
	engine.calls = map[string]int{}
	engine.mu.Unlock() // Count replacement work after the real ready fixture.
	return service, engine, principal, template
}

func TestRejectedWarmFallsBackAndKeepsDeterministicReplay(t *testing.T) {
	for _, golden := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "cold", true: "golden"}[golden],
			func(t *testing.T) {
				s, engine, principal, _ := warmFallbackFixture(t, golden)
				result := claimTest(t, s, principal, "claim", nil)
				if result.Sandbox.ProvisionSource == "warm" ||
					result.Sandbox.State != "running" ||
					result.Sandbox.VMID == engine.warmCandidate {
					t.Fatalf(
						"rejected warm instance was delivered: %+v",
						result.Sandbox,
					)
				}
				if _, err := engine.GetVM(
					context.Background(),
					engine.warmCandidate,
				); !isCode(
					err,
					api.NotFound,
				) {
					t.Fatal("rejected warm instance was retained", err)
				}
				if engine.count("delete") != 1 {
					t.Fatal("warm instance was not deleted exactly once")
				}
				if golden &&
					(engine.count("clone") != 1 || engine.count("create") != 0) {
					t.Fatal("golden fallback did not use one clone")
				}
				if !golden &&
					(engine.count("create") != 1 || engine.count("start") != 1) {
					t.Fatal("cold fallback did not use one create/start")
				}
				replay := claimTest(t, s, principal, "claim", nil)
				if replay.Sandbox.VMID != result.Sandbox.VMID ||
					engine.count("delete") != 1 {
					t.Fatal("claim retry supplied another VM")
				}
			},
		)
	}
}

func TestWarmFallbackRecoversDeletionBeforeReferenceCommit(t *testing.T) {
	s, engine, principal, template := warmFallbackFixture(t, true)
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	o, err := s.Submit(
		s.ctx,
		principal,
		"recovering-claim",
		Command{
			Action:     "claim",
			TemplateID: template.ID,
			Policy:     egress.Policy{Mode: "none"},
		},
	)
	s.mu.Lock()
	s.closing = false
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	b := recoverySandbox(t, s, o.IDs[0])
	if _, err = s.store.db.ExecContext(
		s.ctx,
		`CREATE TRIGGER fallback_metadata_fault BEFORE UPDATE ON sandboxes WHEN NEW.id='`+b.ID+`' AND json_extract(NEW.body,'$.vm_id') IS NULL BEGIN SELECT RAISE(ABORT,'fallback metadata fault'); END`,
	); err != nil {
		t.Fatal(err)
	}
	if err = s.claim(&o); err == nil {
		t.Fatal("fallback metadata fault was not observed")
	}
	if engine.count("delete") != 1 {
		t.Fatal("fault did not follow physical warm deletion")
	}
	if _, err = s.store.db.ExecContext(
		s.ctx,
		`DROP TRIGGER fallback_metadata_fault`,
	); err != nil {
		t.Fatal(err)
	}
	// The durable sandbox still names the rejected VM, as after a crash between
	// confirmed engine deletion and the sandbox reference commit.
	if err := s.claim(&o); err != nil {
		t.Fatal(err)
	}
	if o.Result.Sandbox.ProvisionSource != "golden" ||
		engine.count("clone") != 1 {
		t.Fatal("replay failed to finish the original fallback")
	}
	if err := s.claim(&o); err != nil || engine.count("clone") != 1 {
		t.Fatal("replay duplicated the replacement", err)
	}
}

func TestWarmFallbackPreservesChangedInstance(t *testing.T) {
	s, engine, principal, template := warmFallbackFixture(t, true)
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	o, err := s.Submit(
		s.ctx,
		principal,
		"review",
		Command{Action: "claim", TemplateID: template.ID},
	)
	s.mu.Lock()
	s.closing = false
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	b := recoverySandbox(t, s, o.IDs[0])
	b.Instance = "old-instance"
	if err := s.discardWarm(
		o,
		&b,
		template,
	); !isCode(
		err,
		api.Conflict,
	) {
		t.Fatalf("changed warm instance was not rejected: %v", err)
	}
	if engine.count("delete") != 0 || b.VMID == "" {
		t.Fatal("changed instance was destroyed or forgotten")
	}
}
