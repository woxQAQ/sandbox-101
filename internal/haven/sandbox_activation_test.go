package haven

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
	"github.com/woxQAQ/haven/loom"
)

type workloadFixture struct {
	mu          sync.Mutex
	activations []Activation
	before      func(string, loom.Resource) error
	peers       []net.Conn
}

func (guest *workloadFixture) effect(name string, resource loom.Resource) error {
	if guest.before != nil {
		return guest.before(name, resource)
	}
	return nil
}

func (guest *workloadFixture) Info(_ context.Context, resource loom.Resource) error {
	return guest.effect("info", resource)
}

func (guest *workloadFixture) Activate(_ context.Context, resource loom.Resource, activation Activation) error {
	if err := guest.effect("activate", resource); err != nil {
		return err
	}
	guest.mu.Lock()
	guest.activations = append(guest.activations, activation)
	guest.mu.Unlock()
	return nil
}

func (guest *workloadFixture) PrepareTemplate(_ context.Context, resource loom.Resource) error {
	return guest.effect("prepare", resource)
}

func (guest *workloadFixture) Exec(_ context.Context, resource loom.Resource, _, _ string) error {
	return guest.effect("exec", resource)
}

func (guest *workloadFixture) Connect(_ context.Context, resource loom.Resource) (net.Conn, error) {
	if err := guest.effect("connect", resource); err != nil {
		return nil, err
	}
	one, two := net.Pipe()
	guest.mu.Lock()
	guest.peers = append(guest.peers, two)
	guest.mu.Unlock()
	return one, nil
}

func (guest *workloadFixture) Forward(ctx context.Context, resource loom.Resource, _ int) (net.Conn, error) {
	return guest.Connect(ctx, resource)
}

func (guest *workloadFixture) close() {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	for _, conn := range guest.peers {
		_ = conn.Close()
	}
}

type egressFixture struct {
	mu     sync.Mutex
	binds  map[string]map[string]egress.Secret
	before func(string) error
}

func (manager *egressFixture) Bind(_ context.Context, id, _, _ string, _ egress.Policy, secrets map[string]egress.Secret) error {
	if manager.before != nil {
		if err := manager.before("bind"); err != nil {
			return err
		}
	}
	manager.mu.Lock()
	manager.binds[id] = secrets
	manager.mu.Unlock()
	return nil
}

func (manager *egressFixture) Revoke(id string) error {
	if manager.before != nil {
		if err := manager.before("revoke"); err != nil {
			return err
		}
	}
	manager.mu.Lock()
	delete(manager.binds, id)
	manager.mu.Unlock()
	return nil
}

func (manager *egressFixture) Close() error {
	manager.mu.Lock()
	manager.binds = map[string]map[string]egress.Secret{}
	manager.mu.Unlock()
	return nil
}

func sandboxFixture(t *testing.T) (*Service, *coldRuntime, *workloadFixture, *egressFixture) {
	t.Helper()
	service, runtime := coldService(t, nil)
	guest := &workloadFixture{}
	proxy := &egressFixture{binds: map[string]map[string]egress.Secret{}}
	service.opts.Guest = guest
	service.opts.Egress = proxy
	service.opts.RootToken = "fixture-root-token"
	t.Cleanup(guest.close)
	return service, runtime, guest, proxy
}

func claimSandbox(t *testing.T, service *Service, key string, env map[string]EnvVar, policy egress.Policy) Operation {
	t.Helper()
	operation, err := service.AdmitClaim(context.Background(), Principal{TenantID: "tenant"}, key, Command{Action: "claim", TemplateID: "cold", TTLSeconds: 60, Env: env, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	operation, err = service.Wait(ctx, Principal{TenantID: "tenant"}, operation.ID)
	if err != nil {
		service.mu.Lock()
		reason := service.lastErrors[operation.ID]
		service.mu.Unlock()
		t.Fatal("sandbox claim failed", operation, err, reason)
	}
	return operation
}

func TestHavenClaimDelivery(t *testing.T) {
	service, runtime, guest, _ := sandboxFixture(t)
	operation := claimSandbox(t, service, "claim", map[string]EnvVar{"VISIBLE": {Value: "guest-value"}}, egress.Policy{Mode: "none"})
	sandbox, err := service.Get(context.Background(), Principal{TenantID: "tenant"}, operation.Result.SandboxID)
	if err != nil || sandbox.State != "running" || sandbox.Instance == "" || !time.Now().Before(sandbox.Deadline) {
		t.Fatal(sandbox, err)
	}
	if _, err = service.DataReady(context.Background(), Principal{TenantID: "tenant"}, sandbox.ID); err != nil {
		t.Fatal(err)
	}
	if runtime.count("launch") != 1 || len(guest.activations) != 1 || guest.activations[0].Env["VISIBLE"] != "guest-value" {
		t.Fatal("claim bypassed or repeated guest activation")
	}
	conn, err := service.Connect(context.Background(), Principal{TenantID: "tenant"}, sandbox.ID)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if _, err = service.Connect(context.Background(), Principal{TenantID: "other"}, sandbox.ID); err == nil {
		t.Fatal("foreign workload admission")
	}
}

func TestHavenConfigureAdmission(t *testing.T) {
	service, _, guest, _ := sandboxFixture(t)
	operation := claimSandbox(t, service, "claim", nil, egress.Policy{Mode: "none"})
	entered, finish := make(chan struct{}), make(chan struct{})
	guest.before = func(name string, _ loom.Resource) error {
		if name == "activate" {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-finish
		}
		return nil
	}
	first, err := service.AdmitConfigure(context.Background(), Principal{TenantID: "tenant"}, "blocked-config", Command{Action: "configure", SandboxID: operation.Result.SandboxID, Env: map[string]EnvVar{"VALUE": {Value: "old"}}})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err = service.DataReady(context.Background(), Principal{TenantID: "tenant"}, operation.Result.SandboxID); err == nil {
		t.Fatal("configure fence allowed data")
	}
	newer, err := service.AdmitConfigure(context.Background(), Principal{TenantID: "tenant"}, "new-config", Command{Action: "configure", SandboxID: operation.Result.SandboxID, Env: map[string]EnvVar{"VALUE": {Value: "new"}}})
	if err != nil {
		t.Fatal(err)
	}
	close(finish)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = service.Wait(ctx, Principal{TenantID: "tenant"}, first.ID); err == nil {
		t.Fatal("superseded configuration succeeded")
	}
	if _, err = service.Wait(ctx, Principal{TenantID: "tenant"}, newer.ID); err != nil {
		t.Fatal(err)
	}
	sandbox, err := service.DataReady(ctx, Principal{TenantID: "tenant"}, operation.Result.SandboxID)
	if err != nil || sandbox.Env["VALUE"] != "new" {
		t.Fatal(sandbox, err)
	}
}

func TestHavenSecretRestart(t *testing.T) {
	service, runtime, guest, manager := sandboxFixture(t)
	secretFlag := false
	const secret = "fixture-host-value-never-a-real-credential"
	env := map[string]EnvVar{"TOKEN": {Value: secret, Guest: &secretFlag, Inject: &Injection{Hosts: []string{"api.example.com"}, Header: "Authorization", Placeholder: "placeholder"}}}
	operation := claimSandbox(t, service, "secret-claim", env, egress.Policy{Mode: "relay"})
	for _, activation := range guest.activations {
		for _, value := range activation.Env {
			if strings.Contains(value, secret) {
				t.Fatal("secret reached guest activation")
			}
		}
	}
	rows, err := service.store.db.Query("SELECT request FROM operations UNION ALL SELECT input FROM operation_steps UNION ALL SELECT body FROM sandbox_settings UNION ALL SELECT body FROM journal")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var body []byte
		if err = rows.Scan(&body); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), secret) {
			t.Fatal("host secret entered durable metadata")
		}
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err = service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts := service.opts
	opts.Initialize = false
	opts.Backend = runtime
	opts.Guest = guest
	opts.Egress = manager
	restarted, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	if _, err = restarted.DataReady(context.Background(), Principal{TenantID: "tenant"}, operation.Result.SandboxID); err == nil {
		t.Fatal("restart admitted missing host credentials")
	}
	configured, err := restarted.AdmitConfigure(context.Background(), Principal{TenantID: "tenant"}, "resupply", Command{Action: "configure", SandboxID: operation.Result.SandboxID, Env: env, Policy: egress.Policy{Mode: "relay"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = restarted.Wait(ctx, Principal{TenantID: "tenant"}, configured.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.DataReady(ctx, Principal{TenantID: "tenant"}, operation.Result.SandboxID); err != nil {
		t.Fatal(err)
	}
	changed := env["TOKEN"]
	changed.Value = "different-useless-value"
	env["TOKEN"] = changed
	if _, err = restarted.AdmitConfigure(ctx, Principal{TenantID: "tenant"}, "resupply", Command{Action: "configure", SandboxID: operation.Result.SandboxID, Env: env, Policy: egress.Policy{Mode: "relay"}}); err == nil {
		t.Fatal("credential change reused fixed idempotency identity")
	}
	if configured.Result.SandboxID != operation.Result.SandboxID {
		t.Fatal("configuration changed sandbox identity")
	}
}
