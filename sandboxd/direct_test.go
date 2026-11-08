package sandboxd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func TestDirectPolicyActivationAndRetryableRevocation(t *testing.T) {
	engine := &directTestEngine{fakeEngine: newFakeEngine()}
	var service *Service
	ready := make(chan struct{})
	manager, err := egress.New(
		egress.Options{
			InstallDirect: func(ctx context.Context, binding egress.Binding, policy egress.Policy) (func() error, error) {
				<-ready
				return service.InstallDirect(ctx, binding, policy)
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	service, err = Open(
		Options{
			StateDir:   t.TempDir(),
			Initialize: true,
			RootToken:  "root-token",
			Engine:     engine,
			Agent:      &fakeAgent{},
			Egress:     manager,
			Interval:   time.Hour,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	defer service.Close()
	principal := Principal{TenantID: "alice"}
	_, err = service.PutTenant(
		context.Background(),
		Principal{Root: true},
		Tenant{
			ID:            principal.TenantID,
			MaxSandboxes:  10,
			MaxStorageMiB: 10000,
			Token:         "tenant-token",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Execute(
		context.Background(),
		principal,
		"template",
		Command{
			Action: "template_put",
			Template: &Template{
				ID:    "test",
				Image: "sha256:" + strings.Repeat("1", 64),
				Shape: api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Execute(
		context.Background(),
		principal,
		"claim",
		Command{
			Action:     "claim",
			TemplateID: "test",
			Policy: egress.Policy{
				Mode:         "direct",
				AllowedCIDRs: []string{"1.1.1.1/32"},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	vm, err := engine.GetVM(context.Background(), result.Sandbox.VMID)
	if err != nil || !vm.NetworkActual || vm.NetworkPolicy == nil ||
		vm.NetworkPolicy.MaxConnections != 256 {
		t.Fatal(
			"direct activation did not commit the default budget and network state",
			err,
		)
	}
	engine.requestMu.Lock()
	policyAt, enabledAt := -1, -1
	for n, request := range engine.requests {
		if request.Action == "network_policy" && request.Policy != nil {
			policyAt = n
		}
		if request.Action == "network" && request.NetworkEnabled {
			enabledAt = n
		}
	}
	engine.failRevoke = true
	engine.requestMu.Unlock()
	if policyAt < 0 || enabledAt <= policyAt {
		t.Fatal("network enabled before policy installation")
	}
	if err = service.revoke(result.Sandbox.ID); err == nil {
		t.Fatal("revocation failure was hidden")
	}
	if _, exists := manager.Binding(result.Sandbox.ID); !exists {
		t.Fatal("failed revocation lost its cleanup binding")
	}
	if err = service.revoke(result.Sandbox.ID); err != nil {
		t.Fatal("terminal cleanup failure could not be retried", err)
	}
	vm, err = engine.GetVM(context.Background(), vm.ID)
	if err != nil || vm.NetworkActual || vm.NetworkPolicy != nil {
		t.Fatal("retry left direct permission active", err)
	}
}

func TestDirectAdmissionRejectsHostnameAndHostCredentials(t *testing.T) {
	s, engine, _, p := fixture(t)
	no := false
	for _, command := range []Command{
		{Policy: egress.Policy{Mode: "direct", AllowedHosts: []string{"example.com"}}},
		{Policy: egress.Policy{Mode: "direct", AllowedCIDRs: []string{"1.1.1.2/24"}}},
		{Policy: egress.Policy{Mode: "direct"}, Env: map[string]EnvVar{"TOKEN": {Value: "host-only", Guest: &no, Inject: &Injection{Hosts: []string{"example.com"}, Header: "Authorization"}}}},
	} {
		command.Action, command.TemplateID = "claim", "python"
		if _, err := s.Submit(
			context.Background(),
			p,
			newID("invalid-direct"),
			command,
		); !isCode(
			err,
			api.Invalid,
		) {
			t.Fatalf("invalid direct request accepted: %v", err)
		}
	}
	if engine.count("create") != 0 {
		t.Fatal("invalid direct policy allocated a VM")
	}
}
