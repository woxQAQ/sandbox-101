package component

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

type directRuntime struct {
	*restorableRuntime

	policyMu    sync.Mutex
	policy      map[string]*loom.NetworkPolicy
	events      []string
	rejectClear bool
}

func (runtime *directRuntime) record(event string) {
	runtime.policyMu.Lock()
	runtime.events = append(runtime.events, event)
	runtime.policyMu.Unlock()
}

func (runtime *directRuntime) Network(ctx context.Context, resource loom.Resource, enabled bool) error {
	event := "off"
	if enabled {
		event = "on"
	}
	runtime.record(event)
	return runtime.ColdRuntime.Network(ctx, resource, enabled)
}

func (runtime *directRuntime) InstallNetworkPolicy(_ context.Context, resource loom.Resource, policy *loom.NetworkPolicy) error {
	runtime.policyMu.Lock()
	defer runtime.policyMu.Unlock()
	if policy == nil && runtime.rejectClear {
		return fmt.Errorf("private policy clear failure")
	}
	runtime.events = append(runtime.events, "policy")
	var copy *loom.NetworkPolicy
	if policy != nil {
		value := *policy
		value.AllowedCIDRs = append([]string(nil), policy.AllowedCIDRs...)
		copy = &value
	}
	runtime.policy[resource.VMID] = copy
	return nil
}

func (runtime *directRuntime) VerifyNetworkPolicy(_ context.Context, resource loom.Resource, expected *loom.NetworkPolicy) error {
	runtime.policyMu.Lock()
	defer runtime.policyMu.Unlock()
	if !reflect.DeepEqual(runtime.policy[resource.VMID], expected) {
		return fmt.Errorf("private policy verification mismatch")
	}
	return nil
}

type directEgress struct {
	*testfixture.EgressFixture

	service   *application.App
	cleanupMu sync.Mutex
	cleanups  map[string]func() error
}

func (manager *directEgress) Bind(ctx context.Context, target egress.BindingTarget, policy egress.Policy, secrets map[string]egress.Secret) error {
	id, tenant, path := target.SandboxID, target.TenantID, target.VSockPath
	if policy.Mode == "direct" {
		cleanup, err := manager.service.DirectBindings.InstallDirect(ctx, egress.Binding{
			SandboxID: id,
			TenantID:  tenant,
			VSockPath: path,
			Mode:      "direct",
		}, policy)
		if err != nil {
			return err
		}
		manager.cleanupMu.Lock()
		manager.cleanups[id] = cleanup
		manager.cleanupMu.Unlock()
	}
	return manager.EgressFixture.Bind(ctx, target, policy, secrets)
}

func (manager *directEgress) Revoke(id string) error {
	err := manager.EgressFixture.Revoke(id)
	manager.cleanupMu.Lock()
	defer manager.cleanupMu.Unlock()
	if cleanup := manager.cleanups[id]; cleanup != nil {
		if failure := cleanup(); failure != nil {
			return errors.Join(err, failure)
		}
		delete(manager.cleanups, id)
	}
	return err
}

func (manager *directEgress) Close() error {
	manager.cleanupMu.Lock()
	ids := make([]string, 0, len(manager.cleanups))
	for id := range manager.cleanups {
		ids = append(ids, id)
	}
	manager.cleanupMu.Unlock()
	var result error
	for _, id := range ids {
		result = errors.Join(result, manager.Revoke(id))
	}
	return errors.Join(result, manager.EgressFixture.Close())
}

func directFixture(t *testing.T) (*application.App, *directRuntime, *directEgress) {
	t.Helper()
	return directFixtureWithFault(t, nil)
}

func directFixtureWithFault(t *testing.T, fault func(string) error) (*application.App, *directRuntime, *directEgress) {
	t.Helper()
	guest := &testfixture.WorkloadFixture{}
	manager := &directEgress{
		EgressFixture: &testfixture.EgressFixture{
			Binds: map[string]map[string]egress.Secret{},
		},
		cleanups: map[string]func() error{},
	}
	service, _ := testapp.ColdServiceWithOptions(t, application.Options{
		Fault:     fault,
		Guest:     guest,
		Egress:    manager,
		RootToken: "fixture-root-token",
	}, func(backend *testfixture.ColdRuntime) loom.Backend {
		return &directRuntime{
			restorableRuntime: &restorableRuntime{backend},
			policy:            map[string]*loom.NetworkPolicy{},
		}
	})
	runtime := service.Options.Backend.(*directRuntime)
	manager.service = service
	t.Cleanup(guest.Close)
	guest.Before = func(name string, _ loom.Resource) error {
		if name == "activate" {
			runtime.record("admin")
		}
		return nil
	}
	return service, runtime, manager
}

func TestHavenDirectActivationAndExactRetirement(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, manager := directFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	direct := egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	}
	claim := (sandboxClaims{
		App: service,
	}).claim(t, "claim", nil, direct)
	waitFixtureWorker(t, service, claim.ID)
	runtime.policyMu.Lock()
	events := append([]string(nil), runtime.events...)
	runtime.policyMu.Unlock()
	indexes := map[string]int{}
	for index, event := range events {
		if _, found := indexes[event]; !found {
			indexes[event] = index
		}
	}
	if indexes["off"] >= indexes["policy"] || indexes["policy"] >= indexes["admin"] || indexes["admin"] >= indexes["on"] {
		t.Fatal("direct activation order changed", events)
	}
	manager.cleanupMu.Lock()
	oldCleanup := manager.cleanups[claim.Result.SandboxID]
	manager.cleanupMu.Unlock()
	if oldCleanup == nil {
		t.Fatal("direct binding lacks frozen cleanup")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.SandboxRequests.Execute(ctx, principal, "none", operation.Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err != nil {
		t.Fatal(err)
	}
	old, err := machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
	if err != nil || old.Status != "succeeded" {
		t.Fatal(old, err)
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "direct", operation.Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy:    direct,
	}); err != nil {
		t.Fatal(err)
	}
	runtime.policyMu.Lock()
	before := len(runtime.events)
	runtime.policyMu.Unlock()
	if err = oldCleanup(); err != nil {
		t.Fatal(err)
	}
	runtime.policyMu.Lock()
	after := len(runtime.events)
	runtime.policyMu.Unlock()
	if before != after {
		t.Fatal("old binding cleanup mutated successor policy", before, after)
	}
	sandbox, err := service.Dataplane.Ready(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	source, err := service.Catalog.ResourceForData(ctx, sandbox)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := runtime.Inspect(ctx, source.Input.Resource)
	if err != nil || !actual.Network {
		t.Fatal("old cleanup disabled successor", actual, err)
	}
}

func TestHavenDirectRetirementUnknownRetainsFence(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, _ := directFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (sandboxClaims{
		App: service,
	}).claim(t, "claim", nil, egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	})
	waitFixtureWorker(t, service, claim.ID)
	runtime.policyMu.Lock()
	runtime.rejectClear = true
	runtime.policyMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	failed, err := service.SandboxRequests.Execute(ctx, principal, "none", operation.Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err == nil || failed.Status != "failed" {
		t.Fatal(failed, err)
	}
	retirement, err := machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
	if err != nil || retirement.State.Phase != "pending" || retirement.Status != "running" {
		t.Fatal(retirement, err)
	}
	if _, err = service.VMDeletion.AdmitRelease(ctx, machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "bypass",
		SandboxID: claim.Result.SandboxID,
	}); err == nil {
		t.Fatal("resource admission passed unresolved direct retirement")
	}
	runtime.policyMu.Lock()
	runtime.rejectClear = false
	runtime.policyMu.Unlock()
	if err = service.Maintenance.Reconcile(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		retirement, err = machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
		if err != nil {
			t.Fatal(err)
		}
		if retirement.Status == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("direct retirement did not resume", retirement)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err = service.Dataplane.Ready(ctx, principal, claim.Result.SandboxID); err == nil {
		t.Fatal("retirement removed failed configuration fence")
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "repair", operation.Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err != nil {
		t.Fatal(err)
	}
	original, err := service.Operations.GetOperation(ctx, "tenant", claim.ID)
	if err != nil || !reflect.DeepEqual(original, claim) {
		t.Fatal("independent retirement changed claim result", original, claim, err)
	}
}
