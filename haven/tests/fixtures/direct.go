package fixtures

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
)

type directRuntime struct {
	*RestorableRuntime

	PolicyMu    sync.Mutex
	Policy      map[string]*loom.NetworkPolicy
	Events      []string
	RejectClear bool
}

func (runtime *directRuntime) record(event string) {
	runtime.PolicyMu.Lock()
	runtime.Events = append(runtime.Events, event)
	runtime.PolicyMu.Unlock()
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
	runtime.PolicyMu.Lock()
	defer runtime.PolicyMu.Unlock()
	if policy == nil && runtime.RejectClear {
		return fmt.Errorf("private policy clear failure")
	}
	runtime.Events = append(runtime.Events, "policy")
	var copy *loom.NetworkPolicy
	if policy != nil {
		value := *policy
		value.AllowedCIDRs = append([]string(nil), policy.AllowedCIDRs...)
		copy = &value
	}
	runtime.Policy[resource.VMID] = copy
	return nil
}

func (runtime *directRuntime) VerifyNetworkPolicy(_ context.Context, resource loom.Resource, expected *loom.NetworkPolicy) error {
	runtime.PolicyMu.Lock()
	defer runtime.PolicyMu.Unlock()
	if !reflect.DeepEqual(runtime.Policy[resource.VMID], expected) {
		return fmt.Errorf("private policy verification mismatch")
	}
	return nil
}

type directEgress struct {
	*testfixture.EgressFixture

	Service   *application.App
	CleanupMu sync.Mutex
	Cleanups  map[string]func() error
}

func (manager *directEgress) Bind(ctx context.Context, target egress.BindingTarget, policy egress.Policy, secrets map[string]egress.Secret) error {
	id, tenant, path := target.SandboxID, target.TenantID, target.VSockPath
	if policy.Mode == "direct" {
		cleanup, err := manager.Service.DirectBindings.InstallDirect(ctx, egress.Binding{
			SandboxID: id,
			TenantID:  tenant,
			VSockPath: path,
			Mode:      "direct",
		}, policy)
		if err != nil {
			return err
		}
		manager.CleanupMu.Lock()
		manager.Cleanups[id] = cleanup
		manager.CleanupMu.Unlock()
	}
	return manager.EgressFixture.Bind(ctx, target, policy, secrets)
}

func (manager *directEgress) Revoke(id string) error {
	err := manager.EgressFixture.Revoke(id)
	manager.CleanupMu.Lock()
	defer manager.CleanupMu.Unlock()
	if cleanup := manager.Cleanups[id]; cleanup != nil {
		if failure := cleanup(); failure != nil {
			return errors.Join(err, failure)
		}
		delete(manager.Cleanups, id)
	}
	return err
}

func (manager *directEgress) Close() error {
	manager.CleanupMu.Lock()
	ids := make([]string, 0, len(manager.Cleanups))
	for id := range manager.Cleanups {
		ids = append(ids, id)
	}
	manager.CleanupMu.Unlock()
	var result error
	for _, id := range ids {
		result = errors.Join(result, manager.Revoke(id))
	}
	return errors.Join(result, manager.EgressFixture.Close())
}

func DirectFixture(t *testing.T) (*application.App, *directRuntime, *directEgress) {
	t.Helper()
	return DirectFixtureWithFault(t, nil)
}

func DirectFixtureWithFault(t *testing.T, fault func(string) error) (*application.App, *directRuntime, *directEgress) {
	t.Helper()
	guest := &testfixture.WorkloadFixture{}
	manager := &directEgress{
		EgressFixture: &testfixture.EgressFixture{
			Binds: map[string]map[string]egress.Secret{},
		},
		Cleanups: map[string]func() error{},
	}
	service, _ := testapp.ColdServiceWithOptions(t, application.Options{
		Fault:     fault,
		Guest:     guest,
		Egress:    manager,
		RootToken: "fixture-root-token",
	}, func(backend *testfixture.ColdRuntime) loom.Backend {
		return &directRuntime{
			RestorableRuntime: &RestorableRuntime{backend},
			Policy:            map[string]*loom.NetworkPolicy{},
		}
	})
	runtime := service.Options.Backend.(*directRuntime)
	manager.Service = service
	t.Cleanup(guest.Close)
	guest.Before = func(name string, _ loom.Resource) error {
		if name == "activate" {
			runtime.record("admin")
		}
		return nil
	}
	return service, runtime, manager
}
