package testfixture

import (
	"context"
	"net"
	"sync"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/loom"
)

type WorkloadFixture struct {
	Mu          sync.Mutex
	Activations []guest.Activation
	Before      func(string, loom.Resource) error
	Peers       []net.Conn
}

func (guest *WorkloadFixture) effect(name string, resource loom.Resource) error {
	if guest.Before != nil {
		return guest.Before(name, resource)
	}
	return nil
}

func (guest *WorkloadFixture) Info(_ context.Context, resource loom.Resource) error {
	return guest.effect("info", resource)
}

func (guest *WorkloadFixture) Activate(_ context.Context, resource loom.Resource, activation guest.Activation) error {
	if err := guest.effect("activate", resource); err != nil {
		return err
	}
	guest.Mu.Lock()
	guest.Activations = append(guest.Activations, activation)
	guest.Mu.Unlock()
	return nil
}

func (guest *WorkloadFixture) PrepareTemplate(_ context.Context, resource loom.Resource) error {
	return guest.effect("prepare", resource)
}

func (guest *WorkloadFixture) Exec(_ context.Context, resource loom.Resource, _, _ string) error {
	return guest.effect("exec", resource)
}

func (guest *WorkloadFixture) Connect(_ context.Context, resource loom.Resource) (net.Conn, error) {
	if err := guest.effect("connect", resource); err != nil {
		return nil, err
	}
	one, two := net.Pipe()
	guest.Mu.Lock()
	guest.Peers = append(guest.Peers, two)
	guest.Mu.Unlock()
	return one, nil
}

func (guest *WorkloadFixture) Forward(ctx context.Context, resource loom.Resource, _ int) (net.Conn, error) {
	return guest.Connect(ctx, resource)
}

func (guest *WorkloadFixture) Close() {
	guest.Mu.Lock()
	defer guest.Mu.Unlock()
	for _, conn := range guest.Peers {
		_ = conn.Close()
	}
}

type EgressFixture struct {
	Mu     sync.Mutex
	Binds  map[string]map[string]egress.Secret
	Before func(string) error
}

func (manager *EgressFixture) Bind(_ context.Context, target egress.BindingTarget, _ egress.Policy, secrets map[string]egress.Secret) error {
	if manager.Before != nil {
		if err := manager.Before("bind"); err != nil {
			return err
		}
	}
	manager.Mu.Lock()
	manager.Binds[target.SandboxID] = secrets
	manager.Mu.Unlock()
	return nil
}

func (manager *EgressFixture) Revoke(id string) error {
	if manager.Before != nil {
		if err := manager.Before("revoke"); err != nil {
			return err
		}
	}
	manager.Mu.Lock()
	delete(manager.Binds, id)
	manager.Mu.Unlock()
	return nil
}

func (manager *EgressFixture) Close() error {
	manager.Mu.Lock()
	manager.Binds = map[string]map[string]egress.Secret{}
	manager.Mu.Unlock()
	return nil
}
