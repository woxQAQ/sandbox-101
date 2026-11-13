package haven

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
)

func TestHavenNoneMaintenancePreservesWorkloadConnections(t *testing.T) {
	for _, tracked := range []bool{false, true} {
		t.Run(map[bool]string{false: "untracked", true: "tracked"}[tracked], func(t *testing.T) {
			service, _, guest, manager := sandboxFixture(t)
			claim := claimSandbox(t, service, "claim", nil, egress.Policy{Mode: "none"})
			principal := Principal{TenantID: "tenant"}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			connection, err := service.Connect(ctx, principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = connection.Close() }()
			guest.mu.Lock()
			peer := guest.peers[len(guest.peers)-1]
			before := len(guest.activations)
			guest.mu.Unlock()
			manager.mu.Lock()
			manager.binds[claim.Result.SandboxID] = nil
			manager.mu.Unlock()
			if tracked {
				service.mu.Lock()
				service.bindings[claim.Result.SandboxID] = "private-residual"
				service.mu.Unlock()
			}
			for range 2 {
				if err = service.Reconcile(); err != nil {
					t.Fatal(err)
				}
				manager.mu.Lock()
				_, bound := manager.binds[claim.Result.SandboxID]
				manager.mu.Unlock()
				if bound {
					t.Fatal("none maintenance retained proxy")
				}
				exchange := make(chan error, 1)
				go func() { _, err := peer.Write([]byte("x")); exchange <- err }()
				if err = connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				var buffer [1]byte
				if _, err = connection.Read(buffer[:]); err != nil || buffer[0] != 'x' {
					t.Fatal("none maintenance closed healthy workload", err)
				}
				if err = <-exchange; err != nil {
					t.Fatal(err)
				}
			}
			guest.mu.Lock()
			after := len(guest.activations)
			guest.mu.Unlock()
			if before != after {
				t.Fatal("healthy none maintenance replayed Admin activation")
			}
		})
	}
}

func TestHavenRestartRebindDoesNotRenewLease(t *testing.T) {
	service, backend, _, manager := sandboxFixture(t)
	principal := Principal{TenantID: "tenant"}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{Mode: "relay", AllowedHosts: []string{"unused.invalid"}})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	before, err := service.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	service = reopenCloneService(t, service, backend)
	manager.mu.Lock()
	_, bound := manager.binds[before.ID]
	manager.mu.Unlock()
	if !bound {
		t.Fatal("restart returned before saved proxy was rebound")
	}
	after, err := service.DataReady(ctx, principal, before.ID)
	if err != nil || after.Instance != before.Instance || after.VMID != before.VMID || !after.Deadline.Equal(before.Deadline) {
		t.Fatal("restart rebind changed identity or lease", before, after, err)
	}
}
