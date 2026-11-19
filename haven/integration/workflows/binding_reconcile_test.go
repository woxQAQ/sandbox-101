package workflows_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenNoneMaintenancePreservesWorkloadConnections(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, tracked := range []bool{
		false,
		true,
	} {
		t.Run(map[bool]string{
			false: "untracked",
			true:  "tracked",
		}[tracked], func(t *testing.T) {
			service, _, guest, manager := testapp.SandboxFixture(t)
			claim := (fixtures.SandboxClaims{
				App: service,
			}).Claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			connection, err := service.Dataplane.Connect(ctx, principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = connection.Close()
			}()
			guest.Mu.Lock()
			peer := guest.Peers[len(guest.Peers)-1]
			before := len(guest.Activations)
			guest.Mu.Unlock()
			manager.Mu.Lock()
			manager.Binds[claim.Result.SandboxID] = nil
			manager.Mu.Unlock()
			if tracked {
				service.Ephemeral.SetBinding(claim.Result.SandboxID, "private-residual")
			}
			for range 2 {
				if err = service.Maintenance.Reconcile(); err != nil {
					t.Fatal(err)
				}
				manager.Mu.Lock()
				_, bound := manager.Binds[claim.Result.SandboxID]
				manager.Mu.Unlock()
				if bound {
					t.Fatal("none maintenance retained proxy")
				}
				exchange := make(chan error, 1)
				go func() {
					_, err := peer.Write([]byte("x"))
					exchange <- err
				}()
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
			guest.Mu.Lock()
			after := len(guest.Activations)
			guest.Mu.Unlock()
			if before != after {
				t.Fatal("healthy none maintenance replayed Admin activation")
			}
		})
	}
}

func TestHavenBindingMaintenanceRetainsDiagnosticWithoutChangingOperation(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, _, manager := testapp.SandboxFixture(t)
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "binding-diagnostic", nil, egress.Policy{
		Mode: "none",
	})
	failure := errors.New("fixture cannot revoke residual binding")
	manager.Before = func(name string) error {
		if name == "revoke" {
			return failure
		}
		return nil
	}
	if err := service.Maintenance.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if err := service.Ephemeral.BindingError(claim.Result.SandboxID); !errors.Is(err, failure) {
		t.Fatal("binding maintenance lost its diagnostic", err)
	}
	manager.Before = nil
	if err := service.Maintenance.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if err := service.Ephemeral.BindingError(claim.Result.SandboxID); !errors.Is(err, failure) {
		t.Fatal("successful maintenance erased the last failure", err)
	}
	record, err := service.Operations.Operation(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, claim.ID)
	if err != nil || record.Status != "succeeded" || service.Workers.State(claim.ID).Err != nil {
		t.Fatal("binding diagnostic changed an accepted operation", record, err)
	}
}

func TestHavenRestartRebindDoesNotRenewLease(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, backend, _, manager := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "proxy",
		AllowedHosts: []string{
			"unused.invalid",
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	manager.Mu.Lock()
	_, bound := manager.Binds[before.ID]
	manager.Mu.Unlock()
	if !bound {
		t.Fatal("restart returned before saved proxy was rebound")
	}
	after, err := service.Dataplane.Ready(ctx, principal, before.ID)
	if err != nil || after.Instance != before.Instance || after.VMID != before.VMID || !after.Deadline.Equal(before.Deadline) {
		t.Fatal("restart rebind changed identity or lease", before, after, err)
	}
}
