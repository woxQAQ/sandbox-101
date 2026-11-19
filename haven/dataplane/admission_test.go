package dataplane_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenDataReadinessRenewsWithoutRenewingAttachedStreams(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	accepted, err := service.SandboxRequests.Execute(ctx, principal, "auto-renew-stream", operation.Command{
		Action:     "claim",
		TemplateID: "cold",
		TTLSeconds: 60,
		AutoRenew:  true,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := accepted.Result.SandboxID
	initial, err := service.Catalog.Get(ctx, principal, id)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := service.Dataplane.Ready(ctx, principal, id)
	if err != nil || !ready.Deadline.After(initial.Deadline) {
		t.Fatal("fresh readiness did not renew the lease", initial.Deadline, ready.Deadline, err)
	}
	upstream, err := service.Dataplane.Connect(ctx, principal, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = upstream.Close()
	}()
	connected, err := service.Catalog.Get(ctx, principal, id)
	if err != nil || !connected.Deadline.After(ready.Deadline) {
		t.Fatal("fresh connection readiness did not renew the lease", connected.Deadline, err)
	}
	one, two := net.Pipe()
	defer func() {
		_ = one.Close()
	}()
	defer func() {
		_ = two.Close()
	}()
	downstream, err := service.Dataplane.Attach(ctx, principal, dataplane.HandoffRequest{
		SandboxID: id,
		Upstream:  upstream,
		Accept: func() (net.Conn, error) {
			return one, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = downstream.Close()
	}()
	attached, err := service.Catalog.Get(ctx, principal, id)
	if err != nil || !attached.Deadline.Equal(connected.Deadline) {
		t.Fatal("downstream handoff renewed the original upstream lease", attached.Deadline, err)
	}
	// Keep both endpoints registered across multiple enforcement intervals.
	select {
	case <-upstream.(*ephemeral.Connection).Done():
		t.Fatal("valid upstream was revoked during lease enforcement")
	case <-downstream.(*ephemeral.Connection).Done():
		t.Fatal("valid downstream was revoked during lease enforcement")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(350 * time.Millisecond):
	}
	observed, err := service.Catalog.Get(ctx, principal, id)
	if err != nil || !observed.Deadline.Equal(connected.Deadline) {
		t.Fatal("an attached stream renewed its lease during enforcement", observed.Deadline, err)
	}
}

func TestHavenDownstreamHandoffRetainsOriginalConfiguration(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	acceptedRecord := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "handoff", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	upstream, err := service.Dataplane.Connect(ctx, principal, acceptedRecord.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = upstream.Close()
	}()
	if _, err = service.SandboxRequests.Execute(ctx, principal, "handoff-new-configuration", operation.Command{
		Action:    "configure",
		SandboxID: acceptedRecord.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
		Env: map[string]operation.EnvVar{
			"VERSION": {
				Value: "next",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	accepted := false
	downstream, err := service.Dataplane.Attach(ctx, principal, dataplane.HandoffRequest{
		SandboxID: acceptedRecord.Result.SandboxID,
		Upstream:  upstream,
		Accept: func() (net.Conn, error) {
			accepted = true
			one, two := net.Pipe()
			_ = two.Close()
			return one, nil
		},
	})
	if !errors.Is(err, errs.ErrConflict) || accepted || downstream != nil {
		t.Fatal("late HTTP handoff accepted a superseded upstream", accepted, err)
	}
}

func TestHavenDataAdmissionRepeatsFinalGuard(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, change := range []string{
		"configuration",
		"configuration_provenance",
		"unverified_instance",
		"deadline",
		"shutdown",
	} {
		t.Run(change, func(t *testing.T) {
			service, _, guest, _ := testapp.SandboxFixture(t)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			claim := (fixtures.SandboxClaims{
				App: service,
			}).Claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			fixtures.WaitFixtureWorker(t, service, claim.ID)
			entered, release := make(chan struct{}), make(chan struct{})
			guest.Before = func(name string, _ loom.Resource) error {
				if name == "connect" {
					close(entered)
					<-release
				}
				return nil
			}
			type result struct {
				connection net.Conn
				err        error
			}
			done := make(chan result, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			go func() {
				connection, err := service.Dataplane.Connect(ctx, principal, claim.Result.SandboxID)
				done <- result{connection, err}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			sandboxView, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "configuration":
				if _, err = service.SandboxRequests.Execute(ctx, principal, "configure", operation.Command{
					Action:    "configure",
					SandboxID: sandboxView.ID,
					Env: map[string]operation.EnvVar{
						"REVISION": {
							Value: "new",
						},
					},
					Policy: egress.Policy{
						Mode: "none",
					},
				}); err != nil {
					t.Fatal(err)
				}
			case "configuration_provenance":
				settings, err := sandbox.LoadSettings(ctx, service.State.Database(), sandboxView.ID)
				if err != nil {
					t.Fatal(err)
				}
				settings.Env["UNPUBLISHED"] = "private-fixture"
				if _, err = service.State.Database().Exec("UPDATE sandbox_settings SET body=? WHERE sandbox_id=?", testfixture.Encode(settings), sandboxView.ID); err != nil {
					t.Fatal(err)
				}
			case "unverified_instance":
				source, err := service.Catalog.ResourceForData(ctx, sandboxView)
				if err != nil {
					t.Fatal(err)
				}
				vm, _, _, err := machine.LoadVM(ctx, service.State.Database(), source)
				if err != nil {
					t.Fatal(err)
				}
				vm.Verified = false
				if _, err = service.State.Database().Exec("UPDATE vms SET body=? WHERE id=?", testfixture.Encode(vm), sandboxView.VMID); err != nil {
					t.Fatal(err)
				}
			case "deadline":
				settings, err := sandbox.LoadSettings(ctx, service.State.Database(), sandboxView.ID)
				if err != nil {
					t.Fatal(err)
				}
				settings.Deadline = time.Now().Add(-time.Second)
				if _, err = service.State.Database().Exec("UPDATE sandbox_settings SET body=? WHERE sandbox_id=?", testfixture.Encode(settings), sandboxView.ID); err != nil {
					t.Fatal(err)
				}
			case "shutdown":
				shutdownCtx, stop := context.WithTimeout(ctx, 5*time.Millisecond)
				defer stop()
				_ = service.Close(shutdownCtx)
				if unexpected, err := fixtures.OpenStore(service.State.Directory(), false); !errors.Is(err, store.ErrConflict) {
					if unexpected != nil {
						_ = unexpected.Close()
					}
					t.Fatal("shutdown did not retain writer during pending data call", err)
				}
			}
			close(release)
			select {
			case result := <-done:
				if result.connection != nil {
					_ = result.connection.Close()
					t.Fatal("late connection escaped final admission guard")
				}
				if result.err == nil {
					t.Fatal("late connection lacked rejection")
				}
			case <-ctx.Done():
				t.Fatal(fmt.Errorf("data admission did not finish: %w", ctx.Err()))
			}
		})
	}
}
