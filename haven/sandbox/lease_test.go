package sandbox_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

func TestHavenRenewRejectedWriteKeepsLeaseAndRequest(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, state := range []string{
		"running",
		"paused",
	} {
		for _, rejection := range []string{
			"ABORT",
			"IGNORE",
		} {
			t.Run(state+"/"+rejection, func(t *testing.T) {
				service, _, _, _ := testapp.SandboxFixture(t)
				principal := authorization.Principal{
					TenantID: "tenant",
				}
				claim := (testfixture.SandboxClaims{
					App: service,
				}).Claim(t, "claim", nil, egress.Policy{
					Mode: "none",
				})
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				id := claim.Result.SandboxID
				if state == "paused" {
					if _, err := service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
						Action:    "pause",
						SandboxID: id,
					}); err != nil {
						t.Fatal(err)
					}
				}
				before, err := sandbox.LoadSettings(ctx, service.State.Database(), id)
				if err != nil {
					t.Fatal(err)
				}
				raise := "RAISE(IGNORE)"
				if rejection == "ABORT" {
					raise = "RAISE(ABORT,'lease write rejected')"
				}
				if _, err = service.State.Database().ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER reject_renew BEFORE UPDATE OF body ON sandbox_settings BEGIN SELECT %s; END", raise)); err != nil {
					t.Fatal(err)
				}
				command := operation.Command{
					Action:     "renew",
					SandboxID:  id,
					TTLSeconds: 120,
				}
				if accepted, err := service.Lifecycle.AdmitRenew(ctx, principal, "renew-held", command); err == nil || accepted.ID != "" {
					t.Fatal("rejected lease write admitted a request", accepted, err)
				}
				after, err := sandbox.LoadSettings(ctx, service.State.Database(), id)
				if err != nil || !reflect.DeepEqual(after, before) {
					t.Fatal("rejected renewal changed lease or configuration", after, err)
				}
				var requests int
				if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM operations WHERE scope_kind='tenant' AND scope_id='tenant' AND idem_key='renew-held'").Scan(&requests); err != nil || requests != 0 {
					t.Fatal("rejected renewal committed an operation", requests, err)
				}
				if _, err = service.State.Database().ExecContext(ctx, "DROP TRIGGER reject_renew"); err != nil {
					t.Fatal(err)
				}
				accepted, err := service.Lifecycle.AdmitRenew(ctx, principal, "renew-held", command)
				if err != nil || accepted.Status != "succeeded" {
					t.Fatal("repaired renewal failed", accepted, err)
				}
				after, err = sandbox.LoadSettings(ctx, service.State.Database(), id)
				if err != nil || after.Generation != before.Generation || after.ActiveEpoch != before.ActiveEpoch || after.Instance != before.Instance || reflect.DeepEqual(after, before) {
					t.Fatal("renewal changed identity or did not update the lease", after, err)
				}
				replay, err := service.Lifecycle.AdmitRenew(ctx, principal, "renew-held", command)
				if err != nil || replay.ID != accepted.ID {
					t.Fatal("renewal replay changed identity", replay, err)
				}
				replayed, err := sandbox.LoadSettings(ctx, service.State.Database(), id)
				if err != nil || !reflect.DeepEqual(replayed, after) {
					t.Fatal("renewal replay extended the lease again", replayed, err)
				}
			})
		}
	}
}

func TestHavenRenewPreservesConfiguration(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	id := claim.Result.SandboxID
	before, err := sandbox.LoadSettings(context.Background(), service.State.Database(), id)
	if err != nil {
		t.Fatal(err)
	}
	acceptedRecord, err := service.Lifecycle.AdmitRenew(context.Background(), authorization.Principal{
		TenantID:  "tenant",
		SandboxID: id,
	}, "renew", operation.Command{
		Action:     "renew",
		SandboxID:  id,
		TTLSeconds: 120,
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := sandbox.LoadSettings(context.Background(), service.State.Database(), id)
	if err != nil || !after.Deadline.After(before.Deadline) || after.Generation != before.Generation || after.ActiveEpoch != before.ActiveEpoch || after.Instance != before.Instance {
		t.Fatal(before, after, err)
	}
	replay, err := service.Lifecycle.AdmitRenew(context.Background(), authorization.Principal{
		TenantID:  "tenant",
		SandboxID: id,
	}, "renew", operation.Command{
		Action:     "renew",
		SandboxID:  id,
		TTLSeconds: 120,
	})
	if err != nil || replay.ID != acceptedRecord.ID {
		t.Fatal(replay, err)
	}
	if _, err = service.Lifecycle.AdmitRenew(context.Background(), authorization.Principal{
		TenantID: "other",
	}, "renew", operation.Command{
		Action:    "renew",
		SandboxID: id,
	}); err == nil {
		t.Fatal("foreign renew accepted")
	}
	if _, err = service.Lifecycle.AdmitLifecycle(context.Background(), principal, "stale-expiration", operation.Command{
		Action:        "release",
		SandboxID:     id,
		LeaseDeadline: before.Deadline,
	}); err == nil {
		t.Fatal("stale deadline revoked renewed lease")
	}
	if _, err = service.Dataplane.Ready(context.Background(), principal, id); err != nil {
		t.Fatal(err)
	}
}

func TestHavenLeaseReaper(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, state := range []string{
		"kill",
		"pause",
		"paused",
	} {
		t.Run(state, func(t *testing.T) {
			service, _, _, _ := testapp.SandboxFixture(t)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			claim := (testfixture.SandboxClaims{
				App: service,
			}).Claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			id := claim.Result.SandboxID
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if state == "paused" {
				if _, err := service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
					Action:    "pause",
					SandboxID: id,
				}); err != nil {
					t.Fatal(err)
				}
			}
			settings, err := sandbox.LoadSettings(ctx, service.State.Database(), id)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().UTC().Add(-time.Second)
			if state == "paused" {
				settings.PausedUntil = deadline
			} else {
				settings.Deadline = deadline
				settings.OnExpire = state
			}
			if err = testutil.RequireWrite(service.State.Database().ExecContext(ctx, "UPDATE sandbox_settings SET body=? WHERE sandbox_id=?", operation.Encode(settings), id)); err != nil {
				t.Fatal(err)
			}
			if _, err = service.Lifecycle.AdmitRenew(ctx, principal, "late-renew", operation.Command{
				Action:    "renew",
				SandboxID: id,
			}); err == nil {
				t.Fatal("expired lease revived")
			}
			if err = service.Lifecycle.Reap(ctx); err != nil {
				t.Fatal(err)
			}
			var acceptedRecord string
			if err = service.State.Database().QueryRowContext(ctx, "SELECT id FROM operations WHERE scope_id='tenant' AND idem_key=?", "expire:"+id+":"+deadline.Format(time.RFC3339Nano)).Scan(&acceptedRecord); err != nil {
				t.Fatal(err)
			}
			if _, err = service.Operations.Wait(ctx, principal, acceptedRecord); err != nil {
				t.Fatal(err)
			}
			sandboxView, err := service.Catalog.Get(ctx, principal, id)
			want := "terminated"
			if state == "pause" {
				want = "paused"
			}
			if err != nil || sandboxView.State != want {
				t.Fatal(sandboxView, err)
			}
		})
	}
}
