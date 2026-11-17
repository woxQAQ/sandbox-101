package component

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

func TestHavenDirectDataAdmissionRevalidatesSavedPolicy(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, _ := directFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	policy := egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	}
	claim := claimSandbox(t, service, "claim", nil, policy)
	waitFixtureWorker(t, service, claim.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	runtime.policyMu.Lock()
	runtime.policy[before.VMID] = nil
	runtime.policyMu.Unlock()
	if _, err = service.Data.DataReady(ctx, principal, before.ID); err == nil {
		t.Fatal("data admission accepted missing host policy")
	}
	key := "network-revalidate:" + service.Access.Epoch() + ":" + before.ID + ":" + claim.ID
	var acceptedRecord string
	if err = service.State.Database().QueryRow("SELECT id FROM operations WHERE scope_kind='tenant' AND scope_id='tenant' AND idem_key=?", key).Scan(&acceptedRecord); err != nil {
		t.Fatal("network rejection lacks durable revalidation", err)
	}
	if _, err = service.Operations.Wait(ctx, principal, acceptedRecord); err != nil {
		t.Fatal(err)
	}
	after, err := service.Data.DataReady(ctx, principal, before.ID)
	if err != nil || after.Instance != before.Instance || !after.Deadline.Equal(before.Deadline) {
		t.Fatal("saved policy repair changed identity or lease", before, after, err)
	}
	settings, err := sandbox.LoadSettings(ctx, service.State.Database(), before.ID)
	if err != nil {
		t.Fatal(err)
	}
	newerPolicy := egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.4.4/32",
		},
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "newer", operation.Command{
		Action:    "configure",
		SandboxID: before.ID,
		Policy:    newerPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	runtime.policyMu.Lock()
	count := len(runtime.events)
	runtime.policyMu.Unlock()
	if _, err = service.Sandboxes.AdmitConfigure(ctx, principal, "stale-observer", operation.Command{
		Action:             "configure",
		SandboxID:          before.ID,
		ObservedGeneration: settings.Generation,
		Policy:             policy,
	}); err == nil {
		t.Fatal("stale observer superseded current policy")
	}
	runtime.policyMu.Lock()
	current := len(runtime.events)
	runtime.policyMu.Unlock()
	if current != count {
		t.Fatal("stale observer changed host networking")
	}
	final, err := service.Data.DataReady(ctx, principal, before.ID)
	if err != nil || !reflect.DeepEqual(final.Policy, newerPolicy) {
		t.Fatal(final, err)
	}
}

func TestHavenDirectRetirementReopensOriginalIntent(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, manager := directFixtureWithFault(t, func(point string) error {
		if point == "direct_retirement_effect" {
			return errs.ErrInterrupted
		}
		return nil
	})
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	})
	waitFixtureWorker(t, service, claim.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.SandboxRequests.Execute(ctx, principal, "none", operation.Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err == nil {
		t.Fatal("interrupted policy retirement became success")
	}
	before, err := machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
	if err != nil || before.State.Phase != "pending" {
		t.Fatal(before, err)
	}
	// A process restart loses proxy callbacks; the durable retirement owns recovery.
	manager.cleanupMu.Lock()
	manager.cleanups = map[string]func() error{}
	manager.cleanupMu.Unlock()
	service = testapp.ReopenCloneService(t, service, runtime)
	manager.service = service
	deadline := time.Now().Add(5 * time.Second)
	for {
		step, err := machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
		if err != nil {
			t.Fatal(err)
		}
		if step.Status == "succeeded" {
			if step.Hash != before.Hash || !reflect.DeepEqual(step.Input, before.Input) {
				t.Fatal("recovery replaced original policy authority")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("original policy retirement did not recover", step)
		}
		time.Sleep(time.Millisecond)
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
		t.Fatal("policy cleanup changed original claim", original, claim, err)
	}
}

func TestHavenDirectAdmissionRejectsHostnameAndHostCredentials(t *testing.T) {
	parallelLifecycle(t)
	for _, kind := range []string{
		"hostname",
		"host_credential",
	} {
		t.Run(kind, func(t *testing.T) {
			service, runtime, _ := directFixture(t)
			policy := egress.Policy{
				Mode: "direct",
				AllowedCIDRs: []string{
					"8.8.8.8/32",
				},
			}
			var env map[string]operation.EnvVar
			if kind == "hostname" {
				policy.AllowedHosts = []string{
					"unused.invalid",
				}
			} else {
				guest := false
				env = map[string]operation.EnvVar{
					"PRIVATE": {
						Value: "unused-fixture-value",
						Guest: &guest,
						Inject: &authorization.Injection{
							Hosts: []string{
								"unused.invalid",
							},
							Header:      "Authorization",
							Placeholder: "unused-placeholder",
						},
					},
				}
			}
			if _, err := service.Sandboxes.AdmitClaim(context.Background(), authorization.Principal{
				TenantID: "tenant",
			}, "claim", operation.Command{
				Action:     "claim",
				TemplateID: "cold",
				Env:        env,
				Policy:     policy,
			}); err == nil {
				t.Fatal("unsafe direct input admitted")
			}
			if runtime.Count("launch") != 0 {
				t.Fatal("rejected direct input launched resource")
			}
		})
	}
}
