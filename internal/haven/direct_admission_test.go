package haven

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
)

func TestHavenDirectDataAdmissionRevalidatesSavedPolicy(t *testing.T) {
	service, runtime, _ := directFixture(t)
	principal := Principal{
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
	before, err := service.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	runtime.policyMu.Lock()
	runtime.policy[before.VMID] = nil
	runtime.policyMu.Unlock()
	if _, err = service.DataReady(ctx, principal, before.ID); err == nil {
		t.Fatal("data admission accepted missing host policy")
	}
	key := "network-revalidate:" + service.epoch + ":" + before.ID + ":" + claim.ID
	var operation string
	if err = service.store.db.QueryRow("SELECT id FROM operations WHERE scope_kind='tenant' AND scope_id='tenant' AND idem_key=?", key).Scan(&operation); err != nil {
		t.Fatal("network rejection lacks durable revalidation", err)
	}
	if _, err = service.Wait(ctx, principal, operation); err != nil {
		t.Fatal(err)
	}
	after, err := service.DataReady(ctx, principal, before.ID)
	if err != nil || after.Instance != before.Instance || !after.Deadline.Equal(before.Deadline) {
		t.Fatal("saved policy repair changed identity or lease", before, after, err)
	}
	settings, err := loadSettings(ctx, service.store.db, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	newerPolicy := egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.4.4/32",
		},
	}
	if _, err = service.Execute(ctx, principal, "newer", Command{
		Action:    "configure",
		SandboxID: before.ID,
		Policy:    newerPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	runtime.policyMu.Lock()
	count := len(runtime.events)
	runtime.policyMu.Unlock()
	if _, err = service.AdmitConfigure(ctx, principal, "stale-observer", Command{
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
	final, err := service.DataReady(ctx, principal, before.ID)
	if err != nil || !reflect.DeepEqual(final.Policy, newerPolicy) {
		t.Fatal(final, err)
	}
}

func TestHavenDirectRetirementReopensOriginalIntent(t *testing.T) {
	service, runtime, manager := directFixture(t)
	principal := Principal{
		TenantID: "tenant",
	}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	})
	waitFixtureWorker(t, service, claim.ID)
	service.opts.Fault = func(point string) error {
		if point == "direct_retirement_effect" {
			return ErrInterrupted
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := service.Execute(ctx, principal, "none", Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err == nil {
		t.Fatal("interrupted policy retirement became success")
	}
	before, err := loadDirectRetirement(ctx, service.store.db, claim.ID)
	if err != nil || before.State.Phase != "pending" {
		t.Fatal(before, err)
	}
	// A process restart loses proxy callbacks; the durable retirement owns recovery.
	manager.cleanupMu.Lock()
	manager.cleanups = map[string]func() error{}
	manager.cleanupMu.Unlock()
	service.opts.Fault = nil
	service = reopenCloneService(t, service, runtime)
	manager.service = service
	deadline := time.Now().Add(5 * time.Second)
	for {
		step, err := loadDirectRetirement(ctx, service.store.db, claim.ID)
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
	if _, err = service.Execute(ctx, principal, "repair", Command{
		Action:    "configure",
		SandboxID: claim.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err != nil {
		t.Fatal(err)
	}
	original, err := service.GetOperation(ctx, "tenant", claim.ID)
	if err != nil || !reflect.DeepEqual(original, claim) {
		t.Fatal("policy cleanup changed original claim", original, claim, err)
	}
}

func TestHavenDirectAdmissionRejectsHostnameAndHostCredentials(t *testing.T) {
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
			var env map[string]EnvVar
			if kind == "hostname" {
				policy.AllowedHosts = []string{
					"unused.invalid",
				}
			} else {
				guest := false
				env = map[string]EnvVar{
					"PRIVATE": {
						Value: "unused-fixture-value",
						Guest: &guest,
						Inject: &Injection{
							Hosts: []string{
								"unused.invalid",
							},
							Header:      "Authorization",
							Placeholder: "unused-placeholder",
						},
					},
				}
			}
			if _, err := service.AdmitClaim(context.Background(), Principal{
				TenantID: "tenant",
			}, "claim", Command{
				Action:     "claim",
				TemplateID: "cold",
				Env:        env,
				Policy:     policy,
			}); err == nil {
				t.Fatal("unsafe direct input admitted")
			}
			if runtime.count("launch") != 0 {
				t.Fatal("rejected direct input launched resource")
			}
		})
	}
}
