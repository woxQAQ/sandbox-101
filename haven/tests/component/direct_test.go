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
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenDirectActivationAndExactRetirement(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, runtime, manager := fixtures.DirectFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	direct := egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	}
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, direct)
	fixtures.WaitFixtureWorker(t, service, claim.ID)
	runtime.PolicyMu.Lock()
	events := append([]string(nil), runtime.Events...)
	runtime.PolicyMu.Unlock()
	indexes := map[string]int{}
	for index, event := range events {
		if _, found := indexes[event]; !found {
			indexes[event] = index
		}
	}
	if indexes["off"] >= indexes["policy"] || indexes["policy"] >= indexes["admin"] || indexes["admin"] >= indexes["on"] {
		t.Fatal("direct activation order changed", events)
	}
	manager.CleanupMu.Lock()
	oldCleanup := manager.Cleanups[claim.Result.SandboxID]
	manager.CleanupMu.Unlock()
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
	runtime.PolicyMu.Lock()
	before := len(runtime.Events)
	runtime.PolicyMu.Unlock()
	if err = oldCleanup(); err != nil {
		t.Fatal(err)
	}
	runtime.PolicyMu.Lock()
	after := len(runtime.Events)
	runtime.PolicyMu.Unlock()
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
	fixtures.ParallelLifecycle(t)
	service, runtime, _ := fixtures.DirectFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	})
	fixtures.WaitFixtureWorker(t, service, claim.ID)
	runtime.PolicyMu.Lock()
	runtime.RejectClear = true
	runtime.PolicyMu.Unlock()
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
	runtime.PolicyMu.Lock()
	runtime.RejectClear = false
	runtime.PolicyMu.Unlock()
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

func TestHavenDirectDataAdmissionRevalidatesSavedPolicy(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, runtime, _ := fixtures.DirectFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	policy := egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	}
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, policy)
	fixtures.WaitFixtureWorker(t, service, claim.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	runtime.PolicyMu.Lock()
	runtime.Policy[before.VMID] = nil
	runtime.PolicyMu.Unlock()
	if _, err = service.Dataplane.Ready(ctx, principal, before.ID); err == nil {
		t.Fatal("data admission accepted missing host policy")
	}
	key := "network-revalidate:" + service.Ephemeral.Epoch() + ":" + before.ID + ":" + claim.ID
	var acceptedRecord string
	if err = service.State.Database().QueryRow("SELECT id FROM operations WHERE scope_kind='tenant' AND scope_id='tenant' AND idem_key=?", key).Scan(&acceptedRecord); err != nil {
		t.Fatal("network rejection lacks durable revalidation", err)
	}
	if _, err = service.Operations.Wait(ctx, principal, acceptedRecord); err != nil {
		t.Fatal(err)
	}
	after, err := service.Dataplane.Ready(ctx, principal, before.ID)
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
	runtime.PolicyMu.Lock()
	count := len(runtime.Events)
	runtime.PolicyMu.Unlock()
	if _, err = service.Sandboxes.AdmitConfigure(ctx, principal, "stale-observer", operation.Command{
		Action:             "configure",
		SandboxID:          before.ID,
		ObservedGeneration: settings.Generation,
		Policy:             policy,
	}); err == nil {
		t.Fatal("stale observer superseded current policy")
	}
	runtime.PolicyMu.Lock()
	current := len(runtime.Events)
	runtime.PolicyMu.Unlock()
	if current != count {
		t.Fatal("stale observer changed host networking")
	}
	final, err := service.Dataplane.Ready(ctx, principal, before.ID)
	if err != nil || !reflect.DeepEqual(final.Policy, newerPolicy) {
		t.Fatal(final, err)
	}
}

func TestHavenDirectRetirementReopensOriginalIntent(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, runtime, manager := fixtures.DirectFixtureWithFault(t, func(point string) error {
		if point == "direct_retirement_effect" {
			return errs.ErrInterrupted
		}
		return nil
	})
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	})
	fixtures.WaitFixtureWorker(t, service, claim.ID)
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
	manager.CleanupMu.Lock()
	manager.Cleanups = map[string]func() error{}
	manager.CleanupMu.Unlock()
	service = testapp.ReopenCloneService(t, service, runtime)
	manager.Service = service
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
	fixtures.ParallelLifecycle(t)
	for _, kind := range []string{
		"hostname",
		"host_credential",
	} {
		t.Run(kind, func(t *testing.T) {
			service, runtime, _ := fixtures.DirectFixture(t)
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

func TestHavenDirectOldBindingCannotRevokeResumedInstance(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, runtime, manager := fixtures.DirectFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (fixtures.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "direct",
		AllowedCIDRs: []string{
			"8.8.8.8/32",
		},
	})
	fixtures.WaitFixtureWorker(t, service, claim.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	manager.CleanupMu.Lock()
	oldCleanup := manager.Cleanups[before.ID]
	manager.CleanupMu.Unlock()
	if _, err = service.SandboxRequests.Execute(ctx, principal, "pause", operation.Command{
		Action:    "pause",
		SandboxID: before.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "resume", operation.Command{
		Action:    "resume",
		SandboxID: before.ID,
	}); err != nil {
		t.Fatal(err)
	}
	after, err := service.Dataplane.Ready(ctx, principal, before.ID)
	if err != nil || after.Instance == before.Instance || after.VMID == before.VMID {
		t.Fatal(before, after, err)
	}
	runtime.PolicyMu.Lock()
	count := len(runtime.Events)
	runtime.PolicyMu.Unlock()
	if err = oldCleanup(); err != nil {
		t.Fatal(err)
	}
	runtime.PolicyMu.Lock()
	current := len(runtime.Events)
	runtime.PolicyMu.Unlock()
	if current != count {
		t.Fatal("old binding changed resumed instance networking")
	}
	if _, err = service.Dataplane.Ready(ctx, principal, before.ID); err != nil {
		t.Fatal("old binding revoked resumed direct access", err)
	}
}

func TestHavenDirectRetirementCompletionAtomicity(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT,'private direct failure'",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, runtime, _ := fixtures.DirectFixture(t)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			claim := (fixtures.SandboxClaims{
				App: service,
			}).Claim(t, "claim", nil, egress.Policy{
				Mode: "direct",
				AllowedCIDRs: []string{
					"8.8.8.8/32",
				},
			})
			fixtures.WaitFixtureWorker(t, service, claim.ID)
			if _, err := service.State.Database().Exec("CREATE TRIGGER deny_direct_completion BEFORE UPDATE ON operation_steps WHEN OLD.step_key='direct_retirement' AND NEW.status='succeeded' BEGIN SELECT RAISE(" + rejection + "); END"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := service.SandboxRequests.Execute(ctx, principal, "none", operation.Command{
				Action:    "configure",
				SandboxID: claim.Result.SandboxID,
				Policy: egress.Policy{
					Mode: "none",
				},
			}); err == nil {
				t.Fatal("rejected direct completion published success")
			}
			retirement, err := machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
			if err != nil || retirement.Status != "running" || retirement.State.Phase != "pending" {
				t.Fatal("completion lost durable retirement intent", retirement, err)
			}
			source := retirement.Input.Source
			policy, err := machine.LoadPolicyWitness(ctx, service.State.Database(), source.Candidate)
			if err != nil || policy.OperationID != claim.ID || policy.Instance != source.State.Witness || string(policy.Body) != string(testfixture.Encode(retirement.Input.Policy)) {
				t.Fatal("failed completion partially removed policy ownership", policy, err)
			}
			var cleanup string
			if err = service.State.Database().QueryRow("SELECT status FROM cleanup_intents WHERE id=?", "direct_policy:"+claim.ID).Scan(&cleanup); err != nil || cleanup != "pending" {
				t.Fatal("failed completion partially retired cleanup", cleanup, err)
			}
			if err = runtime.VerifyNetworkPolicy(ctx, source.Input.Resource, nil); err != nil {
				t.Fatal("fixture did not cross policy removal boundary", err)
			}
			if _, err = service.Dataplane.Ready(ctx, principal, claim.Result.SandboxID); err == nil {
				t.Fatal("uncommitted retirement authorized data delivery")
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER deny_direct_completion"); err != nil {
				t.Fatal(err)
			}
			if err = service.Maintenance.Reconcile(); err != nil {
				t.Fatal(err)
			}
			fixtures.WaitFixtureWorker(t, service, claim.ID)
			done, err := machine.LoadDirectRetirement(ctx, service.State.Database(), claim.ID)
			if err != nil || done.Status != "succeeded" || done.Hash != retirement.Hash || !reflect.DeepEqual(done.Input, retirement.Input) {
				t.Fatal("retirement recovery changed original proof", done, err)
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
				t.Fatal("retirement altered original claim result", original, claim, err)
			}
		})
	}
}
