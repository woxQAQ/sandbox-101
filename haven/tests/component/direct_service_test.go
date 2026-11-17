package component

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenDirectRetirementCompletionAtomicity(t *testing.T) {
	parallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT,'private direct failure'",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
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
			waitFixtureWorker(t, service, claim.ID)
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
