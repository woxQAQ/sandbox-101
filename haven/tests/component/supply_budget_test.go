package component

import (
	"context"
	"fmt"
	"testing"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenSupplySlotBudgets(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, budget := range []quota.SlotBudget{
		{
			Kind:  "sandbox_slot",
			Units: 1,
		},
		{
			Kind:  "build_slot",
			Units: 1,
		},
		{
			Kind:  "sandbox_slot",
			Units: 0,
		},
	} {
		t.Run(fmt.Sprintf("%s-%d", budget.Kind, budget.Units), func(t *testing.T) {
			service, _ := fixtures.ColdService(t, nil)
			ctx := context.Background()
			template, err := supply.NewTemplateCatalog(service.State.Database()).Fresh(ctx, "tenant", "cold")
			if err != nil {
				t.Fatal(err)
			}
			imageManifest, err := image.LoadManifest(ctx, service.State.Database(), template.Config.Image)
			if err != nil {
				t.Fatal(err)
			}
			request := machine.FreshRequest{
				TenantID:   "tenant",
				Key:        "supply",
				TemplateID: "cold",
			}
			frozen := testfixture.Encode(struct {
				Action  string
				Request machine.FreshRequest
			}{"claim", request})
			operation, err := service.Provisioning.Admit(ctx, sandbox.ProvisionAdmission{
				Request: supply.ProvisionRequest{
					TenantID: "tenant",
					Key:      request.Key,
					Frozen:   frozen,
					Template: template,
					Image:    imageManifest,
				},
				Target: &sandbox.ProvisionTarget{
					Budget: &budget,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			source := fixtures.AwaitColdWorker(t, service, operation.ID)
			if source.Status != "succeeded" || source.Input.SlotBudget != budget {
				t.Fatal(source)
			}
			var slots int64
			if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots); err != nil || slots != budget.Units {
				t.Fatal(slots, err)
			}
			release, err := service.Deletion.AdmitRelease(ctx, machine.ReleaseRequest{
				TenantID:  "tenant",
				Key:       "release",
				SandboxID: operation.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			released := fixtures.AwaitColdWorker(t, service, release.ID)
			if released.Status != "succeeded" {
				t.Fatal(released)
			}
			var remaining int
			if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM quota_resources WHERE tenant_id='tenant'").Scan(&remaining); err != nil || remaining != 0 {
				t.Fatal(remaining, err)
			}
		})
	}
}
