package haven

import (
	"context"
	"fmt"
	"testing"
)

func TestHavenSupplySlotBudgets(t *testing.T) {
	for _, budget := range []slotBudget{
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
			service, _ := coldService(t, nil)
			ctx := context.Background()
			template, err := loadColdTemplate(ctx, service.store.db, "tenant", "cold")
			if err != nil {
				t.Fatal(err)
			}
			image, err := loadImage(ctx, service.store.db, template.Config.Image)
			if err != nil {
				t.Fatal(err)
			}
			request := ColdRequest{
				TenantID:   "tenant",
				Key:        "supply",
				TemplateID: "cold",
			}
			frozen := encode(struct {
				Action  string
				Request ColdRequest
			}{"claim", request})
			operation, err := service.admitProvision(ctx, "tenant", request.Key, frozen, digest(frozen), template, image, nil, nil, &provisionTarget{
				Budget: &budget,
			})
			if err != nil {
				t.Fatal(err)
			}
			source := awaitColdWorker(t, service, operation.ID)
			if source.Status != "succeeded" || source.Input.SlotBudget != budget {
				t.Fatal(source)
			}
			var slots int64
			if err = service.store.db.QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources WHERE tenant_id='tenant'").Scan(&slots); err != nil || slots != budget.Units {
				t.Fatal(slots, err)
			}
			release, err := service.AdmitRelease(ctx, ReleaseRequest{
				TenantID:  "tenant",
				Key:       "release",
				SandboxID: operation.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			released := awaitColdWorker(t, service, release.ID)
			if released.Status != "succeeded" {
				t.Fatal(released)
			}
			var remaining int
			if err = service.store.db.QueryRow("SELECT COUNT(*) FROM quota_resources WHERE tenant_id='tenant'").Scan(&remaining); err != nil || remaining != 0 {
				t.Fatal(remaining, err)
			}
		})
	}
}
