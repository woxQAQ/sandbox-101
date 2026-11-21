package testapp

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func GoldenSupplyFixture(t *testing.T, target int) (*application.App, *testfixture.ColdRuntime, *testfixture.WorkloadFixture) {
	t.Helper()
	return GoldenSupplyFixtureWithFault(t, target, nil)
}

func GoldenSupplyFixtureWithFault(t *testing.T, target int, fault func(string) error) (*application.App, *testfixture.ColdRuntime, *testfixture.WorkloadFixture) {
	t.Helper()
	service, runtime, guest, _ := SandboxFixtureWithBackend(t, fault, testfixture.RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	cold, err := service.TemplateCatalog.GetTemplate(context.Background(), principal, "cold")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := types.Template{
		ID:         "golden",
		Image:      cold.Image,
		Shape:      cold.Shape,
		TargetWarm: target,
	}
	if _, err = service.CommandAdmission.Execute(ctx, principal, "build", operation.Intent{
		Action:   "template_build",
		Template: &spec,
	}); err != nil {
		t.Fatal(err)
	}
	return service, runtime, guest
}
