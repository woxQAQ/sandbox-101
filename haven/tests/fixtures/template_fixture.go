package fixtures

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/operation"
)

func GoldenSupplyFixture(t *testing.T, target int) (*application.App, *testfixture.ColdRuntime, *testfixture.WorkloadFixture) {
	t.Helper()
	return GoldenSupplyFixtureWithFault(t, target, nil)
}

func GoldenSupplyFixtureWithFault(t *testing.T, target int, fault func(string) error) (*application.App, *testfixture.ColdRuntime, *testfixture.WorkloadFixture) {
	t.Helper()
	service, runtime, guest, _ := testapp.SandboxFixtureWithBackend(t, fault, RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	cold, err := service.TemplateCatalog.GetTemplate(context.Background(), principal, "cold")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	spec := operation.TemplateSpec{
		ID:         "golden",
		Image:      cold.Image,
		Shape:      cold.Shape,
		TargetWarm: target,
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "build", operation.Command{
		Action:   "template_build",
		Template: &spec,
	}); err != nil {
		t.Fatal(err)
	}
	return service, runtime, guest
}
