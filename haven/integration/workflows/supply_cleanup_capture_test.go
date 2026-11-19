package workflows_test

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
)

func TestHavenSupplyCleanupRetiresFailedBuildCapture(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
	var captures atomic.Int64
	runtime.Before = func(name string, _ loom.Resource) error {
		if name == "capture" {
			captures.Add(1)
			return fmt.Errorf("private build capture failure")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	cold, err := service.TemplateCatalog.GetTemplate(ctx, principal, "cold")
	if err != nil {
		t.Fatal(err)
	}
	original, err := service.SandboxRequests.Execute(ctx, principal, "failed-build", operation.Command{
		Action: "template_build",
		Template: &operation.TemplateSpec{
			ID:         "failed-build",
			Image:      cold.Image,
			Shape:      cold.Shape,
			TargetWarm: 1,
		},
	})
	if err == nil || original.Status != "failed" {
		t.Fatal("fixture did not retain failed build", original, err)
	}
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	accepted, err := service.SupplyCleanup.AdmitSupplyCleanup(ctx, root, "cleanup", supply.SupplyCleanupRequest{
		OperationID: original.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Operations.WaitManagement(ctx, root, accepted.ID); err != nil {
		t.Fatal("collector did not complete dependent stop/capture/release", err)
	}
	step, err := supply.LoadSupplyCleanup(ctx, service.State.Database(), accepted.ID)
	if err != nil || step.Status != "succeeded" || step.Phase != "complete" || len(step.Input.CaptureFailures) != 1 {
		t.Fatal("collector lost failed capture provenance", step, err)
	}
	for _, child := range []struct {
		role string
		kind string
	}{
		{"stop", "stop"},
		{"capture:" + step.Input.CaptureFailures[0].OperationID, "capture_discard"},
		{"resource", "release"},
	} {
		link, status, err := machine.LoadResourceLink(ctx, service.State.Database(), accepted.ID, child.role)
		if err != nil || status != "succeeded" || link.Kind != child.kind {
			t.Fatal("collector skipped a historical child receipt", child, link, status, err)
		}
	}
	var machines, storage, cleanup int
	if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM vms),(SELECT COALESCE(SUM(storage_mib),0) FROM quota_resources),(SELECT COUNT(*) FROM cleanup_intents WHERE status IN ('pending','blocked','transferred'))").Scan(&machines, &storage, &cleanup); err != nil || machines != 0 || storage != 0 || cleanup != 0 {
		t.Fatal("collector did not retire completed child responsibility", machines, storage, cleanup, err)
	}
	unchanged, err := service.Operations.GetOperation(ctx, "tenant", original.ID)
	if err != nil || !reflect.DeepEqual(unchanged, original) {
		t.Fatal("cleanup changed the failed build result", unchanged, err)
	}
	if captures.Load() != 1 || runtime.Count("launch") != 1 {
		t.Fatal("collector recaptured or recreated failed build", captures.Load(), runtime.Count("launch"))
	}
}
