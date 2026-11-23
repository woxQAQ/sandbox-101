package workflows_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/supply"
)

func TestHavenImageCatalogRejectsOrphansAndChangedMaterialization(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend := testapp.ColdService(t, nil)
	ctx := context.Background()
	template, err := supply.NewTemplateCatalog(service.State.Database()).Fresh(ctx, "tenant", "cold")
	if err != nil {
		t.Fatal(err)
	}
	imageManifest, err := image.LoadManifest(ctx, service.State.Database(), template.Config.Image)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(service.State.Directory(), "images", imageManifest.Digest[7:])
	if _, err = service.State.Database().Exec("DELETE FROM images WHERE digest=?", imageManifest.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Provisioning.AdmitCold(ctx, machine.FreshRequest{
		TenantID:   "tenant",
		Key:        "orphan",
		TemplateID: "cold",
	}); err == nil || backend.Count("prepare") != 0 {
		t.Fatal("filesystem-only image granted provisioning authority", err)
	}
	// Restore only the original immutable fixture input, never a workflow fact.
	testfixture.PublishManifest(t, service.State.Database(), imageManifest)
	operation := testfixture.ColdClaim(t, service, "published")
	if step := testutil.AwaitColdWorker(t, service, operation.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	before := backend.Count("prepare")
	changed := imageManifest
	changed.Layout = "different-layout"
	if err = loom.WriteJSON(filepath.Join(directory, "image.json"), changed); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Provisioning.AdmitCold(ctx, machine.FreshRequest{
		TenantID:   "tenant",
		Key:        "changed",
		TemplateID: "cold",
	}); err == nil || backend.Count("prepare") != before {
		t.Fatal("changed file materialization granted provisioning authority", err)
	}
	if err = loom.WriteJSON(filepath.Join(directory, "image.json"), imageManifest); err != nil {
		t.Fatal(err)
	}
	if err = image.VerifyFiles(changed, directory); err == nil {
		t.Fatal("different captured materialization matched the catalog")
	}
}
