package haven

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/loom"
)

func TestHavenImageCatalogRejectsOrphansAndChangedMaterialization(t *testing.T) {
	parallelLifecycle(t)
	service, backend := coldService(t, nil)
	ctx := context.Background()
	template, err := loadColdTemplate(ctx, service.store.db, "tenant", "cold")
	if err != nil {
		t.Fatal(err)
	}
	image, err := loadImage(ctx, service.store.db, template.Config.Image)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(service.store.directory, "images", image.Digest[7:])
	if _, err = service.store.db.Exec("DELETE FROM images WHERE digest=?", image.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err = service.AdmitCold(ctx, ColdRequest{TenantID: "tenant", Key: "orphan", TemplateID: "cold"}); err == nil || backend.count("prepare") != 0 {
		t.Fatal("filesystem-only image granted provisioning authority", err)
	}
	// Restore only the original immutable fixture input, never a workflow fact.
	if _, err = service.store.db.Exec("INSERT INTO images VALUES(?,?)", image.Digest, encode(image)); err != nil {
		t.Fatal(err)
	}
	operation := coldClaim(t, service, "published")
	if step := awaitColdWorker(t, service, operation.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	before := backend.count("prepare")
	changed := image
	changed.Layout = "different-layout"
	if err = loom.WriteJSON(filepath.Join(directory, "image.json"), changed); err != nil {
		t.Fatal(err)
	}
	if _, err = service.AdmitCold(ctx, ColdRequest{TenantID: "tenant", Key: "changed", TemplateID: "cold"}); err == nil || backend.count("prepare") != before {
		t.Fatal("changed file materialization granted provisioning authority", err)
	}
	if err = loom.WriteJSON(filepath.Join(directory, "image.json"), image); err != nil {
		t.Fatal(err)
	}
	if err = verifyImageFiles(changed, directory); err == nil {
		t.Fatal("different captured materialization matched the catalog")
	}
}
