package haven

import (
	"context"
	"testing"
	"time"
)

func TestHavenTemplateRootOwnerAndPublicationProvenance(t *testing.T) {
	service, runtime, _, _ := sandboxFixture(t)
	tenant := Principal{TenantID: "tenant"}
	root := Principal{Root: true, TenantID: "root"}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	original, err := service.GetTemplate(ctx, tenant, "cold")
	if err != nil {
		t.Fatal(err)
	}
	spec := original
	spec.ID = "managed"
	spec.TenantID = "tenant"
	command := Command{Action: "template_put", Template: &spec}
	publication, err := service.Execute(ctx, root, "create", command)
	if err != nil {
		t.Fatal(err)
	}
	current, err := service.GetTemplate(ctx, tenant, spec.ID)
	if err != nil || current.TenantID != "tenant" {
		t.Fatal("root publication lost permanent resource owner", current, err)
	}
	if _, err = service.Execute(ctx, tenant, "claim", Command{Action: "claim", TemplateID: spec.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.PutTenant(ctx, root, Tenant{ID: "root", Token: "unused-role-fixture-token", MaxSandboxes: 10, MaxStorageMiB: 4096}); err != nil {
		t.Fatal(err)
	}
	ordinaryRoot := Principal{TenantID: "root"}
	if _, err = service.Operation(ctx, ordinaryRoot, publication.ID); err == nil {
		t.Fatal("tenant named root read administrator operation")
	}
	if _, err = service.AdmitTemplate(ctx, ordinaryRoot, "create", command); err == nil {
		t.Fatal("tenant named root replayed administrator credentials")
	}
	missing := spec
	missing.TenantID = ""
	if _, err = service.AdmitTemplate(ctx, root, "missing-owner", Command{Action: "template_put", Template: &missing}); err == nil {
		t.Fatal("root inferred target owner")
	}
	changed := spec
	changed.TenantID = "other"
	if _, err = service.AdmitTemplate(ctx, root, "changed-owner", Command{Action: "template_put", Template: &changed}); err == nil {
		t.Fatal("root changed permanent owner")
	}
	if _, err = service.Execute(ctx, root, "update", command); err != nil {
		t.Fatal(err)
	}
	record, err := loadOwnedTemplate(ctx, service.store.db, "tenant", spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := record
	corrupt.Warmup = "unpublished fixture command"
	if _, err = service.store.db.Exec("UPDATE templates SET body=? WHERE id=?", encode(corrupt), spec.ID); err != nil {
		t.Fatal(err)
	}
	before := runtime.count("launch")
	if _, err = service.GetTemplate(ctx, tenant, spec.ID); err == nil {
		t.Fatal("catalog acquired authority without original publication")
	}
	if _, err = service.AdmitClaim(ctx, tenant, "corrupt", Command{Action: "claim", TemplateID: spec.ID}); err == nil {
		t.Fatal("corrupted template provisioned a VM")
	}
	if runtime.count("launch") != before {
		t.Fatal("unpublished template performed resource effects")
	}
}
