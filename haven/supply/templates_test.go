package supply_test

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

func TestHavenTemplateRootOwnerAndPublicationProvenance(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, _, _ := testapp.SandboxFixture(t)
	tenant := authorization.Principal{
		TenantID: "tenant",
	}
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	original, err := service.TemplateCatalog.GetTemplate(ctx, tenant, "cold")
	if err != nil {
		t.Fatal(err)
	}
	spec := original
	spec.ID = "managed"
	spec.TenantID = "tenant"
	command := operation.Intent{
		Action:   "template_put",
		Template: &spec,
	}
	publication, err := service.CommandAdmission.Execute(ctx, root, "create", command)
	if err != nil {
		t.Fatal(err)
	}
	current, err := service.TemplateCatalog.GetTemplate(ctx, tenant, spec.ID)
	if err != nil || current.TenantID != "tenant" {
		t.Fatal("root publication lost permanent resource owner", current, err)
	}
	if _, err = service.CommandAdmission.Execute(ctx, tenant, "claim", operation.Intent{
		Action:     "claim",
		TemplateID: spec.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Tenants.PutTenant(ctx, root, types.Tenant{
		ID:            "root",
		Token:         "unused-role-fixture-token",
		MaxSandboxes:  10,
		MaxStorageMiB: 4096,
	}); err != nil {
		t.Fatal(err)
	}
	ordinaryRoot := authorization.Principal{
		TenantID: "root",
	}
	if _, err = service.OperationMonitor.Get(ctx, ordinaryRoot, publication.ID); err == nil {
		t.Fatal("tenant named root read administrator operation")
	}
	if _, err = service.Supply.AdmitTemplate(ctx, ordinaryRoot, "create", command); err == nil {
		t.Fatal("tenant named root replayed administrator credentials")
	}
	missing := spec
	missing.TenantID = ""
	if _, err = service.Supply.AdmitTemplate(ctx, root, "missing-owner", operation.Intent{
		Action:   "template_put",
		Template: &missing,
	}); err == nil {
		t.Fatal("root inferred target owner")
	}
	changed := spec
	changed.TenantID = "other"
	if _, err = service.Supply.AdmitTemplate(ctx, root, "changed-owner", operation.Intent{
		Action:   "template_put",
		Template: &changed,
	}); err == nil {
		t.Fatal("root changed permanent owner")
	}
	if _, err = service.CommandAdmission.Execute(ctx, root, "update", command); err != nil {
		t.Fatal(err)
	}
	record, err := supply.NewTemplateCatalog(service.State.Database()).Owned(ctx, "tenant", spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.State.Database().Exec("UPDATE template_versions SET warmup=? WHERE operation_id=?", "unpublished fixture command", record.Version); err != nil {
		t.Fatal(err)
	}
	before := runtime.Count("launch")
	if _, err = service.TemplateCatalog.GetTemplate(ctx, tenant, spec.ID); err == nil {
		t.Fatal("catalog acquired authority without original publication")
	}
	if _, err = service.Sandboxes.AdmitClaim(ctx, tenant, "corrupt", operation.Intent{
		Action:     "claim",
		TemplateID: spec.ID,
	}); err == nil {
		t.Fatal("corrupted template provisioned a VM")
	}
	if runtime.Count("launch") != before {
		t.Fatal("unpublished template performed resource effects")
	}
}
