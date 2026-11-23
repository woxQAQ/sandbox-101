package authorization_test

import (
	"errors"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
)

func TestSandboxTokenUsesPersistedIdentityColumns(t *testing.T) {
	service := newTenantFixture(t)
	db := service.State.Database()
	const tenant = "tenant"
	const id = "sandbox"
	if _, err := db.Exec(`INSERT INTO sandboxes(id,tenant,state,template_id,vcpus,memory_mib,disk_mib,creation_epoch,created_at,provision_source,last_error,vm_id,instance,snapshot_id) VALUES(?,?,'running','cold',1,128,64,'epoch','invalid birth timestamp','cold','','vm','instance','')`, id, tenant); err != nil {
		t.Fatal(err)
	}
	auth := service.Authentication
	principal, err := auth.Authenticate(t.Context(), auth.SandboxToken(tenant, id))
	if err != nil || principal != (authorization.Principal{
		TenantID:  tenant,
		SandboxID: id,
	}) {
		t.Fatalf("sandbox identity lookup returned %+v, %v", principal, err)
	}
	if _, err = db.Exec("UPDATE sandboxes SET id='' WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Authenticate(t.Context(), auth.SandboxToken(tenant, "")); !errors.Is(err, errs.ErrManualRecovery) {
		t.Fatalf("incomplete sandbox identity returned %v", err)
	}
}
