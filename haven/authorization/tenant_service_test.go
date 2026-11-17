package authorization_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/types"
)

func TestHavenTenantCredentialUpdateRejectsSQL(t *testing.T) {
	for _, rejection := range []string{
		"IGNORE",
		"ABORT,'fixture rejected update'",
	} {
		t.Run(rejection, func(t *testing.T) {
			service := newTenantFixture(t)
			ctx := context.Background()
			root := authorization.Principal{
				Root: true,
			}
			original := types.Tenant{
				ID:            "private-tenant",
				Token:         "fixture-old-credential",
				MaxSandboxes:  2,
				MaxStorageMiB: 128,
			}
			if result, err := service.Tenants.PutTenant(ctx, root, original); err != nil || result.Token != "" {
				t.Fatal("initial tenant write failed or returned its credential", err)
			}
			if _, err := service.State.Database().Exec("CREATE TRIGGER reject_tenant_update BEFORE UPDATE ON tenants BEGIN SELECT RAISE(" + rejection + "); END"); err != nil {
				t.Fatal(err)
			}
			next := original
			next.Token, next.MaxSandboxes = "fixture-new-credential", 4
			if _, err := service.Tenants.PutTenant(ctx, root, next); err == nil {
				t.Fatal("rejected or zero-row tenant update was acknowledged")
			}
			if principal, err := service.Authentication.Authenticate(ctx, original.Token); err != nil || principal.TenantID != original.ID {
				t.Fatal("rejected update retired the existing credential", err)
			}
			if _, err := service.Authentication.Authenticate(ctx, next.Token); err == nil {
				t.Fatal("rejected update activated a replacement credential")
			}
			limits, err := authorization.New(service.State.Database()).Limits(ctx, original.ID)
			if err != nil || limits.MaxSandboxes != original.MaxSandboxes || limits.MaxStorageMiB != original.MaxStorageMiB {
				t.Fatal("rejected update changed the budget", limits, err)
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER reject_tenant_update"); err != nil {
				t.Fatal(err)
			}
			if result, err := service.Tenants.PutTenant(ctx, root, next); err != nil || result.Token != "" {
				t.Fatal("replacement failed or returned its credential", err)
			}
			if _, err = service.Authentication.Authenticate(ctx, original.Token); err == nil {
				t.Fatal("replacement retained the obsolete credential")
			}
			if principal, err := service.Authentication.Authenticate(ctx, next.Token); err != nil || principal.TenantID != original.ID {
				t.Fatal("replacement credential did not authorize its tenant", err)
			}
			var hash string
			var body []byte
			if err = service.State.Database().QueryRow("SELECT token_hash,body FROM tenants WHERE id=?", original.ID).Scan(&hash, &body); err != nil {
				t.Fatal(err)
			}
			if hash == next.Token || bytes.Contains(body, []byte(next.Token)) {
				t.Fatal("tenant metadata persisted the bearer credential")
			}
			if err = service.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = service.Tenants.PutTenant(ctx, root, original); !errors.Is(err, errs.ErrConflict) {
				t.Fatal("closed admission reached tenant persistence", err)
			}
		})
	}
}

func TestHavenTenantCheckpointFailureRetainsCommittedCredential(t *testing.T) {
	service := newTenantFixture(t)
	failure := errors.New("fixture checkpoint unavailable")
	service.Tenants = authorization.NewTenants(authorization.New(service.State.Database()), service.Admission, service.Lifetime, false, func() error {
		return failure
	})
	tenant := types.Tenant{
		ID:            "private-tenant",
		Token:         "fixture-committed-credential",
		MaxSandboxes:  2,
		MaxStorageMiB: 128,
	}
	result, err := service.Tenants.PutTenant(context.Background(), authorization.Principal{
		Root: true,
	}, tenant)
	if !errors.Is(err, failure) || result.ID != tenant.ID || result.Token != "" {
		t.Fatal("checkpoint failure changed committed result handling", err)
	}
	principal, err := service.Authentication.Authenticate(context.Background(), tenant.Token)
	if err != nil || principal.TenantID != tenant.ID {
		t.Fatal("checkpoint error was interpreted as credential rollback", err)
	}
}
