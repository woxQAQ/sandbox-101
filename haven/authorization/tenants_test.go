package authorization_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
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
			limits, err := authorization.LoadTenantLimits(ctx, service.State.Database(), original.ID)
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
			if err = service.State.Database().QueryRow("SELECT token_hash FROM tenants WHERE id=?", original.ID).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			if hash == next.Token {
				t.Fatal("tenant metadata persisted the bearer credential")
			}
			if err = service.close(ctx); err != nil {
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
	service.Tenants = authorization.NewTenants(authorization.New(service.State.Database()), authorization.TenantDependencies{
		StoreWriteMu: service.StoreWriteMu,
		Lifetime:     service.Lifetime,
		Checkpoint: func() error {
			return failure
		},
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

type tenantFixture struct {
	State          *store.State
	Tenants        *authorization.Tenants
	Authentication *authorization.Authentication
	StoreWriteMu   sync.Locker
	Lifetime       *execution.Lifetime
}

func newTenantFixture(t *testing.T) *tenantFixture {
	t.Helper()
	state := testfixture.OpenState(t)
	lifetime := execution.NewLifetime(context.Background())
	storeWriteMu := &sync.Mutex{}
	fixture := &tenantFixture{
		State: state,
		Tenants: authorization.NewTenants(authorization.New(state.Database()), authorization.TenantDependencies{
			StoreWriteMu: storeWriteMu,
			Lifetime:     lifetime,
			Reviewed:     state.Reviewed(),
			Checkpoint:   state.Checkpoint,
		}),
		Authentication: authorization.NewAuthentication("fixture-root-token", authorization.New(state.Database()), sandbox.NewCapabilityLookup(state.Database()), state.Sign),
		StoreWriteMu:   storeWriteMu,
		Lifetime:       lifetime,
	}
	t.Cleanup(func() {
		if err := fixture.close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return fixture
}

func (fixture *tenantFixture) close(ctx context.Context) error {
	return fixture.Lifetime.Close(ctx, func() {}, fixture.State.Close)
}
