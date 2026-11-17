package authorization_test

import (
	"context"
	"sync"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
)

type tenantFixture struct {
	State          *store.State
	Tenants        *authorization.Tenants
	Authentication *authorization.Authentication
	Admission      sync.Locker
	Lifetime       *execution.Lifetime
}

func newTenantFixture(t *testing.T) *tenantFixture {
	t.Helper()
	state := testfixture.OpenState(t)
	lifetime := execution.NewLifetime(context.Background())
	gate := &sync.Mutex{}
	fixture := &tenantFixture{
		State:          state,
		Tenants:        authorization.NewTenants(authorization.New(state.Database()), gate, lifetime, state.Reviewed(), state.Checkpoint),
		Authentication: authorization.NewAuthentication("fixture-root-token", authorization.New(state.Database()), sandbox.NewCapabilityLookup(state.Database()), state.Sign),
		Admission:      gate,
		Lifetime:       lifetime,
	}
	t.Cleanup(func() {
		if err := fixture.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return fixture
}

func (fixture *tenantFixture) Close(ctx context.Context) error {
	return fixture.Lifetime.Close(ctx, func() {}, fixture.State.Close)
}
