package authorization

import (
	"context"
	"fmt"
	"sync"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/types"
)

// ClosingReader exposes the admission barrier without granting shutdown control.
type ClosingReader interface {
	// Closing reports whether shutdown has closed admission to new work.
	Closing() bool
}

// Tenants manages credentials and admission budgets. The admission guard is the
// same instance used by resource transactions and final store shutdown.
type Tenants struct {
	repository   *Repository
	storeWriteMu sync.Locker
	lifetime     ClosingReader
	reviewed     bool
	checkpoint   func() error
}

type TenantDependencies struct {
	StoreWriteMu sync.Locker
	Lifetime     ClosingReader
	Reviewed     bool
	Checkpoint   func() error
}

func NewTenants(repository *Repository, dependencies TenantDependencies) *Tenants {
	return &Tenants{
		repository:   repository,
		storeWriteMu: dependencies.StoreWriteMu,
		lifetime:     dependencies.Lifetime,
		reviewed:     dependencies.Reviewed,
		checkpoint:   dependencies.Checkpoint,
	}
}

func (t *Tenants) PutTenant(ctx context.Context, principal Principal, tenant types.Tenant) (types.Tenant, error) {
	if !principal.Root || tenant.ID == "" || tenant.Token == "" || tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 {
		return types.Tenant{}, fmt.Errorf("%w: tenant administration requires root, token and finite budgets", errs.ErrInvalid)
	}
	t.storeWriteMu.Lock()
	defer t.storeWriteMu.Unlock()
	if t.reviewed {
		return types.Tenant{}, fmt.Errorf("%w: reviewed metadata rollback does not authorize new effects", errs.ErrManualRecovery)
	}
	if t.lifetime.Closing() {
		return types.Tenant{}, fmt.Errorf("%w: Haven is closing", errs.ErrConflict)
	}
	if err := t.repository.PutTenant(ctx, tenant.ID, tenant.Token, Limits{
		MaxSandboxes:  tenant.MaxSandboxes,
		MaxStorageMiB: tenant.MaxStorageMiB,
	}); err != nil {
		return types.Tenant{}, err
	}
	tenant.Token = ""
	// An error here follows a committed write. It is never an instruction to
	// retry a transaction or restore the previous credential in memory.
	return tenant, t.checkpoint()
}
