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
	Closing() bool
}

// Tenants manages credentials and admission budgets. The admission guard is the
// same instance used by resource transactions and final store shutdown.
type Tenants struct {
	repository *Repository
	admission  sync.Locker
	lifetime   ClosingReader
	reviewed   bool
	checkpoint func() error
}

func NewTenants(repository *Repository, admission sync.Locker, lifetime ClosingReader, reviewed bool, checkpoint func() error) *Tenants {
	return &Tenants{
		repository: repository,
		admission:  admission,
		lifetime:   lifetime,
		reviewed:   reviewed,
		checkpoint: checkpoint,
	}
}

func (s *Tenants) PutTenant(ctx context.Context, principal Principal, tenant types.Tenant) (types.Tenant, error) {
	if !principal.Root || tenant.ID == "" || tenant.Token == "" || tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 {
		return types.Tenant{}, fmt.Errorf("%w: tenant administration requires root, token and finite budgets", errs.ErrInvalid)
	}
	s.admission.Lock()
	defer s.admission.Unlock()
	if s.reviewed {
		return types.Tenant{}, fmt.Errorf("%w: reviewed metadata rollback does not authorize new effects", errs.ErrManualRecovery)
	}
	if s.lifetime.Closing() {
		return types.Tenant{}, fmt.Errorf("%w: Haven is closing", errs.ErrConflict)
	}
	if err := s.repository.PutTenant(ctx, tenant.ID, tenant.Token, Limits{
		MaxSandboxes:  tenant.MaxSandboxes,
		MaxStorageMiB: tenant.MaxStorageMiB,
	}); err != nil {
		return types.Tenant{}, err
	}
	tenant.Token = ""
	// An error here follows a committed write. It is never an instruction to
	// retry a transaction or restore the previous credential in memory.
	return tenant, s.checkpoint()
}
