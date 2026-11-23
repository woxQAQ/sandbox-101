package execution

import (
	"fmt"
	"sync"

	"github.com/woxQAQ/haven/internal/errs"
)

// AdmissionGate borrows the existing registration guard and closing state.
// Reviewed metadata is immutable for this store lifetime.
type AdmissionGate struct {
	registrationMu sync.Locker
	lifetime       *Lifetime
	reviewed       bool
}

func NewAdmissionGate(registrationMu sync.Locker, lifetime *Lifetime, reviewed bool) AdmissionGate {
	return AdmissionGate{
		registrationMu: registrationMu,
		lifetime:       lifetime,
		reviewed:       reviewed,
	}
}

func (gate AdmissionGate) Check() error {
	gate.registrationMu.Lock()
	defer gate.registrationMu.Unlock()
	if gate.reviewed {
		return fmt.Errorf("%w: reviewed metadata rollback does not authorize new effects", errs.ErrManualRecovery)
	}
	if gate.lifetime.Closing() {
		return fmt.Errorf("%w: Haven is closing", errs.ErrConflict)
	}
	return nil
}
