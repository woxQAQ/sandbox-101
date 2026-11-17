package execution

import (
	"fmt"
	"sync"

	"github.com/woxQAQ/haven/internal/errs"
)

// AdmissionGate borrows the existing registration guard and closing state.
// Reviewed metadata is immutable for this store lifetime.
type AdmissionGate struct {
	registration sync.Locker
	closing      interface{ Closing() bool }
	reviewed     bool
}

func NewAdmissionGate(registration sync.Locker, closing interface{ Closing() bool }, reviewed bool) AdmissionGate {
	return AdmissionGate{
		registration: registration,
		closing:      closing,
		reviewed:     reviewed,
	}
}

func (gate AdmissionGate) Check() error {
	gate.registration.Lock()
	defer gate.registration.Unlock()
	if gate.reviewed {
		return fmt.Errorf("%w: reviewed metadata rollback does not authorize new effects", errs.ErrManualRecovery)
	}
	if gate.closing.Closing() {
		return fmt.Errorf("%w: Haven is closing", errs.ErrConflict)
	}
	return nil
}
