package sandbox

import (
	"database/sql"
	"errors"

	"github.com/woxQAQ/haven/machine"
)

// Resume selects standalone activation while resource-owned activation remains
// with its resource runner. The original source determines the shared VM guard.
func (service *Activations) Resume(operationID string) (bool, error) {
	activation, err := LoadActivation(service.ctx, service.store.Database, operationID)
	if err == nil && activation.Input.ResourceOperation != operationID {
		source, lookup := machine.LoadResourceStep(service.ctx, service.store.Database, activation.Input.ResourceOperation)
		if lookup != nil {
			return false, lookup
		}
		guard := service.guards.VM(source.Candidate)
		guard.Lock()
		defer guard.Unlock()
		return true, service.Continue(operationID)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	return false, nil
}
