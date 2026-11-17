package supply

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/store"
)

type SupplyCleanupGuards interface {
	Admission() execution.Guard
	NodeRequest(string, string) execution.Guard
	TemplateWork(string) execution.Guard
	SupplyCleanup(string) execution.Guard
}

// SupplyCleanup owns an independent collector for failed supply work or an
// unclaimed warm publication. Child services retain their original resource
// proofs; the collector retires its own pool and image custody only afterward.
type SupplyCleanup struct {
	store      store.Workflow
	ctx        context.Context
	nodeID     string
	admission  execution.Guard
	gate       execution.AdmissionGate
	guards     SupplyCleanupGuards
	workers    *execution.Workers
	proofs     machine.ResourceProofs
	stops      *machine.VMStops
	captures   *machine.CaptureDiscard
	deletion   *machine.VMDeletion
	ownership  CleanupOwnershipReader
	faultPoint func(string) error
}

func NewSupplyCleanup(state *store.State, guards SupplyCleanupGuards, gate execution.AdmissionGate, workers *execution.Workers, proofs machine.ResourceProofs, stops *machine.VMStops, captures *machine.CaptureDiscard, deletion *machine.VMDeletion, fault func(string) error, ownership CleanupOwnershipReader) *SupplyCleanup {
	return &SupplyCleanup{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:        workers.Context(),
		nodeID:     state.Identity().Identity,
		admission:  guards.Admission(),
		gate:       gate,
		guards:     guards,
		workers:    workers,
		proofs:     proofs,
		stops:      stops,
		captures:   captures,
		deletion:   deletion,
		ownership:  ownership,
		faultPoint: fault,
	}
}

func (service *SupplyCleanup) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *SupplyCleanup) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		_, err := service.Resume(id)
		return err
	})
}

func (service *SupplyCleanup) Resume(id string) (bool, error) {
	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume supply cleanup", errs.ErrManualRecovery)
	}
	step, err := LoadSupplyCleanup(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := service.guards.SupplyCleanup(step.Input.Source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadSupplyCleanup(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runSupplyCleanup(step)
}
