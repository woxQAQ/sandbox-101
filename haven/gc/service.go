package gc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	image "github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

type Guards interface {
	Admission() execution.Guard
	NodeRequest(string, string) execution.Guard
	GarbageCollection() execution.Guard
	Artifacts() execution.Guard
	GCOperation(string) execution.Guard
}

// Collector inventories retained roots and coordinates independently
// proved child collectors within the original frozen batch and deadline.
// Unknown roots or child outcomes retain their existing responsibility.
type Collector struct {
	store      store.Workflow
	ctx        context.Context
	nodeID     string
	admission  execution.Guard
	gate       execution.AdmissionGate
	guards     Guards
	workers    *execution.Workers
	proofs     machine.SnapshotProofs
	operations *operation.Operations
	images     *image.Images
	supply     *supply.SupplyCleanup
	snapshots  *machine.SnapshotDeletion
	raw        *machine.CaptureCleanup
	faultPoint func(string) error
}

type Dependencies struct {
	Guards     Guards
	Gate       execution.AdmissionGate
	Workers    *execution.Workers
	Proofs     machine.SnapshotProofs
	Operations *operation.Operations
	Images     *image.Images
	Supply     *supply.SupplyCleanup
	Snapshots  *machine.SnapshotDeletion
	Raw        *machine.CaptureCleanup
}

func NewCollector(state *store.State, dependencies Dependencies, fault func(string) error) *Collector {
	return &Collector{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:        dependencies.Workers.Context(),
		nodeID:     state.Identity().Identity,
		admission:  dependencies.Guards.Admission(),
		gate:       dependencies.Gate,
		guards:     dependencies.Guards,
		workers:    dependencies.Workers,
		proofs:     dependencies.Proofs,
		operations: dependencies.Operations,
		images:     dependencies.Images,
		supply:     dependencies.Supply,
		snapshots:  dependencies.Snapshots,
		raw:        dependencies.Raw,
		faultPoint: fault,
	}
}

func (service *Collector) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *Collector) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		_, err := service.Resume(id)
		return err
	})
}

func (service *Collector) Resume(id string) (bool, error) {
	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume collection", errs.ErrManualRecovery)
	}
	step, err := Load(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if operation.Terminal(step.Status) {
		return true, nil
	}
	guard := service.guards.GCOperation(id)
	guard.Lock()
	defer guard.Unlock()
	step, err = Load(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.run(step)
}
