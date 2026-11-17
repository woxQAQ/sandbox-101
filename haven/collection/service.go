package collection

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

type GarbageCollectionGuards interface {
	Admission() execution.Guard
	NodeRequest(string, string) execution.Guard
	GarbageCollection() execution.Guard
	Artifacts() execution.Guard
	GCOperation(string) execution.Guard
}

// GarbageCollection inventories retained roots and coordinates independently
// proved child collectors within the original frozen batch and deadline.
// Unknown roots or child outcomes retain their existing responsibility.
type GarbageCollection struct {
	store      store.Workflow
	ctx        context.Context
	nodeID     string
	admission  execution.Guard
	gate       execution.AdmissionGate
	guards     GarbageCollectionGuards
	workers    *execution.Workers
	proofs     machine.SnapshotProofs
	operations *operation.Operations
	images     *image.Images
	supply     *supply.SupplyCleanup
	snapshots  *machine.SnapshotDeletion
	raw        *machine.CaptureCleanup
	faultPoint func(string) error
}

func NewGarbageCollection(state *store.State, guards GarbageCollectionGuards, gate execution.AdmissionGate, workers *execution.Workers, proofs machine.SnapshotProofs, operations *operation.Operations, images *image.Images, supply *supply.SupplyCleanup, snapshots *machine.SnapshotDeletion, raw *machine.CaptureCleanup, fault func(string) error) *GarbageCollection {
	return &GarbageCollection{
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
		operations: operations,
		images:     images,
		supply:     supply,
		snapshots:  snapshots,
		raw:        raw,
		faultPoint: fault,
	}
}

func (service *GarbageCollection) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *GarbageCollection) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		_, err := service.Resume(id)
		return err
	})
}

func (service *GarbageCollection) Resume(id string) (bool, error) {
	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume collection", errs.ErrManualRecovery)
	}
	step, err := LoadGC(service.ctx, service.store.Database, id)
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
	step, err = LoadGC(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runGC(step)
}
