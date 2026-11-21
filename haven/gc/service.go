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

type CollectorGuards interface {
	// ForStoreMutation returns the node-wide lock for durable admission, state publication and final store close.
	ForStoreMutation() execution.Guard
	// ForNodeIdempotencyKey returns the lock shared by admissions using this node's key.
	ForNodeIdempotencyKey(nodeID, key string) execution.Guard
	// ForGCAdmission returns the node-wide lock for admitting a collection and taking inventory.
	ForGCAdmission() execution.Guard
	// ForImageArtifacts returns the lock shared by image publication, cleanup and GC inventory.
	ForImageArtifacts() execution.Guard
	// ForGCOperation returns the lock for advancing one admitted collection operation.
	ForGCOperation(operationID string) execution.Guard
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
	guards     CollectorGuards
	workers    *execution.Workers
	proofs     machine.SnapshotProofs
	operations *operation.Monitor
	images     *image.Images
	supply     *supply.SupplyCleanup
	snapshots  *machine.SnapshotDeletion
	raw        *machine.CaptureCleanup
	faultPoint func(string) error
}

type CollectorDependencies struct {
	Guards     CollectorGuards
	Gate       execution.AdmissionGate
	Workers    *execution.Workers
	Proofs     machine.SnapshotProofs
	Operations *operation.Monitor
	Images     *image.Images
	Supply     *supply.SupplyCleanup
	Snapshots  *machine.SnapshotDeletion
	Raw        *machine.CaptureCleanup
}

func NewCollector(state *store.State, dependencies CollectorDependencies, fault func(string) error) *Collector {
	return &Collector{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:        dependencies.Workers.Context(),
		nodeID:     state.Identity().Identity,
		admission:  dependencies.Guards.ForStoreMutation(),
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

func (c *Collector) fault(point string) error {
	if c.faultPoint != nil {
		return c.faultPoint(point)
	}
	return nil
}

func (c *Collector) dispatch(id string) {
	if c.store.Reviewed {
		return
	}
	c.workers.Start(id, func() error {
		_, err := c.Resume(id)
		return err
	})
}

func (c *Collector) Resume(id string) (bool, error) {
	if c.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume collection", errs.ErrManualRecovery)
	}
	step, err := Load(c.ctx, c.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if operation.Terminal(step.Status) {
		return true, nil
	}
	guard := c.guards.ForGCOperation(id)
	guard.Lock()
	defer guard.Unlock()
	step, err = Load(c.ctx, c.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, c.run(step)
}
