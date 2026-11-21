package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type StopBackend interface {
	InstanceInspector

	// Stop stops the process identified by the resource receipt.
	Stop(ctx context.Context, resource loom.Resource) error
	// SetNetworkEnabled sets the host network link state for the identified instance.
	SetNetworkEnabled(ctx context.Context, resource loom.Resource, enabled bool) error
}

// Stops retains the original generation and funding proof while confirming
// process exit and network removal before publishing the stopped state.
type Stops struct {
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	guards     VMGuards
	lifetime   *execution.Lifetime
	workers    *execution.Workers
	proofs     ResourceProofs
	backend    StopBackend
	ownership  StopOwnershipReader
	faultPoint func(string) error
}

type VMStopsDependencies struct {
	Guards    VMGuards
	Lifetime  *execution.Lifetime
	Workers   *execution.Workers
	Proofs    ResourceProofs
	Backend   StopBackend
	Ownership StopOwnershipReader
}

func NewVMStops(state *store.State, dependencies VMStopsDependencies, fault func(string) error) *Stops {
	return &Stops{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:        dependencies.Workers.Context(),
		admission:  dependencies.Guards.ForStoreMutation(),
		guards:     dependencies.Guards,
		lifetime:   dependencies.Lifetime,
		workers:    dependencies.Workers,
		proofs:     dependencies.Proofs,
		backend:    dependencies.Backend,
		ownership:  dependencies.Ownership,
		faultPoint: fault,
	}
}

func (s *Stops) fault(point string) error {
	if s.faultPoint != nil {
		return s.faultPoint(point)
	}
	return nil
}

func (s *Stops) dispatch(id string) {
	if s.store.Reviewed {
		return
	}
	s.workers.Start(id, func() error {
		handled, err := s.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("stop lacks matching immutable step")
		}
		return nil
	})
}

func (s *Stops) Resume(id string) (bool, error) {
	if s.
		ownership ==
		nil {
		return false,

			fmt.Errorf("%w: required Stops capability port unavailable",
				errs.ErrManualRecovery,
			)
	}

	if s.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume stop", errs.ErrManualRecovery)
	}
	step, err := LoadStop(s.ctx, s.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	source, err := LoadResourceStep(s.ctx, s.store.Database, step.Input.SourceOperation)
	if err != nil {
		return true, err
	}
	guard := s.guards.ForVMLifecycle(source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadStop(s.ctx, s.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, s.runStop(step)
}

type StopRequest struct {
	TenantID         string `json:"tenant_id"`
	Key              string `json:"key"`
	SandboxID        string `json:"sandbox_id"`
	ExpectedInstance string `json:"expected_instance"`
}

type StopInput struct {
	Lifecycle       string `json:"lifecycle,omitempty"`
	Request         StopRequest
	SourceOperation string
	SourceHash      string
}

type StopStep struct {
	OperationID string
	Hash        string
	Status      string
	Phase       string
	Input       StopInput
}

func LoadStop(ctx context.Context, database store.DBTX, id string) (StopStep, error) {
	step := StopStep{
		OperationID: id,
	}
	record, err := dbgen.New(database).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "stop",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input, body := record.Input, record.Body
	candidate, expected, action := record.CandidateResourceID, record.ExpectedInstance, record.Action
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
		return step, fmt.Errorf("stop input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "stop" || candidate != step.Input.Request.SandboxID || expected != step.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("stop frozen identity changed")
	}
	return step, nil
}
