package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type DeletionBackend interface {
	StopBackend

	// Remove removes the stopped instance's runtime resources.
	Remove(ctx context.Context, resource loom.Resource) error
}

// Deletion reclaims retained successful or failed generations using their
// original funding and deletion receipts before the final transaction retires
// physical responsibility. Release and discard retain distinct frozen proofs.
type Deletion struct {
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	guards     VMGuards
	lifetime   *execution.Lifetime
	workers    *execution.Workers
	proofs     ResourceProofs
	backend    DeletionBackend
	faultPoint func(string) error
	committer  *operation.Committer
	ownership  DeletionOwnershipReader
	handoff    DeletionParticipant
	usage      VMUsageReader
}

type VMDeletionDependencies struct {
	Guards    VMGuards
	Lifetime  *execution.Lifetime
	Workers   *execution.Workers
	Proofs    ResourceProofs
	Backend   DeletionBackend
	Committer *operation.Committer
	Ownership DeletionOwnershipReader
	Handoff   DeletionParticipant
	Usage     VMUsageReader
}

func NewVMDeletion(state *store.State, dependencies VMDeletionDependencies, fault func(string) error) *Deletion {
	return &Deletion{
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
		handoff:    dependencies.Handoff,
		usage:      dependencies.Usage,
		faultPoint: fault,
		committer:  dependencies.Committer,
	}
}

func (d *Deletion) fault(point string) error {
	if d.faultPoint != nil {
		return d.faultPoint(point)
	}
	return nil
}

func (d *Deletion) validateStep(step ResourceStep) error {
	return ValidateResourceStep(d.store.Directory, step)
}

func (d *Deletion) inspectStoppedGenerations(generations []ResourceStep) error {
	return InspectStoppedGenerations(d.ctx, d.backend, generations)
}

func (d *Deletion) dispatch(id string) {
	if d.store.Reviewed {
		return
	}
	d.workers.Start(id, func() error {
		if handled, err := d.ResumeDiscard(id); handled || err != nil {
			return err
		}
		if handled, err := d.ResumeRelease(id); handled || err != nil {
			return err
		}
		return fmt.Errorf("VM deletion lacks matching immutable step")
	})
}

// ResumeDiscard and ResumeRelease preserve their separate positions in the
// application's recovery order while sharing the original per-VM lock domain.
func (d *Deletion) ResumeDiscard(id string) (bool, error) {
	if d.
		ownership ==
		nil || d.
		handoff ==
		nil || d.usage == nil {
		return false, fmt.Errorf("%w: required Deletion capability port unavailable",

			errs.ErrManualRecovery)
	}

	if d.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume discard", errs.ErrManualRecovery)
	}
	step, err := LoadDiscard(d.ctx, d.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := d.guards.ForVMLifecycle(step.Input.Source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadDiscard(d.ctx, d.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, d.runDiscard(step)
}

func (d *Deletion) ResumeRelease(id string) (bool, error) {
	if d.
		ownership ==
		nil || d.
		handoff ==
		nil || d.usage == nil {
		return false, fmt.Errorf("%w: required Deletion capability port unavailable",

			errs.ErrManualRecovery)
	}

	if d.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume release", errs.ErrManualRecovery)
	}
	step, err := LoadResourceStep(d.ctx, d.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if step.Action != "release" {
		return false, nil
	}
	if err = d.validateStep(step); err != nil {
		return true, err
	}
	guard := d.guards.ForVMLifecycle(step.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadResourceStep(d.ctx, d.store.Database, id)
	if err != nil {
		return true, err
	}
	if operation.Terminal(step.Status) {
		return true, nil
	}
	if err = d.validateStep(step); err != nil {
		return true, err
	}
	currentMachine, instance, owner, err := LoadVM(d.ctx, d.store.Database, step)
	if err != nil {
		return true, err
	}
	if owner != step.OperationID || !currentMachine.Deleting || instance != "" && !ExpectedCurrentInstance(step, instance) {
		return true, fmt.Errorf("resource step lost current VM ownership")
	}
	return true, d.runRelease(step)
}

func GenerationFixedNodes(source ResourceStep) map[string]loom.FileIdentity {
	fixed := map[string]loom.FileIdentity{}
	r := source.Input.Resource
	if source.State.DiskIdentity != nil {
		relative, _ := filepath.Rel(r.Dir, r.Disk)
		fixed[relative] = *source.State.DiskIdentity
	}
	if proof := source.State.Materials; proof != nil {
		version, _ := filepath.Rel(r.Dir, filepath.Dir(r.RestoreDir))
		fixed[version] = proof.Version
		fixed[version+"/restore"] = proof.Restore
		for _, name := range loom.RestoreFileNames() {
			file := proof.Files[name]
			if file.Candidate == "" && name != "memory-ranges" {
				continue
			}
			relative := version + "/restore/" + name
			if name == "disk.raw" {
				relative = version + "/disk.raw"
			}
			fixed[relative] = file.Identity
		}
	}
	return fixed
}
