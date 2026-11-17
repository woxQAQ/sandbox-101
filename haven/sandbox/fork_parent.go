package sandbox

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"slices"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type forkAdmission struct {
	gate     execution.AdmissionGate
	history  machine.SnapshotProofs
	capacity ProvisionCapacity
	Parent   operation.Record
	Input    ForkInput
	MAC      string
	Free     int64
}

type forkBranchAdmission struct {
	Group  ForkStep
	Target ownership.Sandbox
	Index  int
}

func (parent *forkAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, child operation.Record, kind, candidate, hash string) error {
	if parent.Input.Source == nil || kind != "capture" {
		return fmt.Errorf("%w: fork capture dependency changed", errs.ErrConflict)
	}
	source, err := machine.LoadResourceStep(ctx, tx, parent.Input.Source.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(source, *parent.Input.Source) || child.Result.VMID != source.Candidate || child.Result.SandboxID != source.Input.SandboxID || candidate != child.Result.SnapshotID {
		return fmt.Errorf("%w: fork capture original source changed", errs.ErrConflict)
	}
	parent.Parent.Result.SnapshotID = child.Result.SnapshotID
	if err = parent.admitIntent(ctx, tx); err != nil {
		return err
	}
	return machine.InsertResourceLink(ctx, tx, parent.Parent.ID, "capture", parent.Input.Source.State.Witness, child, kind, candidate, hash)
}

func (parent *forkBranchAdmission) ValidateProvisionTarget(ctx context.Context, tx *sql.Tx, target ownership.Sandbox, source machine.Source, holder string) (*machine.QuotaDestination, error) {
	group, err := LoadFork(ctx, tx, parent.Group.OperationID)
	if err != nil {
		return nil, err
	}
	if group.Hash != parent.Group.Hash || operation.Terminal(group.Status) || group.State.Phase != "captured" || !reflect.DeepEqual(group.State.Snapshot, parent.Group.State.Snapshot) || group.State.Snapshot == nil || !reflect.DeepEqual(*group.State.Snapshot, source) || parent.Index < 0 || parent.Index >= len(group.Input.Targets) || !reflect.DeepEqual(group.Input.Targets[parent.Index], target) || holder != forkSlotHolder(group.OperationID, target.ID) {
		return nil, fmt.Errorf("%w: fork placement no longer owns its prefunded slot", errs.ErrConflict)
	}
	settings, err := LoadSettings(ctx, tx, target.ID)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(settings, group.Input.Settings) {
		return nil, fmt.Errorf("%w: fork placement settings changed", errs.ErrConflict)
	}
	return nil, nil
}

func (parent *forkBranchAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, child operation.Record, kind, candidate, hash string) error {
	if kind != "clone" || child.Result.SandboxID != parent.Target.ID {
		return fmt.Errorf("%w: fork branch resource identity changed", errs.ErrConflict)
	}
	if err := store.RequireOne(dbgen.New(tx).DeleteUnchangedSettings(ctx, dbgen.DeleteUnchangedSettingsParams{
		SandboxID: parent.Target.ID,
		Body:      operation.Encode(parent.Group.Input.Settings),
	})); err != nil {
		return err
	}
	return machine.InsertResourceLink(ctx, tx, parent.Group.OperationID, fmt.Sprintf("branch:%d", parent.Index), "", child, kind, candidate, hash)
}

func forkSlotHolder(group, sandbox string) string {
	return "fork:" + group + ":" + sandbox
}

func forkTargetIDs(input ForkInput) []string {
	ids := make([]string, len(input.Targets))
	for index, target := range input.Targets {
		ids[index] = target.ID
	}
	return ids
}

func (service *Forks) saveFork(step *ForkStep, state ForkState) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "fork_group",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(state),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	return service.store.Checkpoint()
}

func (service *Forks) forkSecrets(group ForkStep) {
	service.access.CopyOperationSecrets(group.OperationID, group.State.Children...)
}

func sameForkTargets(first, second []string) bool {
	return slices.Equal(first, second)
}
