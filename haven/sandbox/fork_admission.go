package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

func (service *Forks) AdmitFork(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if command.Count == 0 {
		command.Count = 1
	}
	if command.Action != "fork" || (command.CheckpointID == "" && command.SandboxID == "") || command.Count < 1 || command.Count > 32 || principal.SandboxID != "" || principal.TenantID == "" || key == "" || len(key) > 256 || service.opts.Guest == nil {
		return empty, fmt.Errorf("%w: fork requires tenant authority, one source, count 1..32 and key", errs.ErrInvalid)
	}
	requestGuard := service.guards.TenantRequest(principal.TenantID, key)
	requestGuard.Lock()
	defer requestGuard.Unlock()
	_, secrets, _, err := SplitEnv(command.Env)
	if err != nil {
		return empty, err
	}
	mac := service.commands.Identity.CommandMAC(principal, command)
	if acceptedOperation, found, err := service.existing(ctx, principal, key, &ClaimAdmission{
		MAC:     mac,
		Secrets: secrets,
	}); found || err != nil {
		if found && err == nil && !operation.Terminal(acceptedOperation.Status) {
			group, lookup := LoadFork(ctx, service.store.Database, acceptedOperation.ID)
			if lookup != nil {
				return acceptedOperation, lookup
			}
			service.forkSecrets(group)
		}
		return acceptedOperation, err
	}
	draining := service.availability.IsDraining()
	if draining {
		return empty, fmt.Errorf("%w: node is draining", errs.ErrExhausted)
	}
	input := ForkInput{
		RequestTenantID: principal.TenantID,
	}
	if command.CheckpointID != "" {
		checkpoint, err := service.catalog.GetCheckpoint(ctx, principal, command.CheckpointID)
		if err != nil {
			return empty, err
		}
		source, err := service.history.LoadRestoreSource(ctx, service.store.Database, checkpoint.TenantID, checkpoint.SnapshotID)
		if err != nil {
			return empty, err
		}
		input.OwnerTenantID = checkpoint.TenantID
		input.Snapshot = &source
		input.Checkpoint = &checkpoint
		input.Template = supply.TemplateRecord{
			ID:       checkpoint.TemplateID,
			TenantID: checkpoint.TenantID,
			Config:   source.Snapshot.Manifest.Config,
		}
	} else {

		sandbox, err := service.catalog.OwnedRecord(ctx, principal, command.SandboxID)
		if err != nil {
			return empty, err
		}
		if sandbox.State != "running" && sandbox.State != "paused" {
			return empty, fmt.Errorf("%w: sandbox is not forkable", errs.ErrConflict)
		}
		settings, err := LoadSettings(ctx, service.store.Database, sandbox.ID)
		if err != nil {
			return empty, err
		}
		if err = service.catalog.Unfenced(ctx, sandbox.ID); err != nil {
			return empty, err
		}
		input.SourceSandbox = &sandbox
		input.OwnerTenantID = sandbox.TenantID
		if command.Policy.Mode == "" {
			command.Policy = settings.Policy
		}
		if sandbox.State == "paused" {
			source, err := service.history.LoadRestoreSource(ctx, service.store.Database, sandbox.TenantID, sandbox.SnapshotID)
			if err != nil {
				return empty, err
			}
			input.Snapshot = &source
			input.Template = supply.TemplateRecord{
				ID:       sandbox.Identity.TemplateID,
				TenantID: sandbox.TenantID,
				Config:   source.Snapshot.Manifest.Config,
			}
		} else {
			public, err := service.catalog.Get(ctx, principal, sandbox.ID)
			if err != nil {
				return empty, err
			}
			source, err := service.catalog.ResourceForData(ctx, public)
			if err != nil {
				return empty, err
			}
			input.Source = &source
			input.Template = supply.TemplateRecord{
				ID:       sandbox.Identity.TemplateID,
				TenantID: sandbox.TenantID,
				Config:   source.Input.Resource.Config,
			}
		}
	}
	admission, err := service.commands.Prepare(command)
	if err != nil {
		return empty, err
	}
	if input.Snapshot != nil {
		if err = machine.VerifyRestoreSourceFiles(*input.Snapshot); err != nil {
			return empty, err
		}
	}
	if err = service.opts.Backend.Check(ctx, input.Template.Config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, service.store.Database, input.Template.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	parent := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
	}
	admission.Settings.Generation = parent.ID
	input.Command = admission.Command
	input.Settings = admission.Settings
	for range command.Count {
		target := ownership.Sandbox{
			ID:          operation.NewID("sandbox_"),
			TenantID:    input.OwnerTenantID,
			OperationID: parent.ID,
			State:       "provisioning",
			Identity:    NewSandboxIdentity(input.Template, ""),
		}
		input.Targets = append(input.Targets, target)
	}
	parent.Result.SandboxIDs = forkTargetIDs(input)
	parent.Result.SandboxID = parent.Result.SandboxIDs[0]
	if input.Snapshot != nil {
		parent.Result.SnapshotID = input.Snapshot.Snapshot.ID
	}
	free, err := loom.FreeBytes(service.store.Directory)
	if err != nil {
		return empty, err
	}
	link := &forkAdmission{
		gate:     service.gate,
		history:  service.history,
		capacity: service.opts.Capacity,
		Parent:   parent,
		Input:    input,
		MAC:      mac,
		Free:     free,
	}
	if input.Source != nil {
		captures := service.captures
		child, err := captures.AdmitForParent(ctx, machine.CaptureRequest{
			TenantID:         input.OwnerTenantID,
			Key:              parent.ID + ":capture",
			SandboxID:        input.SourceSandbox.ID,
			ExpectedInstance: input.Source.State.Witness,
		}, link)
		if child.ID == "" {
			return empty, err
		}
		parent.Result.SnapshotID = child.Result.SnapshotID
		if err != nil {
			return parent, err
		}
	} else {
		if err = service.admitForkSnapshot(ctx, link); err != nil {
			return empty, err
		}
	}
	service.access.SetOperationSecrets(parent.ID, admission.Secrets)
	if err = service.fault("fork_admitted"); err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Forks) admitForkSnapshot(ctx context.Context, parent *forkAdmission) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.gate.Check(); err != nil {
		return err
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = parent.admitIntent(ctx, tx); err != nil {
		return err
	}
	if err = service.fault("fork_admission_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.Checkpoint()
}

func (parent *forkAdmission) admitIntent(ctx context.Context, tx *sql.Tx) error {
	input := parent.Input
	if err := parent.gate.Check(); err != nil {
		return err
	}
	if input.SourceSandbox != nil {
		current, err := LoadSandbox(ctx, tx, input.SourceSandbox.ID, input.OwnerTenantID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *input.SourceSandbox) {
			return fmt.Errorf("%w: fork source sandbox changed before admission", errs.ErrConflict)
		}
		_, err = LifecycleFence(ctx, tx, current.ID)
		if err == nil {
			return fmt.Errorf("%w: fork source is configuration fenced", errs.ErrConflict)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if input.Checkpoint != nil {
		current, err := LoadOwnedCheckpoint(ctx, tx, input.Checkpoint.ID, input.OwnerTenantID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *input.Checkpoint) {
			return fmt.Errorf("%w: fork source checkpoint changed before admission", errs.ErrConflict)
		}
	}
	if input.Snapshot != nil {
		current, err := parent.history.LoadRestoreSource(ctx, tx, input.OwnerTenantID, input.Snapshot.Snapshot.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *input.Snapshot) {
			return fmt.Errorf("%w: fork source snapshot changed before admission", errs.ErrConflict)
		}
	}
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Command
	}{"fork", input.Command})
	payload := operation.Encode(input)
	state := ForkState{
		Phase: "admitted",
	}
	if input.Snapshot != nil {
		state.Phase = "captured"
		state.Snapshot = input.Snapshot
	}
	operations := operation.New(tx)
	if err := operations.Insert(ctx, operation.Admission{
		ID:        parent.Parent.ID,
		ScopeKind: "tenant",
		ScopeID:   input.RequestTenantID,
		Key:       parent.Parent.Key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(parent.Parent.Result),
	}); err != nil {
		return err
	}
	if err := operations.InsertCredential(ctx, parent.Parent.ID, parent.MAC); err != nil {
		return err
	}
	if err := store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         parent.Parent.ID,
		StepKey:             "fork_group",
		Action:              "fork",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: parent.Parent.ID,
		ExpectedInstance:    "",
		Status:              "pending",
		Body:                operation.Encode(state),
	})); err != nil {
		return err
	}
	if err := quota.New(tx).AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         input.Template.Config.Image,
		HolderKind: "step",
		HolderID:   parent.Parent.ID + ":fork_group",
	}); err != nil {
		return err
	}
	if err := parent.reserveTargets(ctx, tx); err != nil {
		return err
	}
	if input.Snapshot != nil {
		return machine.PinSnapshotSource(ctx, tx, input.OwnerTenantID, machine.SourceLease{
			SnapshotID:  input.Snapshot.Snapshot.ID,
			OperationID: parent.Parent.ID,
			StepKey:     "fork_group",
		})
	}
	return nil
}

type forkBudget struct {
	Bytes   int64
	Memory  int64
	Storage int64
	CPUs    int
}

func forkFunding(input ForkInput) forkBudget {
	shape := input.Template.Config.Shape
	count := int64(len(input.Targets))
	return forkBudget{
		Bytes:   count * ((2*(shape.MemoryMiB+shape.DiskMiB) + 64) << 20),
		Memory:  count * (shape.MemoryMiB + 128),
		Storage: count * shape.DiskMiB,
		CPUs:    int(count) * shape.VCPUs,
	}
}

func (parent *forkAdmission) reserveTargets(ctx context.Context, tx *sql.Tx) error {
	acceptedOperation, input := parent.Parent, parent.Input
	free, capacity := parent.Free, parent.capacity
	budget := forkFunding(input)
	tenant, err := authorization.New(tx).Limits(ctx, input.OwnerTenantID)
	if err != nil {
		return err
	}
	responsibilities := quota.New(tx)
	usage, err := responsibilities.Usage(ctx, input.OwnerTenantID)
	if err != nil {
		return err
	}
	var captureStorage, captureBytes int64
	if input.Source != nil {
		shape := input.Template.Config.Shape
		captureStorage = shape.MemoryMiB + shape.DiskMiB
		captureBytes = (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20
	}
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 || usage.Slots > tenant.MaxSandboxes || int64(len(input.Targets)) > tenant.MaxSandboxes-usage.Slots || usage.StorageMiB > tenant.MaxStorageMiB || budget.Storage+captureStorage > tenant.MaxStorageMiB-usage.StorageMiB {
		return fmt.Errorf("%w: fork batch exceeds tenant quota", errs.ErrExhausted)
	}
	reserved, err := responsibilities.ReservedCapacity(ctx)
	if err != nil {
		return err
	}
	if reserved.Bytes > free || capacity.SafetyBytes > free-reserved.Bytes || budget.Bytes+captureBytes > free-reserved.Bytes-capacity.SafetyBytes || reserved.MemoryMiB > capacity.MaxMemoryMiB || budget.Memory > capacity.MaxMemoryMiB-reserved.MemoryMiB || reserved.CPUs > int64(capacity.MaxVCPUs) || int64(budget.CPUs) > int64(capacity.MaxVCPUs)-reserved.CPUs {
		return fmt.Errorf("%w: fork batch exceeds host reservation", errs.ErrExhausted)
	}
	for _, target := range input.Targets {
		if err := store.RequireOne(dbgen.New(tx).InsertSandbox(ctx, dbgen.InsertSandboxParams{
			ID:     target.ID,
			Tenant: target.TenantID,
			State:  target.State,
			Body:   operation.Encode(target),
		})); err != nil {
			return err
		}
		if err := store.RequireOne(dbgen.New(tx).InsertSandboxSettings(ctx, dbgen.InsertSandboxSettingsParams{
			SandboxID: target.ID,
			Body:      operation.Encode(input.Settings),
		})); err != nil {
			return err
		}
		if err := responsibilities.AddCost(ctx, quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        target.TenantID,
				Kind:            "sandbox_slot",
				Key:             target.ID,
				SourceOperation: acceptedOperation.ID,
			},
			Slots: 1,
		}); err != nil {
			return err
		}
		if err := responsibilities.AddCostReference(ctx, quota.CostReference{
			TenantID:   target.TenantID,
			Kind:       "sandbox_slot",
			Key:        target.ID,
			HolderKind: "active_use",
			HolderID:   forkSlotHolder(acceptedOperation.ID, target.ID),
		}); err != nil {
			return err
		}
	}
	if err := responsibilities.Reserve(ctx, quota.Reservation{
		Owner:     "fork:" + acceptedOperation.ID,
		Bytes:     budget.Bytes,
		MemoryMiB: budget.Memory,
		CPUs:      int64(budget.CPUs),
	}); err != nil {
		return err
	}
	if err := responsibilities.AddCost(ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        input.OwnerTenantID,
			Kind:            "pending_storage",
			Key:             acceptedOperation.ID + ":fork_group",
			SourceOperation: acceptedOperation.ID,
		},
		StorageMiB: budget.Storage,
	}); err != nil {
		return err
	}
	return responsibilities.AddCostReference(ctx, quota.CostReference{
		TenantID:   input.OwnerTenantID,
		Kind:       "pending_storage",
		Key:        acceptedOperation.ID + ":fork_group",
		HolderKind: "engine_dependency",
		HolderID:   acceptedOperation.ID + ":fork_group",
	})
}

// consumeForkForecast removes only the forecast. Real branch reservations replace
// it in this same transaction, so neither storage nor host responsibility lapses.
func consumeForkForecast(ctx context.Context, tx *sql.Tx, step ForkStep) error {
	budget := forkFunding(step.Input)
	responsibilities := quota.New(tx)
	if err := responsibilities.ReleaseMatchingReservation(ctx, quota.Reservation{
		Owner:     "fork:" + step.OperationID,
		Bytes:     budget.Bytes,
		MemoryMiB: budget.Memory,
		CPUs:      int64(budget.CPUs),
	}); err != nil {
		return err
	}
	if err := responsibilities.RemoveCostReference(ctx, quota.CostReference{
		TenantID:   step.Input.OwnerTenantID,
		Kind:       "pending_storage",
		Key:        step.OperationID + ":fork_group",
		HolderKind: "engine_dependency",
		HolderID:   step.OperationID + ":fork_group",
	}); err != nil {
		return err
	}
	return responsibilities.RetireMatchingCost(ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        step.Input.OwnerTenantID,
			Kind:            "pending_storage",
			Key:             step.OperationID + ":fork_group",
			SourceOperation: step.OperationID,
		},
		StorageMiB: budget.Storage,
	})
}

func retireUnallocatedFork(ctx context.Context, tx *sql.Tx, step ForkStep) error {
	if len(step.State.Children) != 0 {
		return fmt.Errorf("allocated fork cannot retire a forecast")
	}
	if err := consumeForkForecast(ctx, tx, step); err != nil {
		return err
	}
	for _, target := range step.Input.Targets {
		current, err := LoadSandbox(ctx, tx, target.ID, target.TenantID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, target) {
			return fmt.Errorf("%w: unallocated fork placement changed", errs.ErrManualRecovery)
		}
		links, err := dbgen.New(tx).CountForkBranchLinks(ctx, step.OperationID)
		if err != nil {
			return err
		}
		if links != 0 {
			return fmt.Errorf("%w: unallocated fork retains a branch dependency", errs.ErrManualRecovery)
		}
		terminated := current
		terminated.State = "terminated"
		terminated.OperationID = ""
		responsibilities := quota.New(tx)
		if err = responsibilities.RemoveCostReference(ctx, quota.CostReference{
			TenantID:   target.TenantID,
			Kind:       "sandbox_slot",
			Key:        target.ID,
			HolderKind: "active_use",
			HolderID:   forkSlotHolder(step.OperationID, target.ID),
		}); err != nil {
			return err
		}
		if err = responsibilities.RetireMatchingCost(ctx, quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        target.TenantID,
				Kind:            "sandbox_slot",
				Key:             target.ID,
				SourceOperation: step.OperationID,
			},
			Slots: 1,
		}); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).TerminateUnallocatedSandbox(ctx, dbgen.TerminateUnallocatedSandboxParams{
			ID:           target.ID,
			Tenant:       target.TenantID,
			PreviousBody: operation.Encode(current),
			NextBody:     operation.Encode(terminated),
		})); err != nil {
			return err
		}
	}
	return nil
}
