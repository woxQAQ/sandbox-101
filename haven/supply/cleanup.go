package supply

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
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

type SupplyCleanupDependencies struct {
	Guards    SupplyCleanupGuards
	Gate      execution.AdmissionGate
	Workers   *execution.Workers
	Proofs    machine.ResourceProofs
	Stops     *machine.VMStops
	Captures  *machine.CaptureDiscard
	Deletion  *machine.VMDeletion
	Ownership CleanupOwnershipReader
}

func NewSupplyCleanup(state *store.State, dependencies SupplyCleanupDependencies, fault func(string) error) *SupplyCleanup {
	return &SupplyCleanup{
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
		stops:      dependencies.Stops,
		captures:   dependencies.Captures,
		deletion:   dependencies.Deletion,
		ownership:  dependencies.Ownership,
		faultPoint: fault,
	}
}

func (sc *SupplyCleanup) fault(point string) error {
	if sc.faultPoint != nil {
		return sc.faultPoint(point)
	}
	return nil
}

func (sc *SupplyCleanup) dispatch(id string) {
	if sc.store.Reviewed {
		return
	}
	sc.workers.Start(id, func() error {
		_, err := sc.Resume(id)
		return err
	})
}

func (sc *SupplyCleanup) Resume(id string) (bool, error) {
	if sc.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume supply cleanup", errs.ErrManualRecovery)
	}
	step, err := LoadSupplyCleanup(sc.ctx, sc.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := sc.guards.SupplyCleanup(step.Input.Source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadSupplyCleanup(sc.ctx, sc.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, sc.runSupplyCleanup(step)
}

type SupplyCleanupAdmission struct {
	Parent SupplyCleanupStep
	Role   string
}

func (admission *SupplyCleanupAdmission) SupplyCleanupOperation() (string, bool) {
	if admission == nil {
		return "", false
	}
	return admission.Parent.OperationID, true
}

func (admission *SupplyCleanupAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, request machine.ResourceAdmission) error {
	current, err := LoadSupplyCleanup(ctx, tx, admission.Parent.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(current.Status) || current.Hash != admission.Parent.Hash || !reflect.DeepEqual(current.Input, admission.Parent.Input) {
		return fmt.Errorf("%w: supply cleanup admission changed", errs.ErrConflict)
	}
	return machine.InsertResourceLink(ctx, tx, machine.ResourceLinkTarget{
		OperationID:      current.OperationID,
		Role:             admission.Role,
		ExpectedInstance: current.Input.Source.Input.Resource.Instance,
	}, request)
}

func (sc *SupplyCleanup) AdmitSupplyCleanup(ctx context.Context, principal authorization.Principal, key string, request SupplyCleanupRequest) (operation.Record, error) {
	var empty operation.Record
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || !loom.ValidResourceID(request.OperationID, "op-") {
		return empty, fmt.Errorf("%w: supply cleanup requires management authority, original operation and key", errs.ErrInvalid)
	}
	requestGuard := sc.guards.NodeRequest(sc.nodeID, key)
	requestGuard.Lock()
	defer requestGuard.Unlock()
	frozen := operation.Encode(struct {
		Action  string
		Request SupplyCleanupRequest
	}{"supply_cleanup", request})
	hash := operation.Digest(frozen)
	if cached, found, err := operation.New(sc.store.Database).Cached(ctx, operation.Scope{
		Kind: "node",
		ID:   sc.nodeID,
	}, key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			sc.dispatch(cached.ID)
		}
		return cached, err
	}
	original, err := LoadSupply(ctx, sc.store.Database, request.OperationID)
	if err != nil {
		return empty, err
	}
	guard := sc.guards.TemplateWork(original.Input.Template.ID)
	if !guard.TryLock() {
		return empty, fmt.Errorf("%w: supply workflow is active", errs.ErrConflict)
	}
	defer guard.Unlock()
	sc.admission.Lock()
	defer sc.admission.Unlock()
	if err = sc.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := sc.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	original, err = LoadSupply(ctx, tx, request.OperationID)
	if err != nil {
		return empty, err
	}
	if original.Status != "failed" && (original.Status != "succeeded" || original.Input.Command.Action != "refill") {
		return empty, fmt.Errorf("%w: supply cleanup requires fixed failed intent or unclaimed warm publication", errs.ErrConflict)
	}
	role := "clone"
	if original.Input.Command.Action == "template_build" {
		role = "cold"
	}
	link, _, err := machine.LoadResourceLink(ctx, tx, original.OperationID, role)
	if err != nil {
		return empty, err
	}
	source, err := machine.LoadResourceStep(ctx, tx, link.ChildID)
	if err != nil {
		return empty, err
	}
	if !operation.Terminal(source.Status) {
		return empty, fmt.Errorf("%w: supply resource outcome remains unresolved", errs.ErrConflict)
	}
	if err = machine.AllocationCleanupFence(ctx, tx, source.Candidate, nil); err != nil {
		return empty, err
	}
	if source.Input.SandboxID != original.Input.SandboxID || source.Input.TenantID != original.Input.Template.TenantID {
		return empty, fmt.Errorf("%w: supply source ownership changed", errs.ErrManualRecovery)
	}
	record, err := sc.ownership.ReadSandbox(ctx, tx, source.Input.SandboxID, source.Input.TenantID)
	if err != nil {
		return empty, err
	}
	settings, err := sc.ownership.SettingsCount(ctx, tx, record.ID)
	if err != nil {
		return empty, err
	}
	if settings != 0 || record.SlotAssignment != nil || record.SourceOperation != source.OperationID || record.VMID != source.Candidate {
		return empty, fmt.Errorf("%w: raw cleanup cannot bypass sandbox ownership", errs.ErrConflict)
	}
	input := SupplyCleanupInput{
		RequestTenantID: principal.TenantID,
		Request:         request,
		Original:        original,
		Source:          source,
	}
	if released, status, lookup := machine.LoadResourceLink(ctx, tx, original.OperationID, "release"); lookup == nil && status == "succeeded" {
		receipt, err := machine.LoadResourceStep(ctx, tx, released.ChildID)
		if err != nil {
			return empty, err
		}
		if receipt.Status != "succeeded" || receipt.State.Phase != "complete" || receipt.Input.SourceOperation != source.OperationID || receipt.Candidate != source.Candidate || record.State != "terminated" {
			return empty, fmt.Errorf("%w: supply original retirement receipt changed", errs.ErrManualRecovery)
		}
		input.Retired = &ownership.GenerationRef{
			OperationID: receipt.OperationID,
			InputHash:   receipt.Hash,
		}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return empty, lookup
	}
	if input.Retired == nil {
		if record.State != "provisioning" {
			return empty, fmt.Errorf("%w: supply current allocation changed", errs.ErrConflict)
		}
		currentMachine, instance, owner, err := machine.LoadVM(ctx, tx, source)
		if err != nil {
			return empty, err
		}
		if currentMachine.Deleting || (source.Status == "succeeded" && (owner != "" || instance != source.State.Witness || !currentMachine.Verified || !currentMachine.AgentReady)) || (source.Status == "failed" && owner != source.OperationID) {
			return empty, fmt.Errorf("%w: supply current resource ownership changed", errs.ErrConflict)
		}
		if source.Status == "succeeded" {
			refs, err := machine.FailedCaptureReferences(ctx, tx, source)
			if err != nil {
				return empty, err
			}
			input.CaptureFailures = refs
		}
	}
	if original.Input.Command.Action == "refill" {
		reservation, status, err := LoadPoolReservation(ctx, tx, original.OperationID)
		if err != nil {
			return empty, err
		}
		if reservation.Source.OperationID != source.OperationID || reservation.Source.InputHash != source.Hash || (status != "ready" && status != "cleanup") {
			return empty, fmt.Errorf("%w: supply pool responsibility was selected or changed", errs.ErrConflict)
		}
		if status == "ready" {
			if _, _, err = VerifyReadyWarm(sc.proofs, ctx, tx, original.OperationID); err != nil {
				return empty, err
			}
			if err = store.RequireOne(dbgen.New(tx).RemoveWarm(ctx, dbgen.RemoveWarmParams{
				VmID:       reservation.VMID,
				TemplateID: reservation.Template.ID,
				SnapshotID: reservation.Template.SnapshotID,
			})); err != nil {
				return empty, err
			}
			if err = quota.New(tx).RemoveResourceReference(ctx, quota.ResourceReference{
				Kind:       "vm",
				ID:         reservation.VMID,
				HolderKind: "warm",
				HolderID:   original.OperationID,
			}); err != nil {
				return empty, err
			}
			if err = store.RequireOne(dbgen.New(tx).MarkReadyCleanup(ctx, dbgen.MarkReadyCleanupParams{
				OperationID: original.OperationID,
				Body:        operation.Encode(reservation),
			})); err != nil {
				return empty, err
			}
		}
		input.Pool = &reservation
	}
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			SandboxID: record.ID,
			VMID:      source.Candidate,
		},
	}
	payload := operation.Encode(input)
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "node",
		ScopeID:   sc.nodeID,
		Key:       key,
		Hash:      hash,
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "supply_cleanup",
		Action:              "supply_cleanup",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: source.Candidate,
		ExpectedInstance:    source.Input.Resource.Instance,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "supply_cleanup:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: "supply_cleanup",
		ResourceID:   source.Candidate,
		Body:         payload,
	})); err != nil {
		return empty, err
	}
	if err = quota.New(tx).AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         source.Input.Resource.Config.Image,
		HolderKind: "step",
		HolderID:   accepted.ID + ":supply_cleanup",
	}); err != nil {
		return empty, err
	}
	if err = sc.fault("supply_cleanup_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = sc.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = sc.fault("supply_cleanup_admitted"); err != nil {
		return accepted, err
	}
	sc.dispatch(accepted.ID)
	return accepted, nil
}

func (sc *SupplyCleanup) completeResourceLink(parent, role string, link machine.ResourceLink) error {
	sc.admission.Lock()
	defer sc.admission.Unlock()
	tx, err := sc.store.Database.BeginTx(sc.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	changed, err := link.Complete(sc.ctx, tx, parent, role)
	if err != nil || !changed {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return sc.store.Checkpoint()
}

func (sc *SupplyCleanup) resumeChild(link machine.ResourceLink) error {
	var handled bool
	var err error
	switch link.Kind {
	case "stop":
		handled, err = sc.stops.Resume(link.ChildID)
	case "capture_discard":
		handled, err = sc.captures.Resume(link.ChildID)
	case "release":
		handled, err = sc.deletion.ResumeRelease(link.ChildID)
	case "discard":
		handled, err = sc.deletion.ResumeDiscard(link.ChildID)
	default:
		return fmt.Errorf("supply cleanup child kind unsupported")
	}
	if err != nil {
		return err
	}
	if !handled {
		return fmt.Errorf("supply cleanup child lacks matching immutable step")
	}
	return nil
}

func (sc *SupplyCleanup) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	link, status, err := machine.LoadResourceLink(sc.ctx, sc.store.Database, parent, role)
	if err != nil {
		return link, err
	}
	if status == "failed" {
		return link, fmt.Errorf("%w: retained lifecycle resource failed", errs.ErrConflict)
	}
	if status == "succeeded" {
		return link, nil
	}
	runErr := sc.resumeChild(link)
	if errors.Is(runErr, errs.ErrInterrupted) {
		return link, runErr
	}
	if err = sc.completeResourceLink(parent, role, link); err != nil {
		return link, errors.Join(err, runErr)
	}
	_, status, err = machine.LoadResourceLink(sc.ctx, sc.store.Database, parent, role)
	if err != nil {
		return link, err
	}
	if status != "succeeded" {
		return link, fmt.Errorf("%w: retained lifecycle resource failed", errs.ErrConflict)
	}
	return link, nil
}

// CleanupOwnershipReader checks that a retained raw allocation has not acquired
// sandbox settings. Both reads use the caller's original transaction.
type CleanupOwnershipReader interface {
	machine.SandboxOwnershipReader
	SettingsCount(context.Context, store.DBTX, string) (int64, error)
}

func (sc *SupplyCleanup) runSupplyCleanup(step SupplyCleanupStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if step.Input.Retired == nil {
		source := step.Input.Source
		if len(step.Input.CaptureFailures) > 0 {
			if _, _, lookup := machine.LoadResourceLink(sc.ctx, sc.store.Database, step.OperationID, "stop"); errors.Is(lookup, sql.ErrNoRows) {
				parent := &SupplyCleanupAdmission{
					Parent: step,
					Role:   "stop",
				}
				if _, err := sc.stops.AdmitForParent(sc.ctx, machine.StopRequest{
					TenantID:         source.Input.TenantID,
					Key:              step.OperationID + ":stop",
					SandboxID:        source.Input.SandboxID,
					ExpectedInstance: source.State.Witness,
				}, parent); err != nil {
					return err
				}
			} else if lookup != nil {
				return lookup
			}
			stopped, err := sc.runResourceLink(step.OperationID, "stop")
			if err != nil {
				return err
			}
			for _, reference := range step.Input.CaptureFailures {
				role := "capture:" + reference.OperationID
				if _, _, lookup := machine.LoadResourceLink(sc.ctx, sc.store.Database, step.OperationID, role); errors.Is(lookup, sql.ErrNoRows) {
					parent := &SupplyCleanupAdmission{
						Parent: step,
						Role:   role,
					}
					if _, err = sc.captures.AdmitForParent(sc.ctx, machine.CaptureDiscardRequest{
						TenantID:          source.Input.TenantID,
						Key:               step.OperationID + ":" + role,
						SandboxID:         source.Input.SandboxID,
						FailedOperationID: reference.OperationID,
						ExpectedInstance:  source.State.Witness,
						StopOperationID:   stopped.ChildID,
					}, parent); err != nil {
						return err
					}
				} else if lookup != nil {
					return lookup
				}
				if _, err = sc.runResourceLink(step.OperationID, role); err != nil {
					return err
				}
			}
		}
		if _, _, lookup := machine.LoadResourceLink(sc.ctx, sc.store.Database, step.OperationID, "resource"); errors.Is(lookup, sql.ErrNoRows) {
			parent := &SupplyCleanupAdmission{
				Parent: step,
				Role:   "resource",
			}
			if source.Status == "failed" {
				if _, err := sc.deletion.AdmitDiscardForParent(sc.ctx, machine.DiscardRequest{
					TenantID:          source.Input.TenantID,
					Key:               step.OperationID + ":resource",
					SandboxID:         source.Input.SandboxID,
					FailedOperationID: source.OperationID,
					ExpectedInstance:  source.Input.Resource.Instance,
				}, parent); err != nil {
					return err
				}
			} else {
				if _, err := sc.deletion.AdmitReleaseForParent(sc.ctx, machine.ReleaseRequest{
					TenantID:  source.Input.TenantID,
					Key:       step.OperationID + ":resource",
					SandboxID: source.Input.SandboxID,
				}, parent); err != nil {
					return err
				}
			}
		} else if lookup != nil {
			return lookup
		}
		if _, err := sc.runResourceLink(step.OperationID, "resource"); err != nil {
			return err
		}
	}
	return sc.completeSupplyCleanup(step)
}

func (sc *SupplyCleanup) completeSupplyCleanup(step SupplyCleanupStep) error {
	sc.admission.Lock()
	defer sc.admission.Unlock()
	tx, err := sc.store.Database.BeginTx(sc.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSupplyCleanup(sc.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("supply cleanup current intent changed")
	}
	original, err := LoadSupply(sc.ctx, tx, step.Input.Original.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(original, step.Input.Original) {
		return fmt.Errorf("supply cleanup original terminal changed")
	}
	source := step.Input.Source
	if step.Input.Retired != nil {
		retired, err := machine.LoadResourceStep(sc.ctx, tx, step.Input.Retired.OperationID)
		if err != nil {
			return err
		}
		if retired.Hash != step.Input.Retired.InputHash || retired.Status != "succeeded" || retired.State.Phase != "complete" || retired.Input.SourceOperation != source.OperationID {
			return fmt.Errorf("supply cleanup original retirement proof changed")
		}
	} else {
		link, status, err := machine.LoadResourceLink(sc.ctx, tx, step.OperationID, "resource")
		if err != nil {
			return err
		}
		if status != "succeeded" {
			return errs.ErrInterrupted
		}
		switch link.Kind {
		case "release":
			retired, err := machine.LoadResourceStep(sc.ctx, tx, link.ChildID)
			if err != nil {
				return err
			}
			if retired.Status != "succeeded" || retired.State.Phase != "complete" || retired.Input.SourceOperation != source.OperationID || retired.Candidate != source.Candidate {
				return fmt.Errorf("supply cleanup release receipt changed")
			}
		case "discard":
			retired, err := machine.LoadDiscard(sc.ctx, tx, link.ChildID)
			if err != nil {
				return err
			}
			if retired.Status != "succeeded" || retired.State.Phase != "complete" || !reflect.DeepEqual(retired.Input.Source, source) {
				return fmt.Errorf("supply cleanup failed-candidate receipt changed")
			}
		default:
			return fmt.Errorf("supply cleanup resource link kind unsupported")
		}
	}
	count, err := machine.CurrentVMCount(sc.ctx, tx, source.Candidate)
	if err != nil || count != 0 {
		return fmt.Errorf("supply cleanup VM responsibility remains: %v", err)
	}
	if step.Input.Pool != nil {
		reservation, status, err := LoadPoolReservation(sc.ctx, tx, original.OperationID)
		if err != nil {
			return err
		}
		if status != "cleanup" || !reflect.DeepEqual(reservation, *step.Input.Pool) {
			return fmt.Errorf("supply cleanup pool slot changed")
		}
		if err = store.RequireOne(dbgen.New(tx).DeleteReservation(sc.ctx, dbgen.DeleteReservationParams{
			OperationID: original.OperationID,
			Status:      "cleanup",
			Body:        operation.Encode(reservation),
		})); err != nil {
			return err
		}
	}
	if original.Status == "failed" {
		if err = quota.New(tx).RemoveResourceReference(sc.ctx, quota.ResourceReference{
			Kind:       "image",
			ID:         source.Input.Resource.Config.Image,
			HolderKind: "step",
			HolderID:   original.OperationID + ":supply",
		}); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanupKind(sc.ctx, dbgen.CompletePendingCleanupKindParams{
			ID:           "supply:" + original.OperationID,
			OperationID:  original.OperationID,
			ResourceKind: "supply",
		})); err != nil {
			return err
		}
	}
	if err = quota.New(tx).RemoveResourceReference(sc.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         source.Input.Resource.Config.Image,
		HolderKind: "step",
		HolderID:   step.OperationID + ":supply_cleanup",
	}); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanupKind(sc.ctx, dbgen.CompletePendingCleanupKindParams{
		ID:           "supply_cleanup:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "supply_cleanup",
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteUnchangedStep(sc.ctx, dbgen.CompleteUnchangedStepParams{
		OperationID:  step.OperationID,
		StepKey:      "supply_cleanup",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NewBody:      operation.Encode("complete"),
	})); err != nil {
		return err
	}
	if err = operations.Complete(sc.ctx, step.OperationID); err != nil {
		return err
	}
	if err = sc.fault("supply_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return sc.store.Checkpoint()
}
