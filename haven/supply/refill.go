package supply

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (s *Supply) runRefill(step SupplyStep) error {
	linked, err := s.runResourceLink(step.OperationID, "clone")
	if err != nil {
		return err
	}
	source, err := machine.LoadResourceStep(s.ctx, s.store.Database, linked.ChildID)
	if err != nil {
		return err
	}
	reservation, status, err := LoadPoolReservation(s.ctx, s.store.Database, step.OperationID)
	if err != nil {
		return err
	}
	if status != "pending" || reservation.Source.OperationID != source.OperationID || reservation.Source.InputHash != source.Hash || source.Status != "succeeded" || source.State.Witness == "" || source.Input.SlotBudget != (quota.SlotBudget{
		Kind:  "sandbox_slot",
		Units: 0,
	}) {
		return fmt.Errorf("refill source differs from original pool reservation")
	}
	lock := s.guards.VM(source.Candidate)
	lock.Lock()
	defer lock.Unlock()
	if err = s.opts.Backend.Network(s.ctx, source.Input.Resource, false); err != nil {
		return err
	}
	observed, err := s.opts.Backend.Inspect(s.ctx, source.Input.Resource)
	if err != nil {
		return err
	}
	if !observed.Alive || !observed.Booted || observed.Network {
		return fmt.Errorf("%w: warm source readiness/network verification failed", errs.ErrConflict)
	}
	if s.opts.Guest == nil {
		return fmt.Errorf("%w: guest effects unavailable", errs.ErrInvalid)
	}
	if err = s.opts.Guest.Info(s.ctx, source.Input.Resource); err != nil {
		return err
	}
	s.admission.Lock()
	defer s.admission.Unlock()
	tx, err := s.store.Database.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSupply(s.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("refill current intent changed")
	}
	queries := dbgen.New(tx)
	original, state, err := LoadPoolReservation(s.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if state != "pending" || !reflect.DeepEqual(original, reservation) {
		return fmt.Errorf("refill pending pool responsibility changed")
	}
	if err = s.proofs.VerifyGeneration(s.ctx, tx, source); err != nil {
		return err
	}
	vm, instance, owner, err := machine.LoadVM(s.ctx, tx, source)
	if err != nil {
		return err
	}
	if owner != "" || instance != source.State.Witness || !vm.Verified || !vm.AgentReady || vm.Deleting {
		return fmt.Errorf("refill candidate ownership changed")
	}
	reservation.Instance = source.State.Witness
	body, err := operation.New(tx).LiveBody(s.ctx, step.OperationID)
	if err != nil {
		return err
	}
	var result operation.Result
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	result.VMID = reservation.VMID
	result.ResourceReady = true
	completed := operation.Encode(SupplyState{
		Phase:      "complete",
		SnapshotID: reservation.Template.SnapshotID,
		FinishedAt: time.Now().UTC(),
	})
	previous, next := operation.Encode(original), operation.Encode(reservation)
	previousState, resultBody := operation.Encode(step.State), operation.Encode(result)
	references := quota.New(tx)
	operations := operation.New(tx)
	if err = store.RequireOne(queries.InsertWarm(s.ctx, dbgen.InsertWarmParams{
		VmID:       reservation.VMID,
		TemplateID: reservation.Template.ID,
		SnapshotID: reservation.Template.SnapshotID,
		CreatedAt:  reservation.CreatedAt.UnixNano(),
	})); err != nil {
		return err
	}
	if err = references.AddResourceReference(s.ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         reservation.VMID,
		HolderKind: "warm",
		HolderID:   step.OperationID,
	}); err != nil {
		return err
	}
	if err = store.RequireOne(queries.MarkReady(s.ctx, dbgen.MarkReadyParams{
		OperationID:  step.OperationID,
		PreviousBody: previous,
		NewBody:      next,
	})); err != nil {
		return err
	}
	if err = references.RemoveResourceReference(s.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         reservation.Template.Config.Image,
		HolderKind: "step",
		HolderID:   step.OperationID + ":supply",
	}); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteUnchangedStep(s.ctx, dbgen.CompleteUnchangedStepParams{
		OperationID:  step.OperationID,
		StepKey:      "supply",
		InputHash:    step.Hash,
		PreviousBody: previousState,
		NewBody:      completed,
	})); err != nil {
		return err
	}
	if err = operations.CompleteResult(s.ctx, step.OperationID, resultBody); err != nil {
		return err
	}
	if _, _, err = VerifyReadyWarm(s.proofs, s.ctx, tx, step.OperationID); err != nil {
		return err
	}
	if err = s.fault("warm_publication_before_commit"); err != nil {
		return err
	}
	if err = s.committer.Commit(tx); err != nil {
		return err
	}
	return s.store.Checkpoint()
}

func (s *Supply) AdmitRefill(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	var empty operation.Record
	if principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || command.Action != "refill" || command.TemplateID == "" {
		return empty, fmt.Errorf("%w: refill requires tenant authority, template and key", errs.ErrInvalid)
	}
	guard := s.guards.TenantRequest(principal.TenantID, key)
	guard.Lock()
	defer guard.Unlock()
	mac := s.commands.CommandMAC(principal, command)
	if accepted, found, err := s.existing(ctx, principal, key, mac); found || err != nil {
		return accepted, err
	}
	draining := s.availability.IsDraining()
	if draining {
		return empty, fmt.Errorf("%w: node is draining", errs.ErrExhausted)
	}
	catalog := NewTemplateCatalog(s.store.Database)
	public, err := catalog.GetTemplate(ctx, principal, command.TemplateID)
	if err != nil {
		return empty, err
	}
	selected, err := catalog.Owned(ctx, public.TenantID, public.ID)
	if err != nil {
		return empty, err
	}
	if selected.TargetWarm < 1 || selected.SnapshotID == "" {
		return empty, fmt.Errorf("%w: refill requires golden template with warm target", errs.ErrInvalid)
	}
	now := time.Now().UTC()
	blocked, err := RefillBackoff(ctx, s.store.Database, selected.ID, now)
	if err != nil {
		return empty, err
	}
	if blocked {
		return empty, fmt.Errorf("%w: template refill backoff is active", errs.ErrExhausted)
	}
	source, err := s.proofs.Snapshots().LoadRestoreSource(ctx, s.store.Database, selected.TenantID, selected.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = machine.VerifyRestoreSourceFiles(source); err != nil {
		return empty, err
	}
	if err = s.opts.Backend.Check(ctx, selected.Config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, s.store.Database, selected.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(s.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	parent := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			TemplateID: selected.ID,
			SandboxID:  operation.NewID("sandbox_"),
			SnapshotID: selected.SnapshotID,
		},
	}
	input := SupplyInput{
		CreatedAt:       now,
		RequestTenantID: principal.TenantID,
		Command:         operation.SanitizedCommand(command),
		Template:        selected,
		Source:          &source,
		SandboxID:       parent.Result.SandboxID,
	}
	admission := &SupplyAdmission{
		Parent: parent,
		Input:  input,
		MAC:    mac,
		Role:   "clone",
	}
	request := machine.CloneRequest{
		TenantID:   selected.TenantID,
		Key:        parent.ID + ":clone",
		SnapshotID: selected.SnapshotID,
	}
	frozen := operation.Encode(struct {
		Action  string
		Request machine.CloneRequest
	}{"clone", request})
	child, err := s.provisioning.AdmitRaw(ctx, ProvisionRequest{
		TenantID: selected.TenantID,
		Key:      request.Key,
		Frozen:   frozen,
		Template: selected,
		Image:    manifest,
		Restore:  &source,
	}, RawTarget{
		NewSandboxID: input.SandboxID,
		Budget: &quota.SlotBudget{
			Kind:  "sandbox_slot",
			Units: 0,
		},
		Parent: admission,
	})
	if child.ID == "" {
		if errors.Is(err, errs.ErrExhausted) {
			return s.recordRefillFailure(ctx, parent, input, mac)
		}
		return empty, err
	}
	parent.Result.VMID = child.Result.VMID
	if err != nil {
		return parent, err
	}
	s.dispatch(parent.ID)
	return parent, nil
}

func (s *Supply) recordRefillFailure(ctx context.Context, parent operation.Record, input SupplyInput, mac string) (operation.Record, error) {
	s.admission.Lock()
	defer s.admission.Unlock()
	if err := s.gate.Check(); err != nil {
		return operation.Record{}, err
	}
	tx, err := s.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return operation.Record{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Command
	}{"refill", input.Command})
	payload := operation.Encode(input)
	parent.Status = "failed"
	parent.Result.Error = "refill admission exhausted"
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        parent.ID,
		ScopeKind: "tenant",
		ScopeID:   input.RequestTenantID,
		Key:       parent.Key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "failed",
		Body:      operation.Encode(parent.Result),
	}); err != nil {
		return operation.Record{}, err
	}
	if err = operations.InsertCredential(ctx, parent.ID, mac); err != nil {
		return operation.Record{}, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         parent.ID,
		StepKey:             "supply",
		Action:              "refill",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: input.Template.ID,
		ExpectedInstance:    "",
		Status:              "failed",
		Body: operation.Encode(SupplyState{
			Phase:      "failed",
			FinishedAt: time.Now().UTC(),
			Error:      parent.Result.Error,
		}),
	})); err != nil {
		return operation.Record{}, err
	}
	if err = s.committer.Commit(tx); err != nil {
		return operation.Record{}, err
	}
	return parent, s.store.Checkpoint()
}

func RefillBackoff(ctx context.Context, database store.DBTX, template string, now time.Time) (bool, error) {
	attempts, err := dbgen.New(database).ListRefillAttempts(ctx, template)
	if err != nil {
		return true, err
	}
	failures := 0
	var latest time.Time
	for _, attempt := range attempts {
		if attempt.Status == "succeeded" {
			break
		}
		var intent SupplyInput
		var state SupplyState
		if err = json.Unmarshal(attempt.Input, &intent); err != nil {
			return true, err
		}
		if err = json.Unmarshal(attempt.Body, &state); err != nil {
			return true, err
		}
		if failures == 0 {
			latest = state.FinishedAt
			if latest.IsZero() {
				latest = intent.CreatedAt
			}
		}
		failures++
	}
	if failures == 0 {
		return false, nil
	}
	delay := time.Second << min(failures-1, 8)
	return now.Before(latest.Add(delay)), nil
}

// RefillReady considers verified templates and admits only missing warm supply.
// Full pools and active backoff create no intent; admission rechecks capacity.
func (s *Supply) RefillReady(ctx context.Context) error {
	templates, err := NewTemplateCatalog(s.store.Database).Templates(ctx, authorization.Principal{
		Root: true,
	})
	if err != nil {
		return err
	}
	var failures []error
	for _, candidate := range templates {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		if candidate.TargetWarm < 1 || candidate.SnapshotID == "" {
			continue
		}
		count, err := dbgen.New(s.store.Database).CountTemplateReservations(ctx, candidate.ID)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if count >= int64(candidate.TargetWarm) {
			continue
		}
		blocked, err := RefillBackoff(ctx, s.store.Database, candidate.ID, time.Now().UTC())
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if blocked {
			continue
		}
		_, err = s.AdmitRefill(ctx, authorization.Principal{
			Root:     true,
			TenantID: candidate.TenantID,
		}, "refill:"+operation.NewID(""), operation.Command{
			Action:     "refill",
			TemplateID: candidate.ID,
		})
		if err != nil && !errors.Is(err, errs.ErrExhausted) && !errors.Is(err, errs.ErrConflict) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
