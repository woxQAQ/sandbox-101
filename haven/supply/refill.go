package supply

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (service *Supply) runRefill(step SupplyStep) error {
	linked, err := service.runResourceLink(step.OperationID, "clone")
	if err != nil {
		return err
	}
	source, err := machine.LoadResourceStep(service.ctx, service.store.Database, linked.ChildID)
	if err != nil {
		return err
	}
	reservation, status, err := LoadPoolReservation(service.ctx, service.store.Database, step.OperationID)
	if err != nil {
		return err
	}
	if status != "pending" || reservation.Source.OperationID != source.OperationID || reservation.Source.InputHash != source.Hash || source.Status != "succeeded" || source.State.Witness == "" || source.Input.SlotBudget != (quota.SlotBudget{
		Kind:  "sandbox_slot",
		Units: 0,
	}) {
		return fmt.Errorf("refill source differs from original pool reservation")
	}
	lock := service.guards.VM(source.Candidate)
	lock.Lock()
	defer lock.Unlock()
	if err = service.opts.Backend.Network(service.ctx, source.Input.Resource, false); err != nil {
		return err
	}
	observed, err := service.opts.Backend.Inspect(service.ctx, source.Input.Resource)
	if err != nil {
		return err
	}
	if !observed.Alive || !observed.Booted || observed.Network {
		return fmt.Errorf("%w: warm source readiness/network verification failed", errs.ErrConflict)
	}
	if service.opts.Guest == nil {
		return fmt.Errorf("%w: guest effects unavailable", errs.ErrInvalid)
	}
	if err = service.opts.Guest.Info(service.ctx, source.Input.Resource); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSupply(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("refill current intent changed")
	}
	queries := dbgen.New(tx)
	original, state, err := LoadPoolReservation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if state != "pending" || !reflect.DeepEqual(original, reservation) {
		return fmt.Errorf("refill pending pool responsibility changed")
	}
	if err = service.proofs.VerifyGeneration(service.ctx, tx, source); err != nil {
		return err
	}
	vm, instance, owner, err := machine.LoadVM(service.ctx, tx, source)
	if err != nil {
		return err
	}
	if owner != "" || instance != source.State.Witness || !vm.Verified || !vm.AgentReady || vm.Deleting {
		return fmt.Errorf("refill candidate ownership changed")
	}
	reservation.Instance = source.State.Witness
	body, err := operation.New(tx).LiveBody(service.ctx, step.OperationID)
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
	if err = store.RequireOne(queries.InsertWarm(service.ctx, dbgen.InsertWarmParams{
		VmID:       reservation.VMID,
		TemplateID: reservation.Template.ID,
		SnapshotID: reservation.Template.SnapshotID,
		CreatedAt:  reservation.CreatedAt.UnixNano(),
	})); err != nil {
		return err
	}
	if err = references.AddResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         reservation.VMID,
		HolderKind: "warm",
		HolderID:   step.OperationID,
	}); err != nil {
		return err
	}
	if err = store.RequireOne(queries.MarkReady(service.ctx, dbgen.MarkReadyParams{
		OperationID:  step.OperationID,
		PreviousBody: previous,
		NewBody:      next,
	})); err != nil {
		return err
	}
	if err = references.RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         reservation.Template.Config.Image,
		HolderKind: "step",
		HolderID:   step.OperationID + ":supply",
	}); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteUnchangedStep(service.ctx, dbgen.CompleteUnchangedStepParams{
		OperationID:  step.OperationID,
		StepKey:      "supply",
		InputHash:    step.Hash,
		PreviousBody: previousState,
		NewBody:      completed,
	})); err != nil {
		return err
	}
	if err = operations.CompleteResult(service.ctx, step.OperationID, resultBody); err != nil {
		return err
	}
	if _, _, err = VerifyReadyWarm(service.proofs, service.ctx, tx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("warm_publication_before_commit"); err != nil {
		return err
	}
	if err = service.committer.Commit(tx); err != nil {
		return err
	}
	return service.store.Checkpoint()
}
