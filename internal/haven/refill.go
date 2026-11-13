package haven

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) runRefill(step supplyStep) error {
	linked, err := service.runResourceLink(step.OperationID, "clone")
	if err != nil {
		return err
	}
	source, err := loadStep(service.ctx, service.store.db, linked.ChildID)
	if err != nil {
		return err
	}
	pool, status, err := loadPoolReservation(service.ctx, service.store.db, step.OperationID)
	if err != nil {
		return err
	}
	if status != "pending" || pool.Source.OperationID != source.OperationID || pool.Source.InputHash != source.Hash || source.Status != "succeeded" || source.State.Witness == "" || source.Input.SlotBudget != (slotBudget{Kind: "sandbox_slot", Units: 0}) {
		return fmt.Errorf("refill source differs from original pool reservation")
	}
	lock := service.vmLock(source.Candidate)
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
		return loom.Fail(loom.Conflict, "warm source readiness/network verification failed")
	}
	if service.opts.Guest == nil {
		return loom.Fail(loom.Invalid, "guest effects unavailable")
	}
	if err = service.opts.Guest.Info(service.ctx, source.Input.Resource); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadSupply(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("refill current intent changed")
	}
	original, state, err := loadPoolReservation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if state != "pending" || !reflect.DeepEqual(original, pool) {
		return fmt.Errorf("refill pending pool responsibility changed")
	}
	if err = service.verifyGeneration(service.ctx, tx, source); err != nil {
		return err
	}
	vm, instance, owner, err := service.loadVM(service.ctx, tx, source)
	if err != nil {
		return err
	}
	if owner != "" || instance != source.State.Witness || !vm.Verified || !vm.AgentReady || vm.Deleting {
		return fmt.Errorf("refill candidate ownership changed")
	}
	pool.Instance = source.State.Witness
	var body []byte
	if err = tx.QueryRowContext(service.ctx, "SELECT body FROM operations WHERE id=? AND status IN ('pending','running')", step.OperationID).Scan(&body); err != nil {
		return err
	}
	var result operationResult
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	result.VMID = pool.VMID
	result.ResourceReady = true
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO warm VALUES(?,?,?,?)", []any{pool.VMID, pool.Template.ID, pool.Template.SnapshotID, pool.CreatedAt.UnixNano()}},
		{"INSERT INTO resource_refs VALUES('vm',?,'warm',?)", []any{pool.VMID, step.OperationID}},
		{"UPDATE pool_slots SET status='ready',body=? WHERE operation_id=? AND status='pending' AND body=?", []any{encode(pool), step.OperationID, encode(original)}},
		{"DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{pool.Template.Config.Image, step.OperationID + ":supply"}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='supply' AND input_hash=? AND body=? AND status IN ('pending','running')", []any{encode(supplyState{Phase: "complete", SnapshotID: pool.Template.SnapshotID, FinishedAt: time.Now().UTC()}), step.OperationID, step.Hash, encode(step.State)}},
		{"UPDATE operations SET status='succeeded',body=? WHERE id=? AND status IN ('pending','running')", []any{encode(result), step.OperationID}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if _, _, err = service.verifyReadyWarm(service.ctx, tx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("warm_publication_before_commit"); err != nil {
		return err
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	return service.store.checkpoint()
}
