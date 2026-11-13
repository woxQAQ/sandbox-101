package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

type (
	SupplyCleanupRequest struct {
		OperationID string `json:"operation_id"`
	}
	supplyCleanupInput struct {
		RequestTenantID string               `json:"request_tenant_id"`
		Request         SupplyCleanupRequest `json:"request"`
		Original        supplyStep           `json:"original"`
		Source          resourceStep         `json:"source"`
		Pool            *poolReservation     `json:"pool,omitempty"`
		Retired         *generationRef       `json:"retired,omitempty"`
		CaptureFailures []generationRef      `json:"capture_failures,omitempty"`
	}
)

type (
	supplyCleanupStep struct {
		OperationID, Hash, Status, Phase string
		Input                            supplyCleanupInput
	}
	supplyCleanupAdmission struct {
		Parent supplyCleanupStep
		Role   string
	}
)

func loadSupplyCleanup(ctx context.Context, q queryer, id string) (supplyCleanupStep, error) {
	step := supplyCleanupStep{OperationID: id}
	var payload, body []byte
	var action, candidate, expected string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,action,candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key='supply_cleanup'", id).Scan(&step.Hash, &payload, &step.Status, &body, &action, &candidate, &expected)
	if err != nil {
		return step, err
	}
	if digest(payload) != step.Hash {
		return step, fmt.Errorf("supply cleanup input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "supply_cleanup" || candidate != step.Input.Source.Candidate || expected != step.Input.Source.Input.Resource.Instance || step.Input.Request.OperationID != step.Input.Original.OperationID {
		return step, fmt.Errorf("supply cleanup frozen identity changed")
	}
	return step, nil
}

func (parent *supplyCleanupAdmission) admitResource(ctx context.Context, service *Service, tx *sql.Tx, child Operation, kind, candidate, hash string) error {
	current, err := loadSupplyCleanup(ctx, tx, parent.Parent.OperationID)
	if err != nil {
		return err
	}
	if terminal(current.Status) || current.Hash != parent.Parent.Hash || !reflect.DeepEqual(current.Input, parent.Parent.Input) {
		return loom.Fail(loom.Conflict, "supply cleanup admission changed")
	}
	return insertResourceLink(ctx, tx, current.OperationID, parent.Role, current.Input.Source.Input.Resource.Instance, child, kind, candidate, hash)
}

func (service *Service) AdmitSupplyCleanup(ctx context.Context, principal Principal, key string, request SupplyCleanupRequest) (Operation, error) {
	var empty Operation
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || !loom.ValidResourceID(request.OperationID, "op-") {
		return empty, loom.Fail(loom.Invalid, "supply cleanup requires management authority, original operation and key")
	}
	requestGuard := service.vmLock("node-request:" + string(encode([]string{service.store.identity.Identity, key})))
	requestGuard.Lock()
	defer requestGuard.Unlock()
	frozen := encode(struct {
		Action  string
		Request SupplyCleanupRequest
	}{"supply_cleanup", request})
	hash := digest(frozen)
	if cached, found, err := cachedScopedOperation(ctx, service.store.db, "node", service.store.identity.Identity, key, hash); found || err != nil {
		if found && err == nil && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	original, err := loadSupply(ctx, service.store.db, request.OperationID)
	if err != nil {
		return empty, err
	}
	guard := service.vmLock("template-work:" + original.Input.Template.ID)
	if !guard.TryLock() {
		return empty, loom.Fail(loom.Conflict, "supply workflow is active")
	}
	defer guard.Unlock()
	service.admission.Lock()
	defer service.admission.Unlock()
	if err = service.checkAdmissionOpen(); err != nil {
		return empty, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	original, err = loadSupply(ctx, tx, request.OperationID)
	if err != nil {
		return empty, err
	}
	if original.Status != "failed" && (original.Status != "succeeded" || original.Input.Command.Action != "refill") {
		return empty, loom.Fail(loom.Conflict, "supply cleanup requires fixed failed intent or unclaimed warm publication")
	}
	role := "clone"
	if original.Input.Command.Action == "template_build" {
		role = "cold"
	}
	resourceLink, _, err := loadResourceLink(ctx, tx, original.OperationID, role)
	if err != nil {
		return empty, err
	}
	source, err := loadStep(ctx, tx, resourceLink.ChildID)
	if err != nil {
		return empty, err
	}
	if !terminal(source.Status) {
		return empty, loom.Fail(loom.Conflict, "supply resource outcome remains unresolved")
	}
	if err = allocationCleanupFence(ctx, tx, source.Candidate, nil); err != nil {
		return empty, err
	}
	if source.Input.SandboxID != original.Input.SandboxID || source.Input.TenantID != original.Input.Template.TenantID {
		return empty, loom.Fail(loom.ManualRecovery, "supply source ownership changed")
	}
	sandbox, err := loadSandbox(ctx, tx, source.Input.SandboxID, source.Input.TenantID)
	if err != nil {
		return empty, err
	}
	var settings int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sandbox_settings WHERE sandbox_id=?", sandbox.ID).Scan(&settings); err != nil {
		return empty, err
	}
	if settings != 0 || sandbox.SlotAssignment != nil || sandbox.SourceOperation != source.OperationID || sandbox.VMID != source.Candidate {
		return empty, loom.Fail(loom.Conflict, "raw cleanup cannot bypass sandbox ownership")
	}
	input := supplyCleanupInput{RequestTenantID: principal.TenantID, Request: request, Original: original, Source: source}
	if released, status, lookup := loadResourceLink(ctx, tx, original.OperationID, "release"); lookup == nil && status == "succeeded" {
		receipt, err := loadStep(ctx, tx, released.ChildID)
		if err != nil {
			return empty, err
		}
		if receipt.Status != "succeeded" || receipt.State.Phase != "complete" || receipt.Input.SourceOperation != source.OperationID || receipt.Candidate != source.Candidate || sandbox.State != "terminated" {
			return empty, loom.Fail(loom.ManualRecovery, "supply original retirement receipt changed")
		}
		input.Retired = &generationRef{OperationID: receipt.OperationID, InputHash: receipt.Hash}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return empty, lookup
	}
	if input.Retired == nil {
		if sandbox.State != "provisioning" {
			return empty, loom.Fail(loom.Conflict, "supply current allocation changed")
		}
		vm, instance, owner, err := service.loadVM(ctx, tx, source)
		if err != nil {
			return empty, err
		}
		if vm.Deleting || (source.Status == "succeeded" && (owner != "" || instance != source.State.Witness || !vm.Verified || !vm.AgentReady)) || (source.Status == "failed" && owner != source.OperationID) {
			return empty, loom.Fail(loom.Conflict, "supply current resource ownership changed")
		}
		if source.Status == "succeeded" {
			refs, err := failedCaptureReferences(ctx, tx, source)
			if err != nil {
				return empty, err
			}
			input.CaptureFailures = refs
		}
	}
	if original.Input.Command.Action == "refill" {
		pool, status, err := loadPoolReservation(ctx, tx, original.OperationID)
		if err != nil {
			return empty, err
		}
		if pool.Source.OperationID != source.OperationID || pool.Source.InputHash != source.Hash || (status != "ready" && status != "cleanup") {
			return empty, loom.Fail(loom.Conflict, "supply pool responsibility was selected or changed")
		}
		if status == "ready" {
			if _, _, err = service.verifyReadyWarm(ctx, tx, original.OperationID); err != nil {
				return empty, err
			}
			if err = requireWrite(tx.ExecContext(ctx, "DELETE FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=?", pool.VMID, pool.Template.ID, pool.Template.SnapshotID)); err != nil {
				return empty, err
			}
			if err = requireWrite(tx.ExecContext(ctx, "DELETE FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='warm' AND holder_id=?", pool.VMID, original.OperationID)); err != nil {
				return empty, err
			}
			if err = requireWrite(tx.ExecContext(ctx, "UPDATE pool_slots SET status='cleanup' WHERE operation_id=? AND status='ready' AND body=?", original.OperationID, encode(pool))); err != nil {
				return empty, err
			}
		}
		input.Pool = &pool
	}
	operation := Operation{ID: newID("op-"), Key: key, Status: "pending", Result: operationResult{SandboxID: sandbox.ID, VMID: source.Candidate}}
	payload := encode(input)
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{operation.ID, "node", service.store.identity.Identity, key, hash, frozen, "pending", encode(operation.Result)}},
		{"INSERT INTO operation_steps VALUES(?,'supply_cleanup','supply_cleanup',?,?,?,?,'pending',?)", []any{operation.ID, digest(payload), payload, source.Candidate, source.Input.Resource.Instance, encode("admitted")}},
		{"INSERT INTO cleanup_intents VALUES(?,?,'supply_cleanup',?,'pending',?)", []any{"supply_cleanup:" + operation.ID, operation.ID, source.Candidate, payload}},
		{"INSERT INTO resource_refs VALUES('image',?,'step',?)", []any{source.Input.Resource.Config.Image, operation.ID + ":supply_cleanup"}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if err = service.fault("supply_cleanup_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if err = service.fault("supply_cleanup_admitted"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}
