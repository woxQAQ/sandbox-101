package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

type StopRequest struct {
	TenantID         string `json:"tenant_id"`
	Key              string `json:"key"`
	SandboxID        string `json:"sandbox_id"`
	ExpectedInstance string `json:"expected_instance"`
}

type (
	stopInput struct {
		Lifecycle       string `json:"lifecycle,omitempty"`
		Request         StopRequest
		SourceOperation string
		SourceHash      string
	}
	stopStep struct {
		OperationID, Hash, Status, Phase string
		Input                            stopInput
	}
)

func loadStop(ctx context.Context, q queryer, id string) (stopStep, error) {
	step := stopStep{OperationID: id}
	var input, body []byte
	var candidate, expected, action string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,candidate_resource_id,expected_instance,action FROM operation_steps WHERE operation_id=? AND step_key='stop'", id).Scan(&step.Hash, &input, &step.Status, &body, &candidate, &expected, &action)
	if err != nil {
		return step, err
	}
	if digest(input) != step.Hash {
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

// AdmitStop is an internal resource boundary for an undelivered sandbox. Running
// Activated sessions require their separate deactivation/pause workflow.
func (service *Service) AdmitStop(ctx context.Context, request StopRequest) (Operation, error) {
	return service.admitStop(ctx, request, nil)
}

func (service *Service) admitStop(ctx context.Context, request StopRequest, parent resourceParent) (Operation, error) {
	link := lifecycleParent(parent)
	var empty Operation
	if request.TenantID == "" || request.SandboxID == "" || request.ExpectedInstance == "" || request.Key == "" || len(request.Key) > 256 {
		return empty, fmt.Errorf("stop requires scoped sandbox, instance and key")
	}
	frozen := encode(struct {
		Action  string
		Request StopRequest
	}{"stop", request})
	hash := digest(frozen)
	service.admission.Lock()
	defer service.admission.Unlock()
	service.mu.Lock()
	closing := service.closing || service.store.reviewed
	service.mu.Unlock()
	if closing {
		return empty, fmt.Errorf("haven is closing")
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	if cached, found, err := cachedOperation(ctx, tx, request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	sandbox, err := loadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	if err = allocationCleanupFence(ctx, tx, sandbox.VMID, parent); err != nil {
		return empty, err
	}
	if (sandbox.State != "provisioning" && (link == nil || (link.Input.Command.Action != "rollback" && link.Input.Command.Action != "release") || sandbox.State != "running")) || sandbox.Instance != request.ExpectedInstance {
		return empty, fmt.Errorf("stop requires undelivered current sandbox instance")
	}
	source, err := loadStep(ctx, tx, sandbox.SourceOperation)
	if err != nil {
		return empty, err
	}
	if err = service.verifyGeneration(ctx, tx, source); err != nil {
		return empty, err
	}
	if _, err = service.generationHistory(ctx, tx, source); err != nil {
		return empty, err
	}
	vm, instance, owner, err := service.loadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if owner != "" || instance != request.ExpectedInstance || !vm.Verified || !vm.AgentReady || vm.Deleting {
		return empty, fmt.Errorf("stop current ownership changed")
	}
	operation := Operation{ID: newID("op-"), Key: request.Key, Status: "pending", Result: operationResult{SandboxID: sandbox.ID, VMID: sandbox.VMID}}
	input := stopInput{Request: request, SourceOperation: source.OperationID, SourceHash: source.Hash}
	if link != nil {
		input.Lifecycle = link.Parent.ID
	}
	if err = service.admitResourceParent(ctx, tx, parent, operation, "stop", sandbox.VMID, hash); err != nil {
		return empty, err
	}
	payload := encode(input)
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{operation.ID, "tenant", request.TenantID, request.Key, hash, frozen, "pending", encode(operation.Result)}},
		{"INSERT INTO operation_steps VALUES(?,'stop','stop',?,?,?,?,'pending',?)", []any{operation.ID, digest(payload), payload, sandbox.ID, request.ExpectedInstance, encode("admitted")}},
		{"UPDATE vms SET operation_id=? WHERE id=? AND operation_id IS NULL AND current_instance=? AND state='running'", []any{operation.ID, sandbox.VMID, request.ExpectedInstance}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if err = service.fault("stop_admitted"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}

func (service *Service) stopProof(tx *sql.Tx, step stopStep) (resourceStep, vmRecord, error) {
	source, err := loadStep(service.ctx, tx, step.Input.SourceOperation)
	if err != nil {
		return source, vmRecord{}, err
	}
	if source.Hash != step.Input.SourceHash || source.Input.SandboxID != step.Input.Request.SandboxID || source.Input.TenantID != step.Input.Request.TenantID || source.State.Witness != step.Input.Request.ExpectedInstance {
		return source, vmRecord{}, fmt.Errorf("stop source history changed")
	}
	if err = service.verifyGeneration(service.ctx, tx, source); err != nil {
		return source, vmRecord{}, err
	}
	vm, current, owner, err := service.loadVM(service.ctx, tx, source)
	if err != nil {
		return source, vm, err
	}
	sandbox, err := loadSandbox(service.ctx, tx, step.Input.Request.SandboxID, step.Input.Request.TenantID)
	if err != nil {
		return source, vm, err
	}
	state := "provisioning"
	if step.Input.Lifecycle != "" {
		parent, err := loadLifecycle(service.ctx, tx, step.Input.Lifecycle)
		if err != nil {
			return source, vm, err
		}
		link, _, err := loadResourceLink(service.ctx, tx, parent.OperationID, "stop")
		if err != nil {
			return source, vm, err
		}
		var fence string
		if err = tx.QueryRowContext(service.ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", sandbox.ID).Scan(&fence); err != nil {
			return source, vm, err
		}
		if (parent.Input.Command.Action != "rollback" && parent.Input.Command.Action != "release") || terminal(parent.Status) || parent.Input.OwnerTenantID != source.Input.TenantID || parent.Input.Source.OperationID != source.OperationID || parent.Input.Source.InputHash != source.Hash || parent.Input.SourceInstance != step.Input.Request.ExpectedInstance || link.ChildID != step.OperationID || fence != parent.OperationID {
			return source, vm, fmt.Errorf("rollback stop no longer owns original instance")
		}
		state = parent.Input.Sandbox.State
	}
	if owner != step.OperationID || current != step.Input.Request.ExpectedInstance || vm.Deleting || sandbox.SourceOperation != source.OperationID || sandbox.State != state || sandbox.Instance != current {
		return source, vm, fmt.Errorf("stop current instance or sandbox ownership changed")
	}
	var request, body []byte
	var hash, scope, tenant, status string
	if err = tx.QueryRowContext(service.ctx, "SELECT request,request_hash,scope_kind,scope_id,status,body FROM operations WHERE id=?", step.OperationID).Scan(&request, &hash, &scope, &tenant, &status, &body); err != nil {
		return source, vm, err
	}
	var frozen struct {
		Action  string
		Request StopRequest
	}
	var result operationResult
	if err = json.Unmarshal(request, &frozen); err != nil {
		return source, vm, err
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return source, vm, err
	}
	if digest(request) != hash || scope != "tenant" || tenant != source.Input.TenantID || terminal(status) || frozen.Action != "stop" || frozen.Request != step.Input.Request || result.VMID != source.Candidate || result.SandboxID != source.Input.SandboxID {
		return source, vm, fmt.Errorf("stop parent identity changed")
	}
	return source, vm, nil
}

func (service *Service) runStop(step stopStep) error {
	if terminal(step.Status) {
		return nil
	}
	service.admission.Lock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	var source resourceStep
	if err == nil {
		source, _, err = service.stopProof(tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	if step.Phase != "admitted" && step.Phase != "stop_intent" {
		return fmt.Errorf("stop phase unsupported")
	}
	if step.Phase == "admitted" {
		if err = requireWrite(service.store.db.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='stop' AND input_hash=? AND status IN ('pending','running') AND body=?", encode("stop_intent"), step.OperationID, step.Hash, encode(step.Phase))); err != nil {
			return err
		}
		step.Phase = "stop_intent"
		if err = service.store.checkpoint(); err != nil {
			return err
		}
	}
	r := source.Input.Resource
	if err = service.opts.Backend.Stop(service.ctx, r); err != nil {
		return err
	}
	observation, err := service.opts.Backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive {
		return fmt.Errorf("stop exit unconfirmed")
	}
	if err = service.opts.Backend.Network(service.ctx, r, false); err != nil {
		return err
	}
	observation, err = service.opts.Backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive || observation.Network {
		return fmt.Errorf("stop network/exit unconfirmed")
	}
	if err = service.fault("stop_effect_stopped"); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err = service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadStop(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || current.Phase != step.Phase || !reflect.DeepEqual(current.Input, step.Input) {
		return fmt.Errorf("stop completion intent changed")
	}
	_, vm, err := service.stopProof(tx, current)
	if err != nil {
		return err
	}
	vm.Verified = true
	vm.AgentReady = false
	vm.Observation = nil
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"UPDATE vms SET state='stopped',operation_id=NULL,body=? WHERE id=? AND operation_id=? AND current_instance=?", []any{encode(vm), source.Candidate, step.OperationID, step.Input.Request.ExpectedInstance}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='stop' AND input_hash=? AND status='running' AND body=?", []any{encode("complete"), step.OperationID, step.Hash, encode(step.Phase)}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{step.OperationID}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("stop_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}
