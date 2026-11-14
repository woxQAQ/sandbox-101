package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
	"github.com/woxQAQ/haven/loom"
)

type (
	directRetirementInput struct {
		Source         resourceStep        `json:"source"`
		ActivationHash string              `json:"activation_hash"`
		ActivationKey  string              `json:"activation_key"`
		Policy         *loom.NetworkPolicy `json:"policy"`
	}
	directRetirementState struct {
		Phase     string `json:"phase"`
		RetiredBy string `json:"retired_by,omitempty"`
	}
	directRetirementStep struct {
		OperationID, Hash, Status string
		Input                     directRetirementInput
		State                     directRetirementState
	}
)

func loadDirectRetirement(ctx context.Context, q queryer, id string) (directRetirementStep, error) {
	step := directRetirementStep{
		OperationID: id,
	}
	var input, body []byte
	var candidate, expected, action string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,candidate_resource_id,expected_instance,action FROM operation_steps WHERE operation_id=? AND step_key='direct_retirement'", id).Scan(&step.Hash, &input, &step.Status, &body, &candidate, &expected, &action)
	if err != nil {
		return step, err
	}
	if digest(input) != step.Hash {
		return step, fmt.Errorf("direct retirement intent changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "direct_retirement" || candidate != step.Input.Source.Candidate || expected != step.Input.Source.State.Witness || step.Input.Policy == nil {
		return step, fmt.Errorf("direct retirement source identity changed")
	}
	return step, nil
}

func armDirectRetirement(ctx context.Context, tx *sql.Tx, activation activationStep, source resourceStep, policy *loom.NetworkPolicy) error {
	if policy == nil {
		return nil
	}
	input := directRetirementInput{
		Source:         source,
		ActivationHash: activation.Hash,
		ActivationKey:  activation.Key,
		Policy:         policy,
	}
	payload := encode(input)
	if existing, err := loadDirectRetirement(ctx, tx, activation.OperationID); err == nil {
		if existing.Hash != digest(payload) || existing.State.Phase != "armed" || terminal(existing.Status) {
			return fmt.Errorf("direct retirement original arm changed")
		}
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := requireWrite(tx.ExecContext(ctx, "INSERT INTO operation_steps VALUES(?,'direct_retirement','direct_retirement',?,?,?,?,'pending',?)", activation.OperationID, digest(payload), payload, source.Candidate, source.State.Witness, encode(directRetirementState{
		Phase: "armed",
	}))); err != nil {
		return err
	}
	return requireWrite(tx.ExecContext(ctx, "INSERT INTO cleanup_intents VALUES(?,?,'direct_policy',?,'armed',?)", "direct_policy:"+activation.OperationID, activation.OperationID, source.Candidate, payload))
}

func completeDirectReplacement(ctx context.Context, tx *sql.Tx, oldOperation, newOperation, vm, instance string) error {
	old, err := loadDirectRetirement(ctx, tx, oldOperation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if terminal(old.Status) {
		return nil
	}
	if old.Input.Source.Candidate != vm || old.Input.Source.State.Witness != instance || old.State.Phase != "armed" {
		return fmt.Errorf("direct policy replacement has unresolved prior retirement")
	}
	state := directRetirementState{
		Phase:     "replaced",
		RetiredBy: newOperation,
	}
	if err = requireWrite(tx.ExecContext(ctx, "UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='direct_retirement' AND input_hash=? AND body=? AND status='pending'", encode(state), old.OperationID, old.Hash, encode(old.State))); err != nil {
		return err
	}
	return requireWrite(tx.ExecContext(ctx, "UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind='direct_policy' AND status='armed'", "direct_policy:"+oldOperation, oldOperation))
}

// InstallDirect verifies the policy already installed by the activation step.
// The returned cleanup retains that exact step and instance across retries.
func (service *Service) InstallDirect(ctx context.Context, binding egress.Binding, policy egress.Policy) (func() error, error) {
	expected, err := directNetworkPolicy(policy)
	if err != nil {
		return nil, err
	}
	sandbox, err := loadSandbox(ctx, service.store.db, binding.SandboxID, binding.TenantID)
	if err != nil {
		return nil, err
	}
	var operation, instance string
	var body []byte
	if err = service.store.db.QueryRowContext(ctx, "SELECT operation_id,instance,body FROM network_policies WHERE vm_id=?", sandbox.VMID).Scan(&operation, &instance, &body); err != nil {
		return nil, err
	}
	step, err := loadDirectRetirement(ctx, service.store.db, operation)
	if err != nil {
		return nil, err
	}
	source := step.Input.Source
	if terminal(step.Status) || step.State.Phase != "armed" || sandbox.SourceOperation != source.OperationID || sandbox.Instance != instance || instance != source.State.Witness || binding.VSockPath != guestSocket(source.Input.Resource) || string(body) != string(encode(expected)) || !reflect.DeepEqual(expected, step.Input.Policy) {
		return nil, loom.Fail(loom.Conflict, "direct binding differs from original activation instance")
	}
	settings, err := loadSettings(ctx, service.store.db, sandbox.ID)
	if err != nil {
		return nil, err
	}
	if len(settings.Secrets) != 0 || settings.Generation != operation {
		return nil, loom.Fail(loom.Conflict, "direct binding configuration changed")
	}
	activation, err := loadActivation(ctx, service.store.db, operation)
	if err != nil {
		return nil, err
	}
	if activation.Hash != step.Input.ActivationHash || activation.Key != step.Input.ActivationKey {
		return nil, loom.Fail(loom.ManualRecovery, "direct activation witness changed")
	}
	backend, ok := service.opts.Backend.(loom.NetworkPolicyBackend)
	if !ok {
		return nil, loom.Fail(loom.Incompatible, "direct policy effects unavailable")
	}
	if err = backend.VerifyNetworkPolicy(ctx, source.Input.Resource, expected); err != nil {
		return nil, err
	}
	return func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return service.retireDirect(cleanupCtx, operation)
	}, nil
}

func (service *Service) retireDirect(ctx context.Context, operation string) error {
	step, err := loadDirectRetirement(ctx, service.store.db, operation)
	if err != nil {
		return err
	}
	if terminal(step.Status) {
		return nil
	}
	service.admission.Lock()
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err == nil {
		current, lookup := loadDirectRetirement(ctx, tx, operation)
		err = lookup
		if err == nil && !reflect.DeepEqual(current, step) {
			err = loom.Fail(loom.Conflict, "direct retirement changed")
		}
		source := step.Input.Source
		if err == nil {
			err = service.verifyGeneration(ctx, tx, source)
		}
		if err == nil {
			vm, instance, owner, lookup := service.loadVM(ctx, tx, source)
			err = lookup
			if err == nil && (instance != source.State.Witness || owner != "" || !vm.Verified) {
				err = loom.Fail(loom.Conflict, "direct retirement current instance is unverified or busy")
			}
		}
		if err == nil {
			var holder, instance string
			var body []byte
			err = tx.QueryRowContext(ctx, "SELECT operation_id,instance,body FROM network_policies WHERE vm_id=?", source.Candidate).Scan(&holder, &instance, &body)
			if err == nil && (holder != operation || instance != source.State.Witness || string(body) != string(encode(step.Input.Policy))) {
				err = loom.Fail(loom.Conflict, "direct retirement policy ownership changed")
			}
		}
		if err == nil && step.State.Phase == "armed" {
			next := directRetirementState{
				Phase: "pending",
			}
			err = requireWrite(tx.ExecContext(ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='direct_retirement' AND input_hash=? AND body=? AND status='pending'", encode(next), operation, step.Hash, encode(step.State)))
			if err == nil {
				err = requireWrite(tx.ExecContext(ctx, "UPDATE cleanup_intents SET status='pending' WHERE id=? AND operation_id=? AND resource_kind='direct_policy' AND status='armed'", "direct_policy:"+operation, operation))
			}
			if err == nil {
				step.State = next
				step.Status = "running"
			}
		}
		if err == nil {
			err = tx.Commit()
		} else {
			err = errors.Join(err, tx.Rollback())
		}
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	resource := step.Input.Source.Input.Resource
	if err = service.opts.Backend.Network(ctx, resource, false); err != nil {
		return err
	}
	backend, ok := service.opts.Backend.(loom.NetworkPolicyBackend)
	if !ok {
		return loom.Fail(loom.Incompatible, "direct policy effects unavailable")
	}
	if err = backend.InstallNetworkPolicy(ctx, resource, nil); err != nil {
		return err
	}
	if err = backend.VerifyNetworkPolicy(ctx, resource, nil); err != nil {
		return err
	}
	observation, err := service.opts.Backend.Inspect(ctx, resource)
	if err != nil {
		return err
	}
	if observation.Network {
		return loom.Fail(loom.Unknown, "direct retirement network-off unconfirmed")
	}
	if err = service.fault("direct_retirement_effect"); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err = service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadDirectRetirement(ctx, tx, operation)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("direct retirement completion changed")
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM network_policies WHERE vm_id=? AND instance=? AND operation_id=? AND body=?", []any{
			resource.VMID,
			resource.Instance,
			operation,
			encode(step.Input.Policy),
		}},
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind='direct_policy' AND status='pending'", []any{
			"direct_policy:" + operation,
			operation,
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='direct_retirement' AND input_hash=? AND body=? AND status='running'", []any{
			encode(directRetirementState{
				Phase: "complete",
			}),
			operation,
			step.Hash,
			encode(step.State),
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}
