package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
	"github.com/woxQAQ/haven/loom"
)

type activationStep struct {
	OperationID, Key, Hash, Status, Phase string
	Input                                 activationInput
}

func activationKey(attempt string) string {
	if attempt == "" {
		return "activation"
	}
	return "activation:" + attempt
}

func loadActivation(ctx context.Context, q queryer, id string) (activationStep, error) {
	step := activationStep{
		OperationID: id,
	}
	var payload, body []byte
	var candidate, expected, action string
	err := q.QueryRowContext(ctx, "SELECT step_key,input_hash,input,status,body,candidate_resource_id,expected_instance,action FROM operation_steps WHERE operation_id=? AND action='activate' ORDER BY rowid DESC LIMIT 1", id).Scan(&step.Key, &step.Hash, &payload, &step.Status, &body, &candidate, &expected, &action)
	if err != nil {
		return step, err
	}
	if digest(payload) != step.Hash {
		return step, fmt.Errorf("activation input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if step.Key != activationKey(step.Input.Attempt) {
		return step, fmt.Errorf("activation attempt key changed")
	}
	if action != "activate" || expected != step.Input.ExpectedInstance || candidate == "" {
		return step, fmt.Errorf("activation frozen identity changed")
	}
	return step, nil
}

func (service *Service) activationProof(ctx context.Context, tx *sql.Tx, step activationStep) (resourceStep, sandboxRecord, error) {
	var sandbox sandboxRecord
	source, err := loadStep(ctx, tx, step.Input.ResourceOperation)
	if err != nil {
		return source, sandbox, err
	}
	if source.Hash != step.Input.ResourceHash || source.Input.TenantID != step.Input.TenantID || source.Input.SandboxID != step.Input.SandboxID || source.Status != "succeeded" || source.State.Witness != step.Input.ExpectedInstance {
		return source, sandbox, fmt.Errorf("activation original resource witness changed")
	}
	var retiring int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind='direct_policy' AND resource_id=? AND status='pending'", source.Candidate).Scan(&retiring); err != nil {
		return source, sandbox, err
	}
	if retiring != 0 {
		return source, sandbox, ErrInterrupted
	}
	if err = service.verifyGeneration(ctx, tx, source); err != nil {
		return source, sandbox, err
	}
	sandbox, err = loadSandbox(ctx, tx, step.Input.SandboxID, step.Input.TenantID)
	if err != nil {
		return source, sandbox, err
	}
	if sandbox.SourceOperation != source.OperationID || sandbox.VMID != source.Candidate || sandbox.Instance != source.State.Witness || (sandbox.State != "provisioning" && sandbox.State != "running") {
		return source, sandbox, fmt.Errorf("activation sandbox ownership changed")
	}
	vm, current, owner, err := service.loadVM(ctx, tx, source)
	if err != nil {
		return source, sandbox, err
	}
	if owner != "" || current != source.State.Witness || vm.Deleting || !vm.Verified || !vm.AgentReady {
		return source, sandbox, fmt.Errorf("activation current VM is not ready")
	}
	var fence string
	if err = tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", sandbox.ID).Scan(&fence); err != nil {
		return source, sandbox, err
	}
	if fence != step.OperationID {
		return source, sandbox, loom.Fail(loom.Conflict, "activation configuration was superseded")
	}
	settings, err := loadSettings(ctx, tx, sandbox.ID)
	if err != nil {
		return source, sandbox, err
	}
	expectedSettings := step.Input.Settings
	if step.Input.PreviousSettings != nil && step.Phase == "admitted" {
		expectedSettings = *step.Input.PreviousSettings
	}
	if !reflect.DeepEqual(settings, expectedSettings) || step.Input.Settings.Generation != step.OperationID {
		return source, sandbox, fmt.Errorf("activation desired settings changed")
	}
	var payload []byte
	var status, tenant, scope, hash string
	if err = tx.QueryRowContext(ctx, "SELECT status,scope_kind,scope_id,request_hash,request FROM operations WHERE id=?", step.OperationID).Scan(&status, &scope, &tenant, &hash, &payload); err != nil {
		return source, sandbox, err
	}
	if terminal(status) || scope != "tenant" || tenant != step.Input.RequestTenantID || digest(payload) != hash {
		return source, sandbox, fmt.Errorf("activation parent identity changed")
	}
	var candidate, expected string
	if err = tx.QueryRowContext(ctx, "SELECT candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key=? AND input_hash=?", step.OperationID, step.Key, step.Hash).Scan(&candidate, &expected); err != nil {
		return source, sandbox, err
	}
	if candidate != source.Candidate || expected != source.State.Witness {
		return source, sandbox, fmt.Errorf("activation candidate differs from resource")
	}
	return source, sandbox, nil
}

func (service *Service) checkActivation(step activationStep) (resourceStep, error) {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return resourceStep{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return resourceStep{}, err
	}
	if !reflect.DeepEqual(current, step) {
		return resourceStep{}, fmt.Errorf("activation current intent changed")
	}
	source, _, err := service.activationProof(service.ctx, tx, step)
	return source, err
}

func (service *Service) markActivation(step *activationStep, phase string) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, err = service.activationProof(service.ctx, tx, *step); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running') AND body=?", encode(phase), step.OperationID, step.Key, step.Hash, encode(step.Phase))); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.Phase = phase
	step.Status = "running"
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("activation_" + phase)
}

func (service *Service) actualActivation(source resourceStep) error {
	observation, err := service.opts.Backend.Inspect(service.ctx, source.Input.Resource)
	if err != nil {
		return err
	}
	if !observation.Alive || observation.Paused || !observation.Booted {
		return loom.Fail(loom.Unknown, "activation runtime readiness unconfirmed")
	}
	return nil
}

func (service *Service) activationSecrets(step activationStep) (map[string]egress.Secret, error) {
	service.mu.Lock()
	secrets := service.opSecrets[step.OperationID]
	service.mu.Unlock()
	for name, descriptor := range step.Input.Settings.Secrets {
		secret, ok := secrets[name]
		if !ok || secret.Value == "" || secret.Header != descriptor.Header || secret.Placeholder != descriptor.Placeholder || !reflect.DeepEqual(secret.Hosts, descriptor.Hosts) {
			return nil, loom.Fail(loom.Conflict, "host credentials must be supplied again after restart")
		}
	}
	return secrets, nil
}

func (service *Service) bindActivation(step activationStep, source resourceStep) error {
	guard := service.vmLock("configuration:" + step.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	if _, err := service.checkActivation(step); err != nil {
		return err
	}
	secrets, err := service.activationSecrets(step)
	if err != nil {
		return err
	}
	if step.Input.Settings.Policy.Mode == "none" {
		return service.revokeSandbox(step.Input.SandboxID)
	}
	if service.opts.Egress == nil {
		return loom.Fail(loom.Incompatible, "egress effects required for this network mode")
	}
	// Bind fingerprints stay in memory and use keyed comparison of complete secret
	// values. No raw or unkeyed credential digest enters the unified metadata.
	fingerprint := service.store.sign("binding-v1", step.OperationID, guestSocket(source.Input.Resource))
	service.mu.Lock()
	bound := service.bindings[step.Input.SandboxID]
	service.mu.Unlock()
	if bound == fingerprint {
		return nil
	}
	if err = service.revokeSandbox(step.Input.SandboxID); err != nil {
		return err
	}
	if err = service.opts.Egress.Bind(service.ctx, step.Input.SandboxID, step.Input.TenantID, guestSocket(source.Input.Resource), step.Input.Settings.Policy, secrets); err != nil {
		return err
	}
	service.mu.Lock()
	service.bindings[step.Input.SandboxID] = fingerprint
	service.secrets[step.Input.SandboxID] = secrets
	service.mu.Unlock()
	return nil
}

func (service *Service) revokeSandbox(id string) error {
	if service.opts.Egress != nil {
		if err := service.opts.Egress.Revoke(id); err != nil {
			return err
		}
	}
	service.mu.Lock()
	delete(service.bindings, id)
	service.mu.Unlock()
	return nil
}

func (service *Service) continueActivation(operation string) (runErr error) {
	step, err := loadActivation(service.ctx, service.store.db, operation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if terminal(step.Status) {
		return nil
	}
	source, err := loadStep(service.ctx, service.store.db, step.Input.ResourceOperation)
	if err != nil {
		return err
	}
	if source.Hash != step.Input.ResourceHash || source.State.Witness != step.Input.ExpectedInstance {
		return fmt.Errorf("activation lacks original resource witness")
	}
	defer func() {
		if runErr != nil && !errors.Is(runErr, ErrInterrupted) && service.ctx.Err() == nil {
			runErr = errors.Join(runErr, service.blockActivation(step, source))
		}
	}()
	source, err = service.checkActivation(step)
	if err != nil {
		return err
	}
	if service.opts.Guest == nil {
		return loom.Fail(loom.Incompatible, "guest activation effects unavailable")
	}
	if _, err = service.activationSecrets(step); err != nil {
		return err
	}
	resource := source.Input.Resource
	if err = service.actualActivation(source); err != nil {
		return err
	}
	if err = service.opts.Backend.Network(service.ctx, resource, false); err != nil {
		return err
	}
	if step.Input.PreviousSettings != nil && step.Phase == "admitted" {
		if err = service.saveConfigurationIntent(&step); err != nil {
			return err
		}
	}
	if step.Phase == "admitted" || step.Phase == "configuration_saved" {
		if err = service.markActivation(&step, "off_intent"); err != nil {
			return err
		}
	}
	if step.Phase == "off_intent" {
		if err = service.opts.Backend.Network(service.ctx, resource, false); err != nil {
			return err
		}
		observation, err := service.opts.Backend.Inspect(service.ctx, resource)
		if err != nil {
			return err
		}
		if observation.Network || !observation.Alive {
			return loom.Fail(loom.Unknown, "activation network-off unconfirmed")
		}
		if err = service.markActivation(&step, "off"); err != nil {
			return err
		}
	}
	if step.Phase == "off" {
		if err = service.markActivation(&step, "policy_intent"); err != nil {
			return err
		}
	}
	if step.Phase == "policy_intent" {
		var policy *loom.NetworkPolicy
		if step.Input.Settings.Policy.Mode == "direct" {
			policy, err = directNetworkPolicy(step.Input.Settings.Policy)
			if err != nil {
				return err
			}
		}
		if backend, ok := service.opts.Backend.(loom.NetworkPolicyBackend); ok {
			if err = backend.InstallNetworkPolicy(service.ctx, resource, policy); err != nil {
				return err
			}
			if err = backend.VerifyNetworkPolicy(service.ctx, resource, policy); err != nil {
				return err
			}
		} else if policy != nil {
			return loom.Fail(loom.Incompatible, "backend lacks direct policy effects")
		}
		if err = service.commitActivationPolicy(step, source, policy); err != nil {
			return err
		}
		if err = service.markActivation(&step, "policy"); err != nil {
			return err
		}
	}
	if err = service.bindActivation(step, source); err != nil {
		return err
	}
	// Admin activation is replayable only under the exact frozen configuration.
	// A newer admission replaces the fence while this RPC may be blocked.
	if step.Phase == "policy" || step.Phase == "activated" || step.Phase == "workload_ready" || step.Phase == "network_on" {
		if err = service.opts.Guest.Info(service.ctx, resource); err != nil {
			return err
		}
		if _, err = service.checkActivation(step); err != nil {
			return err
		}
		activation := Activation{
			SandboxID:     step.Input.SandboxID,
			Hostname:      strings.ReplaceAll(step.Input.SandboxID, "_", "-"),
			ResetIdentity: step.Input.ResetIdentity,
			Env:           step.Input.Settings.Env,
			Network:       step.Input.Settings.Policy.Mode,
			CAPEM:         service.opts.CAPEM,
		}
		if err = service.opts.Guest.Activate(service.ctx, resource, activation); err != nil {
			return err
		}
		if err = service.fault("activation_admin_effect"); err != nil {
			return err
		}
		if step.Phase == "policy" {
			if err = service.markActivation(&step, "activated"); err != nil {
				return err
			}
		}
		if err = service.opts.Guest.Info(service.ctx, resource); err != nil {
			return err
		}
		if step.Phase == "activated" {
			if err = service.markActivation(&step, "workload_ready"); err != nil {
				return err
			}
		}
	}
	guard := service.vmLock("configuration:" + step.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	if _, err = service.checkActivation(step); err != nil {
		return err
	}
	if err = service.actualActivation(source); err != nil {
		return err
	}
	if step.Input.Settings.Policy.Mode == "direct" {
		policy, err := directNetworkPolicy(step.Input.Settings.Policy)
		if err != nil {
			return err
		}
		backend, ok := service.opts.Backend.(loom.NetworkPolicyBackend)
		if !ok {
			return loom.Fail(loom.Incompatible, "backend lacks policy verification")
		}
		if err = backend.VerifyNetworkPolicy(service.ctx, resource, policy); err != nil {
			return err
		}
		if err = service.opts.Backend.Network(service.ctx, resource, true); err != nil {
			return err
		}
		observation, err := service.opts.Backend.Inspect(service.ctx, resource)
		if err != nil {
			return err
		}
		if !observation.Alive || !observation.Network {
			return loom.Fail(loom.Unknown, "direct network activation unconfirmed")
		}
	}
	if step.Phase == "workload_ready" {
		if err = service.markActivation(&step, "network_on"); err != nil {
			return err
		}
	}
	return service.completeActivation(step)
}

func (service *Service) commitActivationPolicy(step activationStep, source resourceStep, policy *loom.NetworkPolicy) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, err = service.activationProof(service.ctx, tx, step); err != nil {
		return err
	}
	var previous, instance string
	var priorPolicy []byte
	lookup := tx.QueryRowContext(service.ctx, "SELECT operation_id,instance,body FROM network_policies WHERE vm_id=?", source.Candidate).Scan(&previous, &instance, &priorPolicy)
	if lookup == nil && previous != step.OperationID {
		if instance != source.State.Witness && string(priorPolicy) != "null" {
			old, err := loadDirectRetirement(service.ctx, tx, previous)
			if err != nil {
				return err
			}
			known := false
			for _, ancestor := range source.Input.Ancestors {
				if ancestor.OperationID == old.Input.Source.OperationID && ancestor.InputHash == old.Input.Source.Hash && old.Input.Source.State.Witness == instance && old.Input.Source.Input.Resource.NetworkToken == source.Input.Resource.NetworkToken {
					known = true
					break
				}
			}
			if !known {
				return fmt.Errorf("network policy replacement lacks original stopped generation")
			}
		}
		if err = completeDirectReplacement(service.ctx, tx, previous, step.OperationID, source.Candidate, instance); err != nil {
			return err
		}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	if err = armDirectRetirement(service.ctx, tx, step, source, policy); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO network_policies VALUES(?,?,?,?) ON CONFLICT(vm_id) DO UPDATE SET instance=excluded.instance,operation_id=excluded.operation_id,body=excluded.body", source.Candidate, source.State.Witness, step.OperationID, encode(policy))); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) completeActivation(step activationStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.Phase != "network_on" {
		return fmt.Errorf("sandbox completion lacks original activation proof")
	}
	source, sandbox, err := service.activationProof(service.ctx, tx, step)
	if err != nil {
		return err
	}
	settings := step.Input.Settings
	settings.Instance = source.State.Witness
	settings.ActiveEpoch = service.epoch
	if settings.Deadline.IsZero() {
		settings.Deadline = time.Now().UTC().Add(time.Duration(settings.TTLSeconds) * time.Second)
	}
	sandbox.State = "running"
	sandbox.LastError = ""
	sandbox.OperationID = ""
	if err = service.completeWarmClaim(tx, step); err != nil {
		return err
	}
	if err = service.completeLifecycleActivation(tx, step); err != nil {
		return err
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"UPDATE sandboxes SET state='running',body=? WHERE id=? AND tenant=? AND state IN ('provisioning','running')", []any{
			encode(sandbox),
			sandbox.ID,
			sandbox.TenantID,
		}},
		{"UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", []any{
			encode(settings),
			sandbox.ID,
			encode(step.Input.Settings),
		}},
		{"DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?", []any{
			sandbox.ID,
			step.OperationID,
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND body=? AND status='running'", []any{
			encode("complete"),
			step.OperationID,
			step.Key,
			step.Hash,
			encode(step.Phase),
		}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{
			step.OperationID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("activation_completion_before_commit"); err != nil {
		return err
	}
	if err = commitPublications(tx); err != nil {
		return err
	}
	service.mu.Lock()
	delete(service.opSecrets, step.OperationID)
	service.mu.Unlock()
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("activation_completed")
}
