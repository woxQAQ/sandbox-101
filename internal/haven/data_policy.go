package haven

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) verifyDataNetwork(ctx context.Context, sandbox Sandbox, settings sandboxSettings, source resourceStep, observation loom.Observation) error {
	if settings.Policy.Mode != "direct" {
		if observation.Network {
			return loom.Fail(loom.Unknown, "sandbox NIC enabled outside direct mode")
		}
		return nil
	}
	if !observation.Network {
		return loom.Fail(loom.Unknown, "direct sandbox NIC is not enabled")
	}
	expected, err := directNetworkPolicy(settings.Policy)
	if err != nil {
		return err
	}
	var instance, operation string
	var body []byte
	if err = service.store.db.QueryRowContext(ctx, "SELECT instance,operation_id,body FROM network_policies WHERE vm_id=?", sandbox.VMID).Scan(&instance, &operation, &body); err != nil {
		return err
	}
	if instance != sandbox.Instance || operation != settings.Generation || string(body) != string(encode(expected)) {
		return loom.Fail(loom.ManualRecovery, "direct admission lacks exact configuration policy witness")
	}
	activation, err := loadActivation(ctx, service.store.db, operation)
	if err != nil {
		return err
	}
	if activation.Status != "succeeded" || activation.Phase != "complete" || activation.Input.ResourceHash != source.Hash || activation.Input.ExpectedInstance != sandbox.Instance {
		return loom.Fail(loom.ManualRecovery, "direct admission original activation witness changed")
	}
	backend, ok := service.opts.Backend.(loom.NetworkPolicyBackend)
	if !ok {
		return loom.Fail(loom.Incompatible, "direct policy effects unavailable")
	}
	return backend.VerifyNetworkPolicy(ctx, source.Input.Resource, expected)
}

// repairDataNetwork submits the saved configuration with an observed generation.
// A stale verification cannot revoke or overwrite a newer admitted policy.
func (service *Service) repairDataNetwork(sandbox Sandbox, settings sandboxSettings) error {
	env := map[string]EnvVar{}
	for name, value := range settings.Env {
		env[name] = EnvVar{Value: value}
	}
	service.mu.Lock()
	for name, descriptor := range settings.Secrets {
		value, ok := service.secrets[sandbox.ID][name]
		if !ok || value.Value == "" {
			service.mu.Unlock()
			return loom.Fail(loom.Conflict, "host credentials require explicit resupply")
		}
		guest := false
		injection := descriptor
		injection.Hosts = append([]string(nil), descriptor.Hosts...)
		env[name] = EnvVar{Value: value.Value, Guest: &guest, Inject: &injection}
	}
	service.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	operation, err := service.AdmitConfigure(ctx, Principal{TenantID: sandbox.TenantID}, "network-revalidate:"+service.epoch+":"+sandbox.ID+":"+settings.Generation, Command{Action: "configure", SandboxID: sandbox.ID, ObservedGeneration: settings.Generation, Env: env, Policy: settings.Policy})
	if err != nil {
		return err
	}
	if operation.ID == "" {
		return fmt.Errorf("network revalidation lacks accepted intent")
	}
	return nil
}

func (service *Service) rejectDataNetwork(sandbox Sandbox, settings sandboxSettings, cause error) error {
	return errors.Join(cause, service.repairDataNetwork(sandbox, settings))
}
