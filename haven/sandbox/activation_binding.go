package sandbox

import (
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
)

func (service *Activations) activationSecrets(step ActivationStep) (map[string]egress.Secret, error) {
	secrets := service.access.OperationSecrets(step.OperationID)
	for name, descriptor := range step.Input.Settings.Secrets {
		secret, ok := secrets[name]
		if !ok || secret.Value == "" || secret.Header != descriptor.Header || secret.Placeholder != descriptor.Placeholder || !reflect.DeepEqual(secret.Hosts, descriptor.Hosts) {
			return nil, fmt.Errorf("%w: host credentials must be supplied again after restart", errs.ErrConflict)
		}
	}
	return secrets, nil
}

func (service *Activations) bindActivation(step ActivationStep, source machine.ResourceStep) error {
	guard := service.guards.Configuration(step.Input.SandboxID)
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
		return service.RevokeSandbox(step.Input.SandboxID)
	}
	if service.egress == nil {
		return fmt.Errorf("%w: egress effects required for this network mode", errs.ErrIncompatible)
	}
	// Binding fingerprints remain keyed and ephemeral. Secret values stay in the
	// shared access registry and never enter the unified metadata.
	fingerprint := service.sign("binding-v1", step.OperationID, guest.Socket(source.Input.Resource))
	bound := service.access.Binding(step.Input.SandboxID)
	if bound == fingerprint {
		return nil
	}
	if err = service.RevokeSandbox(step.Input.SandboxID); err != nil {
		return err
	}
	if err = service.egress.Bind(service.ctx, egress.BindingTarget{
		SandboxID: step.Input.SandboxID,
		TenantID:  step.Input.TenantID,
		VSockPath: guest.Socket(source.Input.Resource),
	}, step.Input.Settings.Policy, secrets); err != nil {
		return err
	}
	service.access.Bind(step.Input.SandboxID, fingerprint, secrets)
	return nil
}

func (service *Activations) RevokeSandbox(id string) error {
	if service.egress != nil {
		if err := service.egress.Revoke(id); err != nil {
			return err
		}
	}
	service.access.ForgetBinding(id)
	return nil
}
