package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

func (service *Data) verifyDataNetwork(ctx context.Context, view sandbox.View, settings sandbox.Settings, source machine.ResourceStep, observation loom.Observation) error {
	if settings.Policy.Mode != "direct" {
		if observation.Network {
			return fmt.Errorf("%w: sandbox NIC enabled outside direct mode", errs.ErrUnknown)
		}
		return nil
	}
	if !observation.Network {
		return fmt.Errorf("%w: direct sandbox NIC is not enabled", errs.ErrUnknown)
	}
	expected, err := machine.DirectNetworkPolicy(settings.Policy)
	if err != nil {
		return err
	}
	policyRecord, err := machine.LoadPolicyWitness(ctx, service.store.Database, view.VMID)
	if err != nil {
		return err
	}
	instance, operationID, body := policyRecord.Instance, policyRecord.OperationID, policyRecord.Body
	if instance != view.Instance || operationID != settings.Generation || string(body) != string(operation.Encode(expected)) {
		return fmt.Errorf("%w: direct admission lacks exact configuration policy witness", errs.ErrManualRecovery)
	}
	activation, err := sandbox.LoadActivation(ctx, service.store.Database, operationID)
	if err != nil {
		return err
	}
	if activation.Status != "succeeded" || activation.Phase != "complete" || activation.Input.ResourceHash != source.Hash || activation.Input.ExpectedInstance != view.Instance {
		return fmt.Errorf("%w: direct admission original activation witness changed", errs.ErrManualRecovery)
	}
	backend, ok := service.opts.Backend.(loom.NetworkPolicyBackend)
	if !ok {
		return fmt.Errorf("%w: direct policy effects unavailable", errs.ErrIncompatible)
	}
	return backend.VerifyNetworkPolicy(ctx, source.Input.Resource, expected)
}

// repairDataNetwork submits the saved configuration with an observed generation.
// A stale verification cannot revoke or overwrite a newer admitted policy.
func (service *Data) repairDataNetwork(view sandbox.View, settings sandbox.Settings) error {
	env := map[string]operation.EnvVar{}
	for name, value := range settings.Env {
		env[name] = operation.EnvVar{
			Value: value,
		}
	}
	credentials := service.access.SandboxSecrets(view.ID)
	for name, descriptor := range settings.Secrets {
		value, ok := credentials[name]
		if !ok || value.Value == "" {
			return fmt.Errorf("%w: host credentials require explicit resupply", errs.ErrConflict)
		}
		guest := false
		injection := descriptor
		injection.Hosts = append([]string(nil), descriptor.Hosts...)
		env[name] = operation.EnvVar{
			Value:  value.Value,
			Guest:  &guest,
			Inject: &injection,
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	accepted, err := service.sandboxes.AdmitConfigure(ctx, authorization.Principal{
		TenantID: view.TenantID,
	}, "network-revalidate:"+service.epoch+":"+view.ID+":"+settings.Generation, operation.Command{
		Action:             "configure",
		SandboxID:          view.ID,
		ObservedGeneration: settings.Generation,
		Env:                env,
		Policy:             settings.Policy,
	})
	if err != nil {
		return err
	}
	if accepted.ID == "" {
		return fmt.Errorf("network revalidation lacks accepted intent")
	}
	return nil
}

func (service *Data) rejectDataNetwork(view sandbox.View, settings sandbox.Settings, cause error) error {
	return errors.Join(cause, service.repairDataNetwork(view, settings))
}
