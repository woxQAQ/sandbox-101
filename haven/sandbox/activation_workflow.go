package sandbox

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func (service *Activations) actualActivation(source machine.ResourceStep) error {
	observation, err := service.backend.Inspect(service.ctx, source.Input.Resource)
	if err != nil {
		return err
	}
	if !observation.Alive || observation.Paused || !observation.Booted {
		return fmt.Errorf("%w: activation runtime readiness unconfirmed", errs.ErrUnknown)
	}
	return nil
}

func (service *Activations) Continue(acceptedOperation string) (runErr error) {
	if service.store.Reviewed {
		return fmt.Errorf("%w: reviewed rollback cannot activate sandbox", errs.ErrManualRecovery)
	}
	step, err := LoadActivation(service.ctx, service.store.Database, acceptedOperation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) {
		return nil
	}
	source, err := machine.LoadResourceStep(service.ctx, service.store.Database, step.Input.ResourceOperation)
	if err != nil {
		return err
	}
	if source.Hash != step.Input.ResourceHash || source.State.Witness != step.Input.ExpectedInstance {
		return fmt.Errorf("activation lacks original resource witness")
	}
	defer func() {
		if runErr != nil && !errors.Is(runErr, errs.ErrInterrupted) && service.ctx.Err() == nil {
			runErr = errors.Join(runErr, service.blockActivation(step, source))
		}
	}()
	source, err = service.checkActivation(step)
	if err != nil {
		return err
	}
	if service.guest == nil {
		return fmt.Errorf("%w: guest activation effects unavailable", errs.ErrIncompatible)
	}
	if _, err = service.activationSecrets(step); err != nil {
		return err
	}
	resource := source.Input.Resource
	if err = service.actualActivation(source); err != nil {
		return err
	}
	if err = service.backend.Network(service.ctx, resource, false); err != nil {
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
		if err = service.backend.Network(service.ctx, resource, false); err != nil {
			return err
		}
		observation, err := service.backend.Inspect(service.ctx, resource)
		if err != nil {
			return err
		}
		if observation.Network || !observation.Alive {
			return fmt.Errorf("%w: activation network-off unconfirmed", errs.ErrUnknown)
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
			policy, err = machine.DirectNetworkPolicy(step.Input.Settings.Policy)
			if err != nil {
				return err
			}
		}
		if backend, ok := service.backend.(loom.NetworkPolicyBackend); ok {
			if err = backend.InstallNetworkPolicy(service.ctx, resource, policy); err != nil {
				return err
			}
			if err = backend.VerifyNetworkPolicy(service.ctx, resource, policy); err != nil {
				return err
			}
		} else if policy != nil {
			return fmt.Errorf("%w: backend lacks direct policy effects", errs.ErrIncompatible)
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
		if err = service.guest.Info(service.ctx, resource); err != nil {
			return err
		}
		if _, err = service.checkActivation(step); err != nil {
			return err
		}
		activation := guest.Activation{
			SandboxID:     step.Input.SandboxID,
			Hostname:      strings.ReplaceAll(step.Input.SandboxID, "_", "-"),
			ResetIdentity: step.Input.ResetIdentity,
			Env:           step.Input.Settings.Env,
			Network:       step.Input.Settings.Policy.Mode,
			CAPEM:         service.caPEM,
		}
		if err = service.guest.Activate(service.ctx, resource, activation); err != nil {
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
		if err = service.guest.Info(service.ctx, resource); err != nil {
			return err
		}
		if step.Phase == "activated" {
			if err = service.markActivation(&step, "workload_ready"); err != nil {
				return err
			}
		}
	}
	guard := service.guards.Configuration(step.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	if _, err = service.checkActivation(step); err != nil {
		return err
	}
	if err = service.actualActivation(source); err != nil {
		return err
	}
	if step.Input.Settings.Policy.Mode == "direct" {
		policy, err := machine.DirectNetworkPolicy(step.Input.Settings.Policy)
		if err != nil {
			return err
		}
		backend, ok := service.backend.(loom.NetworkPolicyBackend)
		if !ok {
			return fmt.Errorf("%w: backend lacks policy verification", errs.ErrIncompatible)
		}
		if err = backend.VerifyNetworkPolicy(service.ctx, resource, policy); err != nil {
			return err
		}
		if err = service.backend.Network(service.ctx, resource, true); err != nil {
			return err
		}
		observation, err := service.backend.Inspect(service.ctx, resource)
		if err != nil {
			return err
		}
		if !observation.Alive || !observation.Network {
			return fmt.Errorf("%w: direct network activation unconfirmed", errs.ErrUnknown)
		}
	}
	if step.Phase == "workload_ready" {
		if err = service.markActivation(&step, "network_on"); err != nil {
			return err
		}
	}
	return service.completeActivation(step)
}
