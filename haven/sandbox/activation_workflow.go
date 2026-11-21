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

func (a *Activations) actualActivation(source machine.ResourceStep) error {
	observation, err := a.backend.Inspect(a.ctx, source.Input.Resource)
	if err != nil {
		return err
	}
	if !observation.Alive || observation.Paused || !observation.Booted {
		return fmt.Errorf("%w: activation runtime readiness unconfirmed", errs.ErrUnknown)
	}
	return nil
}

func (a *Activations) Continue(acceptedOperation string) (runErr error) {
	if a.store.Reviewed {
		return fmt.Errorf("%w: reviewed rollback cannot activate sandbox", errs.ErrManualRecovery)
	}
	step, err := LoadActivation(a.ctx, a.store.Database, acceptedOperation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) {
		return nil
	}
	source, err := machine.LoadResourceStep(a.ctx, a.store.Database, step.Input.ResourceOperation)
	if err != nil {
		return err
	}
	if source.Hash != step.Input.ResourceHash || source.State.Witness != step.Input.ExpectedInstance {
		return fmt.Errorf("activation lacks original resource witness")
	}
	defer func() {
		if runErr != nil && !errors.Is(runErr, errs.ErrInterrupted) && a.ctx.Err() == nil {
			runErr = errors.Join(runErr, a.blockActivation(step, source))
		}
	}()
	source, err = a.checkActivation(step)
	if err != nil {
		return err
	}
	if a.guest == nil {
		return fmt.Errorf("%w: guest activation effects unavailable", errs.ErrIncompatible)
	}
	if _, err = a.activationSecrets(step); err != nil {
		return err
	}
	resource := source.Input.Resource
	if err = a.actualActivation(source); err != nil {
		return err
	}
	if err = a.backend.SetNetworkEnabled(a.ctx, resource, false); err != nil {
		return err
	}
	if step.Input.PreviousSettings != nil && step.Phase == "admitted" {
		if err = a.saveConfigurationIntent(&step); err != nil {
			return err
		}
	}
	if step.Phase == "admitted" || step.Phase == "configuration_saved" {
		if err = a.markActivation(&step, "off_intent"); err != nil {
			return err
		}
	}
	if step.Phase == "off_intent" {
		if err = a.backend.SetNetworkEnabled(a.ctx, resource, false); err != nil {
			return err
		}
		observation, err := a.backend.Inspect(a.ctx, resource)
		if err != nil {
			return err
		}
		if observation.Network || !observation.Alive {
			return fmt.Errorf("%w: activation network-off unconfirmed", errs.ErrUnknown)
		}
		if err = a.markActivation(&step, "off"); err != nil {
			return err
		}
	}
	if step.Phase == "off" {
		if err = a.markActivation(&step, "policy_intent"); err != nil {
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
		if backend, ok := a.backend.(loom.NetworkPolicyBackend); ok {
			if err = backend.InstallNetworkPolicy(a.ctx, resource, policy); err != nil {
				return err
			}
			if err = backend.VerifyNetworkPolicy(a.ctx, resource, policy); err != nil {
				return err
			}
		} else if policy != nil {
			return fmt.Errorf("%w: backend lacks direct policy effects", errs.ErrIncompatible)
		}
		if err = a.commitActivationPolicy(step, source, policy); err != nil {
			return err
		}
		if err = a.markActivation(&step, "policy"); err != nil {
			return err
		}
	}
	if err = a.bindActivation(step, source); err != nil {
		return err
	}
	// Admin activation is replayable only under the exact frozen configuration.
	// A newer admission replaces the fence while this RPC may be blocked.
	if step.Phase == "policy" || step.Phase == "activated" || step.Phase == "workload_ready" || step.Phase == "network_on" {
		if err = a.guest.Info(a.ctx, resource); err != nil {
			return err
		}
		if _, err = a.checkActivation(step); err != nil {
			return err
		}
		activation := guest.Activation{
			SandboxID:     step.Input.SandboxID,
			Hostname:      strings.ReplaceAll(step.Input.SandboxID, "_", "-"),
			ResetIdentity: step.Input.ResetIdentity,
			Env:           step.Input.Settings.Env,
			Network:       step.Input.Settings.Policy.Mode,
			CAPEM:         a.caPEM,
		}
		if err = a.guest.Activate(a.ctx, resource, activation); err != nil {
			return err
		}
		if err = a.fault("activation_admin_effect"); err != nil {
			return err
		}
		if step.Phase == "policy" {
			if err = a.markActivation(&step, "activated"); err != nil {
				return err
			}
		}
		if err = a.guest.Info(a.ctx, resource); err != nil {
			return err
		}
		if step.Phase == "activated" {
			if err = a.markActivation(&step, "workload_ready"); err != nil {
				return err
			}
		}
	}
	guard := a.guards.ForSandboxConfiguration(step.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	if _, err = a.checkActivation(step); err != nil {
		return err
	}
	if err = a.actualActivation(source); err != nil {
		return err
	}
	if step.Input.Settings.Policy.Mode == "direct" {
		policy, err := machine.DirectNetworkPolicy(step.Input.Settings.Policy)
		if err != nil {
			return err
		}
		backend, ok := a.backend.(loom.NetworkPolicyBackend)
		if !ok {
			return fmt.Errorf("%w: backend lacks policy verification", errs.ErrIncompatible)
		}
		if err = backend.VerifyNetworkPolicy(a.ctx, resource, policy); err != nil {
			return err
		}
		if err = a.backend.SetNetworkEnabled(a.ctx, resource, true); err != nil {
			return err
		}
		observation, err := a.backend.Inspect(a.ctx, resource)
		if err != nil {
			return err
		}
		if !observation.Alive || !observation.Network {
			return fmt.Errorf("%w: direct network activation unconfirmed", errs.ErrUnknown)
		}
	}
	if step.Phase == "workload_ready" {
		if err = a.markActivation(&step, "network_on"); err != nil {
			return err
		}
	}
	return a.completeActivation(step)
}
