package machine

import (
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
)

func (service *Provisioning) runCold(step ResourceStep) error {
	service.admission.Lock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err == nil {
		_, _, _, err = service.ownership.VerifyProvisionIntent(service.proofs, service.ctx, tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	r := step.Input.Resource
	if err := image.VerifyFiles(step.Input.Image, r.ImageDir); err != nil {
		return err
	}
	if step.State.Phase == "admitted" {
		if err := service.markPhase(&step, "prepare_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "prepare_intent" {
		before, err := service.backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if before.Alive {
			return fmt.Errorf("live candidate lacks a durable launch intent")
		}
		if err := service.backend.Prepare(service.ctx, r); err != nil {
			return service.failStep(step, err)
		}
		if err := service.fault("effect_prepared"); err != nil {
			return err
		}
		if err := service.markPhase(&step, "prepared"); err != nil {
			return err
		}
	}
	if step.State.Phase == "prepared" {
		before, err := service.backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if before.Alive {
			return fmt.Errorf("live candidate lacks a durable launch intent")
		}
		if err := service.markPhase(&step, "launch_intent"); err != nil {
			return err
		}
	}
	return service.startCandidate(step, false)
}

func (service *Provisioning) startCandidate(step ResourceStep, restore bool) error {
	r := step.Input.Resource
	if step.State.Phase != "launch_intent" && step.State.Phase != "launched" && step.State.Phase != "booted" && step.State.Phase != "agent_ready" {
		return fmt.Errorf("unknown cold resource phase")
	}
	if step.State.DiskIdentity == nil {
		return fmt.Errorf("prepared cold disk proof missing")
	}
	if err := loom.VerifyWritableDisk(r, *step.State.DiskIdentity); err != nil {
		return err
	}
	observation, err := service.backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Network {
		if err = service.backend.Network(service.ctx, r, false); err != nil {
			return err
		}
	}
	if !observation.Alive {
		if step.State.Phase != "launch_intent" {
			return service.failStep(step, fmt.Errorf("%w: previous candidate exited; refusing another launch", errs.ErrUnknown))
		}
		if err = service.backend.Network(service.ctx, r, false); err != nil {
			return service.failStep(step, err)
		}
		if err = service.backend.Launch(service.ctx, r, restore); err != nil {
			return service.failStep(step, err)
		}
		if err = service.fault("effect_launched"); err != nil {
			return err
		}
	}
	if step.State.Phase == "launch_intent" {
		if err = service.markPhase(&step, "launched"); err != nil {
			return err
		}
	}
	if step.State.Phase == "launched" {
		if err = service.backend.Boot(service.ctx, r, restore); err != nil {
			return service.failStep(step, err)
		}
		if err = service.markPhase(&step, "booted"); err != nil {
			return err
		}
	}
	if err = service.backend.Agent(service.ctx, r, restore); err != nil {
		networkErr := service.backend.Network(service.ctx, r, false)
		stopErr := service.backend.Stop(service.ctx, r)
		return service.failStep(step, errors.Join(err, networkErr, stopErr))
	}
	if step.State.Phase != "agent_ready" {
		if err = service.markPhase(&step, "agent_ready"); err != nil {
			return err
		}
	}
	observation, err = service.backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if !observation.Alive || !observation.Booted || !observation.Configured || observation.Paused || observation.Network || observation.PID <= 0 || observation.StartTime == 0 {
		return service.failStep(step, fmt.Errorf("%w: candidate is not verified System ready with network off", errs.ErrUnknown))
	}
	if restore {
		if step.Input.Restore == nil || !step.State.BackingReady {
			return fmt.Errorf("restore start lacks backing handoff")
		}
		if err := loom.VerifyRestoreMaterials(step.State.Materials, r, step.Input.Restore.Snapshot.Manifest.Files, false); err != nil {
			return service.failStep(step, err)
		}
	}
	if err = loom.VerifyWritableDisk(r, *step.State.DiskIdentity); err != nil {
		return err
	}
	return service.completeCold(step, observation)
}
