package haven

import (
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) runClone(step resourceStep) error {
	if step.Input.Restore == nil || step.State.RestoreBlocked {
		return fmt.Errorf("%w: restore source missing or candidate blocked", ErrManualRecovery)
	}
	service.admission.Lock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err == nil {
		_, _, _, err = service.checkProvisionIntent(tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	r := step.Input.Resource
	source := step.Input.Restore
	if err = verifyImageFiles(step.Input.Image, r.ImageDir); err != nil {
		return err
	}
	if !step.State.BackingReady {
		observation, err := service.opts.Backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if observation.Alive {
			return service.failStep(step, fmt.Errorf("%w: unproved restore candidate already in use", ErrManualRecovery))
		}
		if step.State.Materials != nil && !step.State.Materials.Complete {
			return service.failStep(step, fmt.Errorf("%w: restore interrupted before material completion; original candidate retained", ErrIncompatible))
		}
		if err = verifyRestoreSourceFiles(*source); err != nil {
			return service.failStep(step, err)
		}
		if step.State.Materials == nil {
			if step.State.Phase == "admitted" {
				next := step.State
				next.Phase = "restore_prepare_intent"
				if err = service.saveRestore(&step, next); err != nil {
					return err
				}
			}
			if step.State.Phase != "restore_prepare_intent" {
				return service.failStep(step, fmt.Errorf("unproved restore phase changed"))
			}
			boundary, err := loom.PrepareRestore(r, step.OperationID, source.Snapshot.Dir, source.Snapshot.Manifest.Files, step.Action == "clone")
			if err != nil {
				return service.failStep(step, err)
			}
			defer func() {
				_ = boundary.Close()
			}()
			next := step.State
			next.Materials = copyRestoreMaterials(boundary.Materials)
			next.Phase = "restore_candidate_prepared"
			if err = service.saveRestore(&step, next); err != nil {
				return err
			}
			for _, name := range loom.RestoreFileNames() {
				if err = boundary.CopyFile(name); err != nil {
					return service.failStep(step, err)
				}
				if name == "memory-ranges" {
					continue
				}
				next = step.State
				next.Phase = "restore_candidate_copied"
				if err = service.saveRestore(&step, next); err != nil {
					return err
				}
				if err = boundary.PublishFile(name); err != nil {
					return service.failStep(step, err)
				}
				next = step.State
				next.Phase = "restore_candidate_renamed"
				if err = service.saveRestore(&step, next); err != nil {
					return err
				}
			}
			if err = boundary.SyncMaterials(); err != nil {
				return service.failStep(step, err)
			}
			next = step.State
			next.Materials = copyRestoreMaterials(boundary.Materials)
			next.Phase = "restore_candidate_synced"
			if err = service.saveRestore(&step, next); err != nil {
				return err
			}
			next = step.State
			next.Materials = copyRestoreMaterials(boundary.Materials)
			next.Materials.Complete = true
			identity := next.Materials.Files["disk.raw"].Identity
			next.DiskIdentity = &identity
			next.Phase = "restore_materials_complete"
			if err = service.saveRestore(&step, next); err != nil {
				return err
			}
		}
		if err = loom.VerifyRestoreMaterials(step.State.Materials, r, source.Snapshot.Manifest.Files, true); err != nil {
			return service.failStep(step, err)
		}
		if err = service.opts.Backend.Prepare(service.ctx, r); err != nil {
			return service.failStep(step, err)
		}
		if err = loom.VerifyRestoreMaterials(step.State.Materials, r, source.Snapshot.Manifest.Files, true); err != nil {
			return service.failStep(step, err)
		}
		if err = service.transferRestore(&step); err != nil {
			return err
		}
	}
	observation, err := service.opts.Backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive && step.State.Phase == "restore_resources_ready" {
		return service.failStep(step, fmt.Errorf("%w: live restore candidate lacks durable launch intent", ErrManualRecovery))
	}
	checkDisk := !observation.Alive && (step.State.Phase == "restore_resources_ready" || step.State.Phase == "launch_intent")
	if err = loom.VerifyRestoreMaterials(step.State.Materials, r, source.Snapshot.Manifest.Files, checkDisk); err != nil {
		return service.failStep(step, err)
	}
	if err = loom.ValidateCOWTopology(r.RestoreDir, uint64(r.Config.Shape.MemoryMiB)<<20); err != nil {
		return service.failStep(step, err)
	}
	if step.State.Phase == "restore_resources_ready" {
		if err = service.markPhase(&step, "launch_intent"); err != nil {
			return err
		}
	}
	return service.startCandidate(step, true)
}
