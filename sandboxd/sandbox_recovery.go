package sandboxd

import (
	"strings"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) recoverSandboxResource(r *resourceRecovery) error {
	unlock := s.lock(r.SandboxID)
	defer unlock()
	progress, err := s.reconcileRecoveryAllocations(r)
	if err != nil {
		return err
	}
	if !progress.AllowedLifecycleWork && !progress.AllowedParentDone {
		return nil
	}
	b, err := getSandbox(s.ctx, s.store.db, r.SandboxID)
	if isCode(err, api.NotFound) {
		return s.finishRecoveryAttempt(r, nil)
	}
	if err != nil {
		return err
	}
	source, err := s.store.operation(s.ctx, r.Source)
	if err != nil || source.Status != "failed" {
		return err
	}
	owned := b.OperationID == source.ID ||
		strings.HasPrefix(b.OperationID, "recovery:"+r.ID+":")
	if r.Release != nil && b.State == "terminated" &&
		b.TenantID == r.Release.Tenant &&
		b.VMID == r.Release.VM &&
		b.Instance == r.Release.Instance {
		owned = true
	}
	if !owned {
		// Activation can commit running before retiring the old sleep snapshot.
		if b.State == "running" && r.Instance != "" &&
			b.Instance == r.Instance {
			o := r.operation()
			snapshot := b.SnapshotID
			err = s.finishResume(&o, &b)
			return s.finishRecoveryAttempt(r, err, "retire-snapshot:"+snapshot)
		}
		return s.finishRecoveryAttempt(r, nil)
	}
	if b.State == "provisioning" && b.VMID == "" {
		step := "provision"
		if source.Command.Action == "fork" {
			step = "fork-clone:" + b.ID
		}
		proof, e := s.findResourceStep(source, step, "")
		if e != nil {
			return e
		}
		if proof != nil {
			if !recoveryTerminal(proof.Status) {
				return nil
			}
			b.VMID = proof.VMID
			if err = saveSandbox(s.ctx, s.store.db, b); err != nil {
				return err
			}
		}
	}
	// A queued operation on this sandbox owns its dependencies until it finishes.
	ownership := s.lockResourceOwnership()
	vms, _, err := s.resourceReferences(b.ID, source.ID)
	ownership()
	if err != nil {
		return err
	}
	if b.VMID != "" && vms[b.VMID] {
		return nil
	}
	o := r.operation()
	o.TenantID, o.ChargeTenantID, o.Command = source.TenantID, source.ChargeTenantID, source.Command
	switch b.State {
	case "provisioning", "terminated":
		cleanup := &o
		if b.VMID != "" &&
			(source.Command.Action == "fork" || source.Command.Action == "release") {
			deletion, e := s.findResourceStep(
				source,
				releaseVMStep(b.ID),
				"delete",
			)
			if e != nil {
				return e
			}
			if deletion != nil {
				if deletion.Request.VMID != b.VMID ||
					deletion.Request.ExpectedInstance != b.Instance {
					return api.Fail(
						api.ManualRecovery,
						"original release does not match the sandbox instance",
					)
				}
				if !recoveryTerminal(deletion.Status) {
					return nil
				}
				if deletion.Status == "succeeded" {
					_, e = s.opts.Engine.GetVM(s.ctx, b.VMID)
					if e == nil {
						return api.Fail(
							api.ManualRecovery,
							"successful deletion still has a live VM",
						)
					}
					if !isCode(e, api.NotFound) {
						return e
					}
				}
			}
			// Unknown dispatch reuses the original key. Only a proven terminal
			// engine failure permits the compensation intent's distinct key.
			if deletion == nil || deletion.Status != "failed" {
				cleanup = &source
			}
		}
		// Release is compensating work; it never retries activation or guest exec.
		err = s.release(cleanup, &b)
		return s.finishRecoveryAttempt(
			r,
			err,
			releaseVMStep(b.ID),
			"retire-snapshot:"+b.SnapshotID,
		)
	case "pausing":
		if source.Command.Action != "pause" {
			return api.Fail(
				api.ManualRecovery,
				"pausing sandbox has no pause intent",
			)
		}
		if b.SnapshotID == "" {
			proof, e := s.findResourceStep(
				source,
				"hibernate-snapshot",
				"snapshot",
			)
			if e != nil {
				return e
			}
			if proof != nil && !recoveryTerminal(proof.Status) {
				return nil
			}
			if proof != nil && proof.Status == "succeeded" &&
				proof.Request.VMID == b.VMID &&
				proof.Request.ExpectedInstance == b.Instance {
				b.SnapshotID = proof.SnapshotID
				ownership := s.lockResourceOwnership()
				err = recordSnapshotOwner(
					s.ctx,
					s.store.db,
					b.SnapshotID,
					b.TenantID,
					source.ID,
					"captured",
				)
				ownership()
				if err != nil {
					return err
				}
				if err = saveSandbox(s.ctx, s.store.db, b); err != nil {
					return err
				}
			}
		}
		if b.SnapshotID != "" {
			if err = s.recoverySnapshotReady(b.SnapshotID); err != nil {
				return err
			}
		}
		v, e := s.opts.Engine.GetVM(s.ctx, b.VMID)
		if isCode(e, api.NotFound) && b.SnapshotID != "" {
			if err = s.reclaimQuota("vm_disk", b.VMID); err != nil {
				return err
			}
			b.State, b.VMID, b.Instance, b.OperationID = "paused", "", "", ""
			b.PausedUntil = time.Now().UTC().Add(s.opts.PausedTTL)
			return s.finishRecoveryAttempt(r, saveSandbox(s.ctx, s.store.db, b))
		}
		if e != nil {
			return e
		}
		if !v.Verified || (v.Phase != "running" && b.SnapshotID == "") ||
			b.Instance == "" ||
			v.Instance != b.Instance {
			return api.Fail(
				api.ManualRecovery,
				"cannot prove the original hibernating VM instance",
			)
		}
		if b.SnapshotID == "" {
			if err = s.admitQuotaRecovery(
				r,
				o,
				b,
				"hibernate-snapshot",
				"snapshot",
				api.Request{
					Action:           "snapshot",
					VMID:             b.VMID,
					ExpectedInstance: b.Instance,
				},
				b.Shape.DiskMiB+b.Shape.MemoryMiB,
			); err != nil {
				return err
			}
		}
		err = s.pause(&o, &b)
		return s.finishRecoveryAttempt(
			r,
			err,
			"pause-network",
			"hibernate-snapshot",
			"hibernate-delete",
		)
	case "resuming":
		if source.Command.Action == "rollback" {
			err = s.recoverRollback(r, source, &b)
			return s.finishRecoveryAttempt(
				r,
				err,
				"rollback-restore",
				"rollback-clone",
				"resume-cleanup",
				"network-off:"+b.ID,
				"network-on:"+b.ID,
				"retire-snapshot:"+b.SnapshotID,
			)
		}
		if source.Command.Action != "resume" {
			return api.Fail(
				api.ManualRecovery,
				"resuming sandbox has no resume or rollback intent",
			)
		}
		err = s.recoverFailedClone(r, source, &b, "resume-clone")
		return s.finishRecoveryAttempt(r, err, "resume-cleanup")
	default:
		return s.finishRecoveryAttempt(r, nil)
	}
}

func (s *Service) recoverFailedClone(
	r *resourceRecovery,
	source Operation,
	b *Sandbox,
	cloneStep string,
) error {
	if err := s.recoverySnapshotReady(b.SnapshotID); err != nil {
		return err
	}
	proof, err := s.findResourceStep(source, cloneStep, "clone")
	if err != nil {
		return err
	}
	if proof != nil && !recoveryTerminal(proof.Status) {
		return api.Fail(api.Conflict, "resume clone is still running")
	}
	if b.VMID != "" && (proof == nil || proof.VMID != b.VMID) {
		return api.Fail(
			api.ManualRecovery,
			"failed resume VM has no matching clone receipt",
		)
	}
	vmid := b.VMID
	if proof != nil {
		vmid = proof.VMID
	}
	s.closeConnections(b.ID)
	if err = s.revoke(b.ID); err != nil {
		return err
	}
	if vmid != "" {
		unlock := s.lockResourceOwnership()
		defer unlock()
		vms, _, e := s.resourceReferences(b.ID, source.ID)
		if e != nil {
			return e
		}
		if vms[vmid] {
			return api.Fail(api.Conflict, "failed resume VM has another owner")
		}
		v, e := s.opts.Engine.GetVM(s.ctx, vmid)
		if e != nil && !isCode(e, api.NotFound) {
			return e
		}
		if isCode(e, api.NotFound) {
			if reclaimErr := s.reclaimQuota(
				"vm_disk",
				vmid,
			); reclaimErr != nil {
				return reclaimErr
			}
		}
		if e == nil {
			if !v.Verified || (b.Instance != "" && b.Instance != v.Instance) ||
				(r.Instance != "" && r.Instance != v.Instance) {
				return api.Fail(
					api.ManualRecovery,
					"failed resume VM instance cannot be proven",
				)
			}
			r.VMID, r.Instance = vmid, v.Instance
			if err = s.saveRecovery(*r); err != nil {
				return err
			}
			if _, err = s.engine(
				r.operation(),
				"resume-cleanup",
				api.Request{
					Action:           "delete",
					VMID:             vmid,
					ExpectedInstance: v.Instance,
				},
			); err != nil {
				return err
			}
		}
	}
	b.State, b.VMID, b.Instance, b.OperationID = "paused", "", "", ""
	b.ResumeAttempt++
	b.LastError = "resume failed; restored the retained sleep snapshot"
	return saveSandbox(s.ctx, s.store.db, *b)
}

// Rollback can have reused a VM ID. A resume-clone receipt never authorizes
// deleting that target. Only retry restore on a proven, already stopped source;
// an unproven running instance is left for explicit operator recovery.
func (s *Service) recoverRollback(
	r *resourceRecovery,
	source Operation,
	b *Sandbox,
) error {
	if source.Step == "rollback_clone" {
		// Rollback began asleep: retain its original sleep capture and discard
		// only the clone made for the failed checkpoint activation.
		return s.recoverFailedClone(r, source, b, "rollback-clone")
	}
	checkpointUnlock := s.lock("checkpoint:" + source.Command.CheckpointID)
	defer checkpointUnlock()
	cp, err := s.store.checkpoint(s.ctx, source.Command.CheckpointID)
	if err != nil {
		return err
	}
	if cp.SandboxID != b.ID || cp.TenantID != b.TenantID {
		return api.Fail(
			api.ManualRecovery,
			"rollback checkpoint ownership does not match",
		)
	}
	if err = requireSnapshotOwner(
		s.ctx,
		s.store.db,
		cp.SnapshotID,
		cp.TenantID,
	); err != nil {
		return err
	}
	if err = s.recoverySnapshotReady(cp.SnapshotID); err != nil {
		return err
	}
	if s.credentialsMissing(*b) {
		return api.Fail(
			api.Conflict,
			"host credentials must be supplied before rollback recovery",
		)
	}
	o := r.operation()
	o.Command, o.TenantID, o.ChargeTenantID = source.Command, source.TenantID, source.ChargeTenantID
	if b.VMID == "" {
		return api.Fail(
			api.ManualRecovery,
			"rollback target is missing; refusing an unproven replacement",
		)
	}
	v, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
	if err != nil {
		return err
	}
	if !v.Verified {
		return api.Fail(api.ManualRecovery, "rollback VM is not verified")
	}
	target := ""
	for _, a := range r.Allocations {
		if a.Step == "rollback-restore" &&
			a.Resolution.State == "result_known" &&
			a.Resolution.Proof != nil {
			target = a.Resolution.Proof.ResultInstance
			o.ID = a.OperationID
		}
	}
	if target != "" && v.Instance != target {
		return api.Fail(api.ManualRecovery, "restore result instance changed")
	}
	if target == "" && v.Phase == "stopped" && source.SourceInstance != "" &&
		v.Instance == source.SourceInstance {
		stop, e := s.findResourceStep(source, "rollback-stop", "stop")
		if e != nil {
			return e
		}
		if stop == nil || stop.Status != "succeeded" || stop.VMID != v.ID ||
			stop.Request.ExpectedInstance != v.Instance {
			return api.Fail(
				api.ManualRecovery,
				"rollback stop receipt does not prove the stopped source",
			)
		}
		if err = s.admitQuotaRecovery(
			r,
			o,
			*b,
			"rollback-restore",
			"vm_disk",
			api.Request{
				Action:           "restore",
				VMID:             v.ID,
				SnapshotID:       cp.SnapshotID,
				ExpectedInstance: v.Instance,
			},
			b.Shape.DiskMiB,
		); err != nil {
			return err
		}
		_, err = s.engine(
			o,
			"rollback-restore",
			api.Request{
				Action:           "restore",
				VMID:             v.ID,
				SnapshotID:       cp.SnapshotID,
				ExpectedInstance: v.Instance,
			},
		)
		if err != nil {
			return err
		}
		if _, err = s.reconcileRecoveryAllocations(r); err != nil {
			return err
		}
		for _, a := range r.Allocations {
			if a.OperationID == o.ID && a.Step == "rollback-restore" &&
				a.Resolution.Proof != nil {
				target = a.Resolution.Proof.ResultInstance
			}
		}
		v, err = s.opts.Engine.GetVM(s.ctx, b.VMID)
		if err != nil {
			return err
		}
	}
	if target == "" {
		// Preserve ordinary public activation recovery's independent network proof;
		// it does not mint a compensation allocation or a new generation.
		activation, e := s.findResourceStep(
			source,
			"network-off:"+b.ID,
			"network",
		)
		if e != nil {
			return e
		}
		if activation == nil || activation.Status != "succeeded" ||
			activation.VMID != v.ID ||
			activation.Request.ExpectedInstance == "" ||
			activation.Request.ExpectedInstance != v.Instance ||
			!v.Verified ||
			!v.AgentReady ||
			v.Phase != "running" {
			return api.Fail(
				api.ManualRecovery,
				"rollback running instance cannot be proven",
			)
		}
		r.Instance = activation.Request.ExpectedInstance
		if err = s.saveRecovery(*r); err != nil {
			return err
		}
		if err = s.activate(o, b, false); err != nil {
			return err
		}
		return s.finishResume(&o, b)
	}
	if v.Instance != target || v.OperationID != "" {
		return api.Fail(
			api.ManualRecovery,
			"rollback result has another instance or owner",
		)
	}
	r.Instance = target
	if v.Phase != "running" || !v.AgentReady || !v.Verified {
		return api.Fail(
			api.ManualRecovery,
			"rollback restore is not verified running",
		)
	}
	if err = s.saveRecovery(*r); err != nil {
		return err
	}
	if err = s.activateTarget(
		o,
		b,
		false,
		&activationTarget{
			Origin:   o.ID,
			Kind:     "recovery",
			VM:       b.VMID,
			Instance: target,
		},
	); err != nil {
		return err
	}
	return s.finishResume(&o, b)
}
