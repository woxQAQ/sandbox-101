package sandboxd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// activationFence requires configure to own its fence and every other
// activation to observe no fence. Callers guard publication with configuration:ID.
func (s *Service) activationFence(o Operation, id string) error {
	fence, err := configFence(s.ctx, s.store.db, id)
	if err != nil {
		return err
	}
	if (o.Command.Action == "configure" && fence != o.ID) ||
		(o.Command.Action != "configure" && fence != "") {
		return api.Fail(
			api.Conflict,
			"sandbox configuration operation is fenced or superseded",
		)
	}
	return nil
}

func (s *Service) activate(o Operation, b *Sandbox, reset bool) error {
	return s.activateTarget(o, b, reset, "")
}

func (s *Service) activateTarget(
	o Operation,
	b *Sandbox,
	reset bool,
	target string,
) (err error) {
	if err = s.activationFence(o, b.ID); err != nil {
		return err
	}
	if s.CredentialsMissing(*b) {
		return api.Fail(
			api.Conflict,
			"host credentials must be supplied again after daemon restart",
		)
	}
	v, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
	if err != nil {
		return err
	}
	if target != "" && (v.ID != b.VMID || v.Instance != target) {
		return api.Fail(
			api.ManualRecovery,
			"activation read returned another instance",
		)
	}
	if target != "" {
		if err = s.checkActivationTarget(o, *b, target); err != nil {
			return err
		}
	}
	if v.Phase != "running" || !v.AgentReady || !v.Verified {
		return api.Fail(api.Conflict, "engine VM is not verified ready")
	}
	// Network is disabled even for a recovered activation. Sandbox policy always precedes enable.
	if _, err = s.engine(
		o,
		"network-off:"+b.ID,
		api.Request{
			Action:           "network",
			VMID:             v.ID,
			ExpectedInstance: v.Instance,
			NetworkEnabled:   false,
		},
	); err != nil {
		return err
	}
	if target != "" {
		if err = s.checkActivationTarget(o, *b, target); err != nil {
			return err
		}
	}
	if err = s.opts.Agent.Info(s.ctx, v); err != nil {
		return err
	}
	// Fence replacement and Bind are serialized, so an old worker cannot
	// publish after a newer intent was durably admitted.
	publication := s.lock("configuration:" + b.ID)
	if err = s.activationFence(o, b.ID); err == nil {
		if s.opts.Egress != nil {
			err = s.bind(s.ctx, *b, v.VSockPath)
		} else if b.Policy.Mode != "none" {
			err = api.Fail(
				api.Incompatible,
				"egress manager required for network mode",
			)
		}
	}
	publication()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			s.closeConnections(b.ID)
			err = errors.Join(err, s.revoke(b.ID))
		}
	}()
	if target != "" {
		if err = s.checkActivationTarget(o, *b, target); err != nil {
			return err
		}
	}
	if err = s.opts.Agent.Activate(
		s.ctx,
		v,
		Activation{
			SandboxID:     b.ID,
			Hostname:      strings.ReplaceAll(b.ID, "_", "-"),
			ResetIdentity: reset,
			Env:           b.Env,
			Network:       b.Policy.Mode,
			CAPEM:         s.opts.CAPEM,
		},
	); err != nil {
		return err
	}
	publication = s.lock("configuration:" + b.ID)
	defer publication()
	if err = s.activationFence(o, b.ID); err != nil {
		return err
	}
	if b.Policy.Mode == "direct" {
		if _, err = s.engine(
			o,
			"network-on:"+b.ID,
			api.Request{
				Action:           "network",
				VMID:             v.ID,
				ExpectedInstance: v.Instance,
				NetworkEnabled:   true,
			},
		); err != nil {
			return err
		}
	}
	if target != "" {
		if err = s.checkActivationTarget(o, *b, target); err != nil {
			return err
		}
	}
	next := *b
	next.Instance = v.Instance
	if b.State != "running" {
		next.Deadline = time.Now().
			UTC().
			Add(time.Duration(b.TTLSeconds) * time.Second)
	}
	next.State, next.OperationID, next.LastError = "running", "", ""
	if err = s.commitRunning(o, &next, target); err != nil {
		return err
	}
	*b = next
	return nil
}

func (s *Service) claim(o *Operation) error {
	id := o.IDs[0]
	unlock := s.lock(id)
	defer unlock()
	b, err := getSandbox(s.ctx, s.store.db, id)
	if err != nil {
		return err
	}
	if b.State == "running" {
		o.Result.Sandbox = &b
		return nil
	}
	if b.State == "terminated" {
		return api.Fail(api.Conflict, "sandbox was released")
	}
	var t Template
	if o.Command.Template != nil {
		t = *o.Command.Template
	} else {
		t, err = s.store.template(s.ctx, b.TemplateID)
		if err != nil {
			return err
		}
	}
	if err = requireSnapshotOwner(
		s.ctx,
		s.store.db,
		t.SnapshotID,
		b.TenantID,
	); err != nil {
		return err
	}
	if b.VMID == "" && b.ProvisionSource == "" {
		if err = s.claimWarm(t, &b); err != nil {
			return err
		}
	}
	if b.ProvisionSource == "warm" {
		if err = s.activateWarm(*o, &b); err == nil {
			o.Result.Sandbox = &b
			return nil
		}
		if s.ctx.Err() != nil {
			return err
		}
		b.ProvisionSource = "warm-rejected"
		b.LastError = "warm activation failed"
		if saveErr := saveSandbox(s.ctx, s.store.db, b); saveErr != nil {
			return saveErr
		}
	}
	if b.ProvisionSource == "warm-rejected" {
		if err = s.discardWarm(*o, &b, t); err != nil {
			return err
		}
	}
	if b.VMID == "" {
		if t.SnapshotID == "" {
			b.ProvisionSource = "cold"
		} else {
			b.ProvisionSource = "golden"
		}
		var op api.Operation
		if t.SnapshotID != "" {
			op, err = s.engine(
				*o,
				"provision",
				api.Request{Action: "clone", SnapshotID: t.SnapshotID},
			)
		} else {
			op, err = s.engine(
				*o,
				"provision",
				api.Request{
					Action: "create",
					Config: &api.Config{
						Image: t.Image,
						Shape: t.Shape,
						Labels: map[string]string{
							"sandboxd":   "sandbox",
							"sandbox_id": b.ID,
						},
					},
				},
			)
		}
		b.VMID = op.VMID
		if saveErr := s.saveSandboxReference(b); saveErr != nil {
			return saveErr
		}
		if err != nil {
			return s.failProvision(*o, &b, err)
		}
	}
	if t.SnapshotID == "" {
		if _, err = s.engine(
			*o,
			"start",
			api.Request{Action: "start", VMID: b.VMID},
		); err != nil {
			return s.failProvision(*o, &b, err)
		}
	}
	if err = s.activate(*o, &b, true); err != nil {
		return s.failProvision(*o, &b, err)
	}
	o.Result.Sandbox = &b
	return nil
}

func (s *Service) claimWarm(t Template, b *Sandbox) error {
	unlock := s.lockResourceOwnership()
	defer unlock()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var vmid string
	err = tx.QueryRowContext(s.ctx, "SELECT vm_id FROM warm WHERE template_id=? AND snapshot_id=? ORDER BY created_at LIMIT 1", t.ID, t.SnapshotID).
		Scan(&vmid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		s.ctx,
		"DELETE FROM warm WHERE vm_id=?",
		vmid,
	); err != nil {
		return err
	}
	b.VMID = vmid
	b.ProvisionSource = "warm"
	o := Operation{ID: b.OperationID, ChargeTenantID: b.TenantID}
	if err = putQuotaResource(
		s.ctx,
		tx,
		quotaResource{
			Tenant:  b.TenantID,
			Kind:    "vm_disk",
			Key:     vmid,
			Storage: b.Shape.DiskMiB,
			Source:  o.ID,
		},
		quotaRef{Kind: "active_use", ID: "sandbox:" + b.ID},
	); err != nil {
		return err
	}
	if err = quotaSnapshot(
		s.ctx,
		tx,
		o,
		t.SnapshotID,
		b.Shape,
		quotaRef{Kind: "active_use", ID: "backing:" + vmid},
	); err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
		b.TenantID,
		stepKey(o, "provision"),
	); err != nil {
		return err
	}
	if err = saveSandbox(s.ctx, tx, *b); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) failProvision(o Operation, b *Sandbox, cause error) error {
	if s.ctx.Err() != nil {
		return cause
	}
	if s.opts.Egress != nil {
		if err := s.revoke(b.ID); err != nil {
			b.LastError = err.Error()
			_ = saveSandbox(s.ctx, s.store.db, *b)
			return err
		}
	}
	if b.VMID != "" {
		if _, err := s.engine(
			o,
			"failed-delete:"+b.ID,
			api.Request{Action: "delete", VMID: b.VMID},
		); err != nil {
			b.LastError = "provisioning failed; cleanup pending: " + err.Error()
			_ = saveSandbox(s.ctx, s.store.db, *b)
			return err
		}
	}
	b.State = "terminated"
	b.VMID = ""
	b.Instance = ""
	b.LastError = cause.Error()
	b.OperationID = ""
	_ = s.saveQuotaSandbox(*b)
	s.mu.Lock()
	delete(s.secrets, b.ID)
	s.mu.Unlock()
	return cause
}

func (s *Service) change(o *Operation) error {
	unlock := s.lock(o.Command.SandboxID)
	defer unlock()
	b, err := getSandbox(s.ctx, s.store.db, o.Command.SandboxID)
	if err != nil {
		return err
	}
	if !o.Command.LeaseDeadline.IsZero() {
		deadline := b.Deadline
		if b.State == "paused" {
			deadline = b.PausedUntil
		}
		if !deadline.Equal(o.Command.LeaseDeadline) ||
			time.Now().Before(deadline) {
			o.Result.Sandbox = &b
			return nil
		}
	}
	switch o.Command.Action {
	case "pause":
		err = s.pause(o, &b)
	case "resume":
		err = s.resume(o, &b)
	case "rollback":
		err = s.rollback(o, &b)
	case "release":
		err = s.release(o, &b)
	case "renew":
		if b.State != "running" {
			return api.Fail(api.Conflict, "renew requires running sandbox")
		}
		if time.Now().After(b.Deadline) {
			return api.Fail(api.Conflict, "lease expired")
		}
		ttl, e := s.ttl(o.Command.TTLSeconds)
		if e != nil {
			return e
		}
		b.TTLSeconds = ttl
		b.Deadline = time.Now().UTC().Add(time.Duration(ttl) * time.Second)
		err = saveSandbox(s.ctx, s.store.db, b)
	case "configure":
		err = s.configure(o, &b)
	case "checkpoint":
		cp := Checkpoint{
			ID:        o.IDs[0],
			TenantID:  b.TenantID,
			SandboxID: b.ID,
			Shape:     b.Shape,
			CreatedAt: o.CreatedAt,
		}
		// A sleeping source already owns an immutable capture. Its sandbox references
		// keep that engine snapshot alive without starting another VM.
		switch b.State {
		case "paused":
			if err = requireSnapshotOwner(
				s.ctx,
				s.store.db,
				b.SnapshotID,
				b.TenantID,
			); err != nil {
				return err
			}
			cp.SnapshotID = b.SnapshotID
		case "running":
			snap, e := s.engine(
				*o,
				"checkpoint-capture",
				api.Request{
					Action:           "snapshot",
					VMID:             b.VMID,
					ExpectedInstance: b.Instance,
				},
			)
			if e != nil {
				return e
			}
			cp.SnapshotID = snap.SnapshotID
		default:
			return api.Fail(
				api.Conflict,
				"sandbox cannot checkpoint in %s",
				b.State,
			)
		}
		ownership := s.lockResourceOwnership()
		tx, e := s.store.db.BeginTx(s.ctx, nil)
		if e == nil {
			e = quotaSnapshot(
				s.ctx,
				tx,
				*o,
				cp.SnapshotID,
				cp.Shape,
				quotaRef{Kind: "active_use", ID: "checkpoint:" + cp.ID},
			)
			if e == nil {
				_, e = tx.ExecContext(
					s.ctx,
					"INSERT INTO checkpoints(id,tenant,body) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
					cp.ID,
					cp.TenantID,
					encode(cp),
				)
			}
			if e == nil {
				e = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
		}
		err = e
		ownership()
		o.Result.Checkpoint = &cp
	}
	o.Result.Sandbox = &b
	return err
}

func (s *Service) configure(o *Operation, b *Sandbox) (err error) {
	defer func() {
		s.mu.Lock()
		delete(s.opSecrets, o.ID)
		s.mu.Unlock()
	}()
	configuration := s.lock("configuration:" + b.ID)
	if err = s.activationFence(*o, b.ID); err != nil {
		configuration()
		return err
	}
	// Once an owned configure starts, every error closes its data paths.
	// Read the durable configuration for diagnostics so a failed save never
	// writes the rejected policy back through LastError handling.
	defer func() {
		if err != nil {
			s.closeConnections(b.ID)
			err = errors.Join(err, s.revoke(b.ID))
			if saved, e := getSandbox(s.ctx, s.store.db, b.ID); e == nil {
				saved.LastError = err.Error()
				_ = saveSandbox(s.ctx, s.store.db, saved)
			}
		}
	}()
	if b.State != "running" && b.State != "paused" {
		configuration()
		return api.Fail(
			api.Conflict,
			"configuration requires running or paused sandbox",
		)
	}
	s.closeConnections(b.ID)
	if err = s.revoke(b.ID); err != nil {
		configuration()
		return err
	}
	s.mu.Lock()
	s.secrets[b.ID] = s.opSecrets[o.ID]
	s.mu.Unlock()
	b.Env = map[string]string{}
	b.RequiresSecrets = false
	for k, v := range o.Command.Env {
		if v.IsSecret() {
			b.RequiresSecrets = true
			if v.Inject != nil && v.Inject.Placeholder != "" {
				b.Env[k] = v.Inject.Placeholder
			}
		} else {
			b.Env[k] = v.Value
		}
	}
	if o.Command.Policy.Mode != "" {
		b.Policy = o.Command.Policy
	}
	if b.State == "running" {
		err = saveSandbox(s.ctx, s.store.db, *b)
	}
	configuration()
	if err != nil {
		return err
	}
	if b.State == "running" {
		if err = s.activate(*o, b, false); err != nil {
			return err
		}
	}
	configuration = s.lock("configuration:" + b.ID)
	defer configuration()
	if err = s.activationFence(*o, b.ID); err != nil {
		return err
	}
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err = saveSandbox(s.ctx, tx, *b); err != nil {
		return err
	}
	if err = clearConfigFence(s.ctx, tx, b.ID, o.ID); err != nil {
		return err
	}
	completed := *o
	completed.Status, completed.Error = "succeeded", nil
	completed.FinishedAt = time.Now().UTC()
	completed.Result.Sandbox = b
	if err = saveOp(s.ctx, tx, completed); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*o = completed
	return nil
}

func (s *Service) pause(o *Operation, b *Sandbox) error {
	if b.State == "paused" {
		return nil
	}
	if b.State != "running" && b.State != "pausing" {
		return api.Fail(api.Conflict, "sandbox cannot pause in %s", b.State)
	}
	if b.State == "running" && b.SnapshotID != "" {
		if err := s.commitRunning(*o, b); err != nil {
			return err
		}
	}
	b.State = "pausing"
	b.OperationID = o.ID
	if err := saveSandbox(s.ctx, s.store.db, *b); err != nil {
		return err
	}
	s.closeConnections(b.ID)
	if s.opts.Egress != nil {
		if err := s.revoke(b.ID); err != nil {
			return err
		}
	}
	if _, err := s.engine(
		*o,
		"pause-network",
		api.Request{
			Action:           "network",
			VMID:             b.VMID,
			ExpectedInstance: b.Instance,
			NetworkEnabled:   false,
		},
	); err != nil {
		return err
	}
	if b.SnapshotID == "" {
		snap, err := s.engine(
			*o,
			"hibernate-snapshot",
			api.Request{
				Action:           "snapshot",
				VMID:             b.VMID,
				ExpectedInstance: b.Instance,
			},
		)
		if err != nil {
			b.State = "running"
			b.OperationID = ""
			b.LastError = err.Error()
			_ = saveSandbox(s.ctx, s.store.db, *b)
			restoreErr := s.activate(
				Operation{ID: o.ID + ":rollback"},
				b,
				false,
			)
			if restoreErr != nil {
				b.LastError = "pause failed; network remains blocked: " + restoreErr.Error()
				_ = saveSandbox(s.ctx, s.store.db, *b)
			}
			return err
		}
		b.SnapshotID = snap.SnapshotID
		if err = s.saveSandboxReference(*b); err != nil {
			return err
		}
	}
	if _, err := s.engine(
		*o,
		"hibernate-delete",
		api.Request{
			Action:           "delete",
			VMID:             b.VMID,
			ExpectedInstance: b.Instance,
		},
	); err != nil {
		return err
	}
	b.State = "paused"
	b.VMID = ""
	b.Instance = ""
	b.PausedUntil = time.Now().UTC().Add(s.opts.PausedTTL)
	b.OperationID = ""
	return saveSandbox(s.ctx, s.store.db, *b)
}

func (s *Service) resume(o *Operation, b *Sandbox) error {
	if b.State == "running" {
		return s.finishResume(o, b)
	}
	if b.State != "paused" && b.State != "resuming" {
		return api.Fail(api.Conflict, "sandbox cannot resume in %s", b.State)
	}
	if s.CredentialsMissing(*b) {
		return api.Fail(
			api.Conflict,
			"host credentials must be supplied again after daemon restart",
		)
	}
	if err := requireSnapshotOwner(
		s.ctx,
		s.store.db,
		b.SnapshotID,
		b.TenantID,
	); err != nil {
		return err
	}
	b.State = "resuming"
	b.OperationID = o.ID
	if err := saveSandbox(s.ctx, s.store.db, *b); err != nil {
		return err
	}
	if b.VMID == "" {
		op, err := s.engine(
			*o,
			"resume-clone",
			api.Request{Action: "clone", SnapshotID: b.SnapshotID},
		)
		b.VMID = op.VMID
		if saveErr := s.saveSandboxReference(*b); saveErr != nil {
			return saveErr
		}
		if err != nil {
			if b.VMID == "" {
				b.State = "paused"
				b.ResumeAttempt++
				b.OperationID = ""
			}
			b.LastError = err.Error()
			_ = saveSandbox(s.ctx, s.store.db, *b)
			return err
		}
	}
	if err := s.activate(*o, b, false); err != nil {
		if s.opts.Egress != nil {
			err = errors.Join(err, s.revoke(b.ID))
		}
		if _, cleanup := s.engine(
			*o,
			"resume-rollback",
			api.Request{Action: "delete", VMID: b.VMID},
		); cleanup != nil {
			b.LastError = "resume failed; cleanup pending: " + cleanup.Error()
			_ = saveSandbox(s.ctx, s.store.db, *b)
			return cleanup
		}
		b.State = "paused"
		b.ResumeAttempt++
		b.VMID = ""
		b.Instance = ""
		b.OperationID = ""
		b.LastError = err.Error()
		_ = saveSandbox(s.ctx, s.store.db, *b)
		return err
	}
	return s.finishResume(o, b)
}

func (s *Service) finishResume(o *Operation, b *Sandbox) error {
	if b.SnapshotID != "" {
		if err := s.commitRunning(*o, b); err != nil {
			return err
		}
	}
	intents, err := s.resourceRecoveries()
	if err != nil {
		return err
	}
	for _, r := range intents {
		if r.Kind == "retired_snapshot" && r.SandboxID == b.ID && !r.Done {
			// Cleanup owns its errors independently from a successful activation.
			_ = s.recoverRetiredSnapshot(&r)
		}
	}
	return nil
}

func (s *Service) proveReleaseInstance(o Operation, b *Sandbox) error {
	if b.VMID == "" || b.Instance != "" {
		return nil
	}
	source := o
	if b.OperationID != "" && b.OperationID != o.ID {
		var err error
		source, err = s.store.operation(s.ctx, b.OperationID)
		if err != nil {
			return err
		}
	}
	proof, err := s.findResourceStep(source, "network-off:"+b.ID, "network")
	if err != nil {
		return err
	}
	if proof == nil || proof.Status != "succeeded" || proof.VMID != b.VMID ||
		proof.Request.ExpectedInstance == "" {
		return api.Fail(
			api.ManualRecovery,
			"sandbox VM instance cannot be proven for release",
		)
	}
	b.Instance = proof.Request.ExpectedInstance
	return saveSandbox(s.ctx, s.store.db, *b)
}

func (s *Service) release(o *Operation, b *Sandbox) error {
	if b.State == "terminated" {
		intents, err := s.resourceRecoveries()
		if err != nil {
			return err
		}
		for _, r := range intents {
			if r.SandboxID == b.ID && hasUnresolvedAllocations(r) {
				if _, err = s.reconcileRecoveryAllocations(&r); err != nil {
					return err
				}
			}
		}
		current, err := getSandbox(s.ctx, s.store.db, b.ID)
		if err != nil {
			return err
		}
		*b = current
		collector, err := s.boundCollector(*b)
		if err != nil {
			return err
		}
		if collector != nil {
			if err = s.consumeInstanceCleanup(collector); err != nil {
				return err
			}
			current, err = getSandbox(s.ctx, s.store.db, b.ID)
			if err != nil {
				return err
			}
			*b = current
		}
	}
	// A lost delete response can leave metadata pointing at an already reclaimed
	// VM. Confirm absence before issuing another delete or requiring a live fence.
	if b.VMID != "" {
		_, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
		if isCode(err, api.NotFound) {
			if err = s.reclaimQuota("vm_disk", b.VMID); err != nil {
				return err
			}
			b.VMID = ""
			b.Instance = ""
		} else if err != nil {
			return err
		}
	}
	// Establish the deletion fence before transferring cleanup ownership. This
	// check applies to explicit release, fork rollback and background recovery.
	if err := s.proveReleaseInstance(*o, b); err != nil {
		return err
	}
	s.closeConnections(b.ID)
	if s.opts.Egress != nil {
		if err := s.revoke(b.ID); err != nil {
			return err
		}
	}
	origin, err := s.freezeRelease(*o, b)
	if err != nil {
		return err
	}
	if b.VMID != "" {
		deletionOp := *o
		request := api.Request{
			Action:           "delete",
			VMID:             b.VMID,
			ExpectedInstance: b.Instance,
		}
		if origin != nil {
			request = origin.Release.Request
			deletionOp = origin.Release.Source
			var proof api.Operation
			var e error
			if origin.Release.Dispatched {
				proof, e = s.opts.Engine.FindOperation(s.ctx, request.Key)
			} else {
				e = api.Fail(api.NotFound, "release not dispatched")
			}
			if e != nil && !isCode(e, api.NotFound) {
				return e
			}
			if e == nil {
				if proof.Key != request.Key ||
					!reflect.DeepEqual(proof.Request, request) {
					return api.Fail(
						api.ManualRecovery,
						"release receipt differs from frozen intent",
					)
				}
				if proof.Status == "failed" {
					compensationKey := stepKey(
						origin.operation(),
						releaseVMStep(b.ID),
					)
					compensation, e := s.opts.Engine.FindOperation(
						s.ctx,
						compensationKey,
					)
					if e != nil && !isCode(e, api.NotFound) {
						return e
					}
					if e == nil {
						expected := request
						expected.Key = compensationKey
						if compensation.Key != compensationKey ||
							!reflect.DeepEqual(compensation.Request, expected) {
							return api.Fail(
								api.ManualRecovery,
								"A compensation receipt mismatch",
							)
						}
						if compensation.Status == "failed" {
							origin.Attempt++
							origin.Done = false
							if err = s.saveRecovery(*origin); err != nil {
								return err
							}
						}
					}
					deletionOp = origin.operation()
					deletionOp.TenantID = origin.Release.Source.TenantID
					deletionOp.ChargeTenantID = origin.Release.Tenant
					deletionOp.Command = origin.Release.Source.Command
				}
			}
			origin.Release.Dispatched = true
			if err = s.saveRecovery(*origin); err != nil {
				return err
			}
		}
		request.Key = ""
		if _, err := s.engine(
			deletionOp,
			releaseVMStep(b.ID),
			request,
		); err != nil {
			return err
		}
		b.VMID = ""
		b.Instance = ""
		if err := saveSandbox(s.ctx, s.store.db, *b); err != nil {
			return err
		}
	}
	if b.SnapshotID != "" {
		if err := s.retireSnapshot(*o, b.SnapshotID, b.ID, ""); err != nil {
			return err
		}
		b.SnapshotID = ""
	}
	b.OperationID = ""
	s.mu.Lock()
	delete(s.secrets, b.ID)
	s.mu.Unlock()
	// Termination retires any configuration fence left by a failed configure,
	// atomically with the final persisted state.
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err = quotaTerminate(s.ctx, tx, *b); err != nil {
		return err
	}
	if err = saveSandbox(s.ctx, tx, *b); err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		s.ctx,
		"DELETE FROM configuration_fences WHERE sandbox_id=?",
		b.ID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) template(o *Operation) error {
	t := *o.Command.Template
	unlock := s.lock("template:" + t.ID)
	defer unlock()
	if o.Command.Action == "template_build" {
		v, err := s.engine(
			*o,
			"build-create",
			api.Request{
				Action: "create",
				Config: &api.Config{
					Image: t.Image,
					Shape: t.Shape,
					Labels: map[string]string{
						"sandboxd":     "build",
						"operation_id": o.ID,
					},
				},
			},
		)
		if err != nil {
			return err
		}
		if o.SnapshotID == "" {
			if _, err = s.engine(
				*o,
				"build-start",
				api.Request{Action: "start", VMID: v.VMID},
			); err != nil {
				return err
			}
			vm, err := s.opts.Engine.GetVM(s.ctx, v.VMID)
			if err != nil {
				return err
			}
			if err = s.opts.Agent.Info(s.ctx, vm); err != nil {
				return err
			}
			if t.Warmup != "" {
				// Guest execution results cannot be inferred after a crash. Record its start before dispatch.
				if o.Step == "warmup_inflight" {
					return api.Fail(
						api.Unknown,
						"template warmup result unknown; submit a new build intent",
					)
				}
				if o.Step != "warmup_done" {
					if err = s.setStep(o, "warmup_inflight"); err != nil {
						return err
					}
					if err = s.opts.Agent.Exec(
						s.ctx,
						vm,
						"sbx-op:"+o.ID+":step:template-warmup",
						t.Warmup,
					); err != nil {
						return err
					}
					if err = s.setStep(o, "warmup_done"); err != nil {
						return err
					}
				}
			}
			if err = s.opts.Agent.Activate(
				s.ctx,
				vm,
				Activation{
					SandboxID:     "template-" + t.ID,
					Hostname:      "sandbox-template",
					ResetIdentity: true,
					Env:           map[string]string{},
					Network:       "none",
				},
			); err != nil {
				return err
			}
			if err = s.opts.Agent.PrepareTemplate(s.ctx, vm); err != nil {
				return err
			}
			snap, err := s.engine(
				*o,
				"build-snapshot",
				api.Request{
					Action:           "snapshot",
					VMID:             vm.ID,
					ExpectedInstance: vm.Instance,
				},
			)
			if err != nil {
				return err
			}
			o.SnapshotID = snap.SnapshotID
			if err = s.saveOperationReference(*o); err != nil {
				return err
			}
		}
		t.SnapshotID = o.SnapshotID
		if _, err = s.engine(
			*o,
			"build-delete",
			api.Request{Action: "delete", VMID: v.VMID},
		); err != nil {
			return err
		}
	}
	// Engine inspection stays outside the ownership guard. The admitted
	// operation retains the snapshot until its catalog reference is published.
	if t.SnapshotID != "" {
		snap, err := s.opts.Engine.GetSnapshot(s.ctx, t.SnapshotID)
		if err != nil {
			return err
		}
		if !snap.Published || snap.Deleting || snap.Config.Shape != t.Shape {
			return api.Fail(
				api.Incompatible,
				"template snapshot shape or publication does not match",
			)
		}
		if t.Image == "" {
			t.Image = snap.Config.Image
		}
	}
	t.State = "ready"
	// Publication never creates or transfers ownership: the placeholder
	// admitted with this operation is the only accepted provenance, and it is
	// verified with all three identities in the same transaction as the
	// catalog write. A zero match is never an UPSERT fallback.
	// Lock order is template, then resource ownership, then SQLite transaction.
	// No guest or engine call can extend this publication critical section.
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if t.SnapshotID != "" {
		if err = requireSnapshotOwner(
			s.ctx,
			tx,
			t.SnapshotID,
			t.TenantID,
		); err != nil {
			return err
		}
	}
	owner, current, err := templateIntent(s.ctx, tx, t.ID)
	if err != nil {
		return err
	}
	if owner == "" {
		return api.Fail(
			api.ManualRecovery,
			"template ownership placeholder is missing",
		)
	}
	if owner != t.TenantID {
		return api.Fail(
			api.Conflict,
			"template ownership does not match this operation",
		)
	}
	if current != o.ID {
		return api.Fail(
			api.Conflict,
			"template publication was superseded",
		)
	}
	if _, err = tx.ExecContext(
		s.ctx,
		"INSERT INTO templates(id,tenant,body) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET tenant=excluded.tenant,body=excluded.body",
		t.ID,
		t.TenantID,
		encode(t),
	); err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`DELETE FROM quota_refs WHERE holder_id=?`,
		"template:"+t.ID,
	); err != nil {
		return err
	}
	if err = referenceQuotaSnapshot(
		s.ctx,
		tx,
		t.TenantID,
		t.SnapshotID,
		quotaRef{Kind: "active_use", ID: "template:" + t.ID},
	); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	o.Result.Template = &t
	return nil
}

func (s *Service) DataReady(
	ctx context.Context,
	p Principal,
	id string,
) (Sandbox, error) {
	// Serialize admission before choosing a durable resume attempt. Explicit resume
	// operations still converge under the sandbox lock in change.
	admit := s.lock("auto-resume:" + id)
	defer admit()
	b, err := s.Get(ctx, p, id)
	if err != nil {
		return b, err
	}
	// A configuration fence blocks admission before any automatic resume.
	// A failed fence query is never treated as the absence of a fence.
	fence, err := configFence(ctx, s.store.db, b.ID)
	if err != nil {
		return b, err
	}
	if fence != "" {
		return b, api.Fail(
			api.Conflict,
			"sandbox configuration change is pending",
		)
	}
	if b.State == "paused" || b.State == "resuming" {
		key := "auto-resume:" + b.ID + ":" + b.SnapshotID + ":" + fmt.Sprint(
			b.ResumeAttempt,
		)
		_, err = s.Execute(
			ctx,
			Principal{TenantID: b.TenantID},
			key,
			Command{Action: "resume", SandboxID: id},
		)
		if err != nil {
			return b, err
		}
	}
	unlock := s.lock(id)
	defer unlock()
	b, err = s.Get(ctx, p, id)
	if err != nil {
		return b, err
	}
	// Recheck under the sandbox lock: an admitted configure may have fenced
	// the sandbox between the first read and this acquisition.
	fence, err = configFence(ctx, s.store.db, b.ID)
	if err != nil {
		return b, err
	}
	if fence != "" {
		return b, api.Fail(
			api.Conflict,
			"sandbox configuration change is pending",
		)
	}
	if b.State != "running" || !time.Now().Before(b.Deadline) {
		return b, api.Fail(api.Conflict, "sandbox lease is not active")
	}
	if b.SnapshotID != "" {
		if err = s.commitRunning(
			Operation{ID: "handoff:" + b.ID + ":" + b.SnapshotID},
			&b,
		); err != nil {
			return b, err
		}
	}
	if s.CredentialsMissing(b) {
		return b, api.Fail(
			api.Conflict,
			"host credentials must be supplied again after daemon restart",
		)
	}
	if s.opts.Egress != nil && b.Policy.Mode != "none" {
		s.mu.Lock()
		bound := s.bindings[b.ID] != ""
		s.mu.Unlock()
		if !bound {
			o := Operation{
				ID: "rebind:" + s.epoch + ":" + b.ID + ":" + b.Instance,
			}
			if err = s.activate(o, &b, false); err != nil {
				return b, err
			}
		}
	}
	if b.LastError != "" {
		return b, api.Fail(api.Conflict, "sandbox is blocked: %s", b.LastError)
	}
	if !time.Now().Before(b.Deadline) {
		return b, api.Fail(
			api.Conflict,
			"sandbox lease expired during activation",
		)
	}
	if b.AutoRenew {
		b.Deadline = time.Now().
			UTC().
			Add(time.Duration(b.TTLSeconds) * time.Second)
		if err = saveSandbox(ctx, s.store.db, b); err != nil {
			return b, err
		}
	}
	return b, nil
}

// StoreCheckpointList returns only records the caller owns.
func (s *Service) Checkpoints(
	ctx context.Context,
	p Principal,
) ([]Checkpoint, error) {
	if p.SandboxID != "" {
		return nil, api.Fail(api.Conflict, "tenant access required")
	}
	rows, err := s.store.db.QueryContext(
		ctx,
		"SELECT body FROM checkpoints ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Checkpoint{}
	for rows.Next() {
		var raw []byte
		var cp Checkpoint
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &cp); err != nil {
			return nil, err
		}
		if p.Root || p.TenantID == cp.TenantID {
			out = append(out, cp)
		}
	}
	return out, rows.Err()
}
