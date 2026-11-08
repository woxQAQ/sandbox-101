package sandboxd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

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
	if group, err := s.findAllocationGroup(o); err != nil {
		return errors.Join(cause, err)
	} else if group != nil {
		b.State = "terminated"
		b.LastError = cause.Error()
		if err = s.saveQuotaSandbox(*b); err != nil {
			return errors.Join(cause, err)
		}
		return cause
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
	if s.credentialsMissing(*b) {
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
	if handled, err := s.releaseAllocation(b); handled {
		return err
	}
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

func (s *Service) Get(
	ctx context.Context,
	p Principal,
	id string,
) (Sandbox, error) {
	b, err := getSandbox(ctx, s.store.db, id)
	if err != nil {
		return b, err
	}
	if !visible(p, b.TenantID, id) {
		return Sandbox{}, api.Fail(api.NotFound, "not found")
	}
	return b, nil
}

func (s *Service) List(ctx context.Context, p Principal) ([]Sandbox, error) {
	if p.SandboxID != "" {
		return nil, api.Fail(api.Conflict, "tenant access required")
	}
	all, err := s.store.sandboxes(ctx)
	if err != nil {
		return nil, err
	}
	out := []Sandbox{}
	for _, b := range all {
		if visible(p, b.TenantID, b.ID) {
			out = append(out, b)
		}
	}
	return out, nil
}

func (s *Service) ttl(n int64) (int64, error) {
	if n == 0 {
		n = 300
	}
	if n < 1 || time.Duration(n) > s.opts.MaxTTL/time.Second {
		return 0, api.Fail(
			api.Invalid,
			"ttl_seconds must be between 1 and %d",
			int64(s.opts.MaxTTL/time.Second),
		)
	}
	return n, nil
}

// diskSandbox intentionally contains only guest-visible values. Host secrets never reach SQLite.
type diskSandbox struct {
	Sandbox

	Env map[string]string `json:"guest_env"`
}

func decodeSandbox(b []byte) (Sandbox, error) {
	var d diskSandbox
	err := json.Unmarshal(b, &d)
	d.Sandbox.Env = d.Env
	return d.Sandbox, err
}

func saveSandbox(ctx context.Context, e executor, b Sandbox) error {
	result, err := e.ExecContext(
		ctx,
		"INSERT INTO sandboxes(id,tenant,state,body) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,body=excluded.body",
		b.ID,
		b.TenantID,
		b.State,
		encode(diskSandbox{b, b.Env}),
	)
	return requireSQLWrite(result, err)
}

func getSandbox(ctx context.Context, e executor, id string) (Sandbox, error) {
	var b []byte
	err := e.QueryRowContext(ctx, "SELECT body FROM sandboxes WHERE id=?", id).
		Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return Sandbox{}, api.Fail(api.NotFound, "not found")
	}
	if err != nil {
		return Sandbox{}, err
	}
	return decodeSandbox(b)
}

func (s *store) sandboxes(ctx context.Context) ([]Sandbox, error) {
	rows, err := s.db.QueryContext(
		ctx,
		"SELECT body FROM sandboxes ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Sandbox{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		b, e := decodeSandbox(raw)
		if e != nil {
			return nil, e
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// Sandbox is the durable lifecycle and lease projection of a tenant sandbox.
// Env is excluded from JSON; host secret values are retained only in memory.
type Sandbox struct {
	ID              string            `json:"id"`
	TenantID        string            `json:"tenant_id"`
	State           string            `json:"state"`
	VMID            string            `json:"vm_id,omitempty"`
	SnapshotID      string            `json:"snapshot_id,omitempty"`
	Instance        string            `json:"instance,omitempty"`
	TemplateID      string            `json:"template_id"`
	ProvisionSource string            `json:"provision_source,omitempty"`
	Shape           api.Shape         `json:"shape"`
	Deadline        time.Time         `json:"deadline"`
	PausedUntil     time.Time         `json:"paused_until"`
	TTLSeconds      int64             `json:"ttl_seconds"`
	OnExpire        string            `json:"on_expire"`
	AutoRenew       bool              `json:"auto_renew"`
	Env             map[string]string `json:"-"`
	Policy          egress.Policy     `json:"policy"`
	RequiresSecrets bool              `json:"requires_secrets"`
	ResumeAttempt   uint64            `json:"resume_attempt,omitempty"`
	Epoch           string            `json:"creation_epoch"`
	OperationID     string            `json:"operation_id,omitempty"`
	LastError       string            `json:"last_error,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
}
