package sandboxd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) Submit(
	ctx context.Context,
	p Principal,
	key string,
	c Command,
) (Operation, error) {
	if len(key) < 1 || len(key) > 256 {
		return Operation{}, api.Fail(
			api.Invalid,
			"Idempotency-Key required (max 256 bytes)",
		)
	}
	if p.SandboxID != "" &&
		(c.SandboxID != p.SandboxID || (c.Action != "renew" && c.Action != "release")) {
		return Operation{}, api.Fail(api.NotFound, "not found")
	}
	// HMAC permits comparing retries without persisting credential material or a guessable secret hash.
	hash := s.store.sign("request-v1", p.SandboxID, string(encode(c)))
	unlock := s.lock("request:" + p.TenantID + ":" + key)
	defer unlock()
	old, err := s.store.byKey(ctx, p.TenantID, key)
	if err == nil {
		if old.hash != hash {
			return old, api.Fail(
				api.Conflict,
				"idempotency key has different parameters",
			)
		}
		if !visible(p, old.TenantID, old.Command.SandboxID) && !p.Root {
			return Operation{}, api.Fail(api.NotFound, "not found")
		}
		s.start(old)
		return old, nil
	}
	if !isCode(err, api.NotFound) {
		return Operation{}, err
	}
	env, secrets, err := splitEnv(c.Env)
	if err != nil {
		return Operation{}, err
	}
	o := Operation{
		ID:             newID("op_"),
		Key:            key,
		TenantID:       p.TenantID,
		ChargeTenantID: p.TenantID,
		hash:           hash,
		Command:        sanitized(c),
		Status:         "pending",
		CreatedAt:      time.Now().UTC(),
		Result:         Result{},
	}
	o.Result.OperationID = o.ID
	// Configuration admission and binding publication share this short guard.
	// Agent calls never hold it: a newer intent can fence a blocked worker.
	if c.Action == "configure" {
		configuration := s.lock("configuration:" + c.SandboxID)
		defer configuration()
	}
	// Source inspection precedes ownership/SQL; trusted captures need no Engine query.
	var snapshotSource *templateSnapshotSource
	if c.Action == "template_put" && c.Template != nil &&
		c.Template.SnapshotID != "" {
		if snapshotSource, err = s.templateSource(
			ctx,
			p,
			*c.Template,
		); err != nil {
			return o, err
		}
	}
	if c.Action == "claim" {
		pool := s.lock("pool:" + c.TemplateID)
		defer pool()
	}
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return o, err
	}
	defer func() { _ = tx.Rollback() }()
	var source Sandbox
	switch c.Action {
	case "claim", "fork":
		s.mu.Lock()
		draining := s.draining
		s.mu.Unlock()
		if draining {
			return o, api.Fail(api.Exhausted, "node is draining")
		}
		var t Template
		count := 1
		if c.Action == "claim" {
			t, err = getTemplateTx(ctx, tx, c.TemplateID)
			if err != nil {
				return o, err
			}
			if t.TenantID != p.TenantID {
				return o, api.Fail(api.NotFound, "not found")
			}
			if t.State != "ready" {
				return o, api.Fail(api.Conflict, "template is not ready")
			}
			if err = requireSnapshotOwner(
				ctx,
				tx,
				t.SnapshotID,
				o.ChargeTenantID,
			); err != nil {
				return o, err
			}
			o.Command.Template = &t
			o.SnapshotID = t.SnapshotID
		}
		if c.Action == "fork" {
			count = c.Count
			if count < 1 || count > 32 {
				return o, api.Fail(api.Invalid, "fork count must be 1..32")
			}
			if c.CheckpointID != "" {
				cp, e := getCheckpointTx(ctx, tx, c.CheckpointID)
				if e != nil {
					return o, e
				}
				if !p.Root && cp.TenantID != p.TenantID {
					return o, api.Fail(api.NotFound, "not found")
				}
				source = Sandbox{
					TenantID:   cp.TenantID,
					SnapshotID: cp.SnapshotID,
					Shape:      cp.Shape,
					State:      "paused",
				}
				o.Step = "checkpoint_source"
			} else {
				source, err = getSandbox(ctx, tx, c.SandboxID)
				if err != nil {
					return o, err
				}
				if !visible(p, source.TenantID, source.ID) {
					return o, api.Fail(api.NotFound, "not found")
				}
				if source.State != "running" && source.State != "paused" {
					return o, api.Fail(
						api.Conflict,
						"sandbox cannot fork in %s",
						source.State,
					)
				}
			}
			if source.TenantID != p.TenantID {
				return o, api.Fail(api.NotFound, "not found")
			}
			o.ChargeTenantID = source.TenantID
			if err = requireSnapshotOwner(
				ctx,
				tx,
				source.SnapshotID,
				o.ChargeTenantID,
			); err != nil {
				return o, err
			}
			o.SnapshotID = source.SnapshotID
			t = Template{ID: source.TemplateID, Shape: source.Shape}
			if c.Env == nil {
				env = map[string]string{}
			}
			if c.Policy.Mode == "" {
				c.Policy = source.Policy
				o.Command.Policy = c.Policy
			}
		}
		if c.Action == "fork" && source.State == "running" {
			o.ReservedMiB = source.Shape.MemoryMiB + source.Shape.DiskMiB
		}
		sharedCost, e := quoteSnapshot(
			ctx,
			tx,
			o.ChargeTenantID,
			o.SnapshotID,
			t.Shape,
		)
		if e != nil {
			return o, e
		}
		var warm *resourceRecovery
		if c.Action == "claim" {
			warm, err = s.selectWarmTx(ctx, tx, t)
			if err != nil {
				return o, err
			}
		}
		diskCost := t.Shape.DiskMiB * int64(count)
		if warm != nil {
			diskCost = 0
		}
		if err = s.quota(
			ctx,
			tx,
			Principal{Root: p.Root, TenantID: o.ChargeTenantID},
			count,
			diskCost+o.ReservedMiB+sharedCost,
		); err != nil {
			return o, err
		}
		if err = quotaSnapshot(
			ctx,
			tx,
			o,
			o.SnapshotID,
			t.Shape,
			quotaRef{Kind: "engine_dependency", ID: "operation:" + o.ID},
		); err != nil {
			return o, err
		}
		if o.ReservedMiB > 0 {
			if err = reserveQuotaStep(
				ctx,
				tx,
				o,
				"fork-snapshot",
				source.ID,
				o.ReservedMiB,
			); err != nil {
				return o, err
			}
		}
		ttl, e := s.ttl(c.TTLSeconds)
		if e != nil {
			return o, e
		}
		expiry := c.OnExpire
		if expiry == "" {
			expiry = "kill"
		}
		if expiry != "kill" && expiry != "pause" {
			return o, api.Fail(api.Invalid, "on_expire must be kill or pause")
		}
		policy := c.Policy
		if policy.Mode == "" {
			policy.Mode = "relay"
		}
		if err = checkEgressPolicy(policy, len(secrets) > 0); err != nil {
			return o, err
		}
		for i := 0; i < count; i++ {
			b := Sandbox{
				ID:              newID("sbx_"),
				TenantID:        o.ChargeTenantID,
				State:           "provisioning",
				TemplateID:      t.ID,
				Shape:           t.Shape,
				TTLSeconds:      ttl,
				OnExpire:        expiry,
				AutoRenew:       c.AutoRenew,
				Env:             env,
				Policy:          policy,
				RequiresSecrets: len(secrets) > 0,
				Epoch:           newID("epoch_"),
				OperationID:     o.ID,
				CreatedAt:       time.Now().UTC(),
			}
			o.IDs = append(o.IDs, b.ID)
			if err = quotaSlot(ctx, tx, o, b); err != nil {
				return o, err
			}
			step := "provision"
			if c.Action == "fork" {
				step = "fork-clone:" + b.ID
			}
			if warm != nil {
				if err = s.claimWarmTx(ctx, tx, o, &b, t, *warm); err != nil {
					return o, err
				}
			} else if err = reserveQuotaStep(
				ctx,
				tx,
				o,
				step,
				b.ID,
				b.Shape.DiskMiB,
			); err != nil {
				return o, err
			}
			if err = saveSandbox(ctx, tx, b); err != nil {
				return o, err
			}
		}
	case "pause",
		"resume",
		"release",
		"renew",
		"checkpoint",
		"configure",
		"rollback":
		source, err = getSandbox(ctx, tx, c.SandboxID)
		if err != nil {
			return o, err
		}
		if !visible(p, source.TenantID, source.ID) {
			return o, api.Fail(api.NotFound, "not found")
		}
		if source.State == "terminated" && c.Action != "release" {
			return o, api.Fail(api.Conflict, "sandbox terminated")
		}
		o.ChargeTenantID = source.TenantID
		o.SnapshotID = source.SnapshotID
		if c.Action == "rollback" {
			cp, e := getCheckpointTx(ctx, tx, c.CheckpointID)
			if e != nil {
				return o, e
			}
			if cp.TenantID != source.TenantID || cp.SandboxID != source.ID {
				return o, api.Fail(api.NotFound, "not found")
			}
			o.SnapshotID = cp.SnapshotID
		}
		if c.Action == "renew" {
			if _, err = s.ttl(c.TTLSeconds); err != nil {
				return o, err
			}
		}
		if c.Action == "checkpoint" {
			if source.State != "running" && source.State != "paused" {
				return o, api.Fail(
					api.Conflict,
					"sandbox cannot checkpoint in %s",
					source.State,
				)
			}
			o.ReservedMiB = 0
			if source.State == "running" {
				o.ReservedMiB = source.Shape.MemoryMiB + source.Shape.DiskMiB
			}
			o.IDs = []string{newID("cp_")}
		}
		if c.Action == "pause" &&
			(source.State == "running" || (source.State == "pausing" && source.SnapshotID == "")) {
			o.ReservedMiB = source.Shape.MemoryMiB + source.Shape.DiskMiB
		}
		step := ""
		switch c.Action {
		case "checkpoint":
			if source.State == "running" {
				step = "checkpoint-capture"
			}
		case "pause":
			if source.State == "running" ||
				(source.State == "pausing" && source.SnapshotID == "") {
				step = "hibernate-snapshot"
			}
		case "resume":
			if source.State == "paused" || source.State == "resuming" {
				step = "resume-clone"
				o.ReservedMiB = source.Shape.DiskMiB
			}
		case "rollback":
			if source.State == "running" || source.State == "paused" ||
				source.State == "resuming" {
				step = "rollback-clone"
				if source.VMID != "" {
					step = "rollback-restore"
				}
				o.ReservedMiB = source.Shape.DiskMiB
			}
		}
		if o.ReservedMiB > 0 {
			if err = s.quota(
				ctx,
				tx,
				Principal{Root: p.Root, TenantID: o.ChargeTenantID},
				0,
				o.ReservedMiB,
			); err != nil {
				return o, err
			}
		}
		if step != "" {
			if err = reserveQuotaStep(
				ctx,
				tx,
				o,
				step,
				source.ID,
				o.ReservedMiB,
			); err != nil {
				return o, err
			}
		}
		if o.SnapshotID != "" &&
			(c.Action == "resume" || c.Action == "rollback" || c.Action == "checkpoint") {
			if e := requireSnapshotOwner(
				ctx,
				tx,
				o.SnapshotID,
				o.ChargeTenantID,
			); e != nil {
				return o, api.Fail(
					api.ManualRecovery,
					"snapshot responsibility has no trusted provenance",
				)
			}
			if err = putQuotaRef(
				ctx,
				tx,
				o.ChargeTenantID,
				"snapshot",
				o.SnapshotID,
				quotaRef{Kind: "engine_dependency", ID: "operation:" + o.ID},
			); err != nil {
				return o, err
			}
		}
		if c.Action == "configure" {
			if c.Env == nil {
				return o, api.Fail(
					api.Invalid,
					"env is required; an empty object clears environment",
				)
			}
			policy := c.Policy
			if policy.Mode == "" {
				policy = source.Policy
			}
			if err = checkEgressPolicy(policy, len(secrets) > 0); err != nil {
				return o, err
			}
			// The configuration fence is admitted atomically with the
			// operation. It blocks automatic resume/rebind until this
			// configure delivers, is replaced by a newer explicit configure,
			// or the sandbox is released.
			if err = recordConfigFence(
				ctx,
				tx,
				c.SandboxID,
				o.ID,
			); err != nil {
				return o, err
			}
		}
	case "delete_checkpoint":
		cp, e := getCheckpointTx(ctx, tx, c.CheckpointID)
		if e != nil {
			return o, e
		}
		if !p.Root && cp.TenantID != p.TenantID {
			return o, api.Fail(api.NotFound, "not found")
		}
		o.ChargeTenantID = cp.TenantID
	case "template_put", "template_build":
		if p.SandboxID != "" || c.Template == nil {
			return o, api.Fail(api.Invalid, "template required")
		}
		t := *c.Template
		if t.ID == "" || t.Shape.VCPUs < 1 || t.Shape.VCPUs > 255 ||
			t.Shape.MemoryMiB < 128 ||
			t.Shape.MemoryMiB > 1<<24 ||
			t.Shape.DiskMiB < 64 ||
			t.Shape.DiskMiB > 1<<30 ||
			t.TargetWarm < 0 ||
			t.TargetWarm > s.opts.MaxWarm {
			return o, api.Fail(
				api.Invalid,
				"invalid template shape or pool target",
			)
		}
		existing, e := getTemplateTx(ctx, tx, t.ID)
		if e != nil && !isCode(e, api.NotFound) {
			return o, e
		}
		owner, current, e := templateIntent(ctx, tx, t.ID)
		if e != nil {
			return o, e
		}
		// Foreign and missing names share one response; root must name the
		// owning tenant explicitly and can never transfer the reservation.
		if owner != "" && owner != p.TenantID ||
			owner == "" && existing.ID != "" &&
				existing.TenantID != p.TenantID {
			return o, api.Fail(api.NotFound, "not found")
		}
		if existing.ID != "" && (owner == "" || existing.TenantID != owner) {
			return o, api.Fail(
				api.ManualRecovery,
				"published template has missing or conflicting ownership provenance",
			)
		}
		if current != "" {
			var tenant, status string
			var body []byte
			e := tx.QueryRowContext(
				ctx,
				"SELECT tenant,status,body FROM operations WHERE id=?",
				current,
			).Scan(&tenant, &status, &body)
			if errors.Is(e, sql.ErrNoRows) {
				return o, api.Fail(
					api.ManualRecovery,
					"template intent references a missing operation",
				)
			}
			if e != nil {
				return o, e
			}
			var prior Operation
			if json.Unmarshal(body, &prior) != nil || prior.ID != current ||
				tenant != owner || prior.TenantID != owner || prior.Status != status ||
				(prior.Command.Action != "template_put" && prior.Command.Action != "template_build") ||
				prior.Command.Template == nil || prior.Command.Template.ID != t.ID ||
				prior.Command.Template.TenantID != owner {
				return o, api.Fail(
					api.ManualRecovery,
					"template intent operation has inconsistent provenance",
				)
			}
			switch status {
			case "pending", "running":
				return o, api.Fail(
					api.Conflict,
					"template publication is already in flight",
				)
			case "succeeded", "failed":
				// Recycle only a proven terminal pointer, retaining its result.
			default:
				return o, api.Fail(
					api.ManualRecovery,
					"template intent operation has an unknown status",
				)
			}
		}
		if !p.Root {
			target := 0
			if existing.ID != "" {
				target = existing.TargetWarm
			}
			if t.TargetWarm != target {
				return o, api.Fail(
					api.Conflict,
					"root access required to change pool target",
				)
			}
		}
		if p.TenantID == "" {
			return o, api.Fail(api.Invalid, "target tenant required")
		}
		// The first admission reserves the name permanently for this tenant
		// and commits the placeholder atomically with the operation.
		if owner == "" {
			owner = p.TenantID
		}
		if e = reserveTemplateIntent(ctx, tx, t.ID, owner, o.ID); e != nil {
			return o, e
		}
		t.TenantID = p.TenantID
		t.State = "building"
		if c.Action == "template_put" {
			t.State = "ready"
			if t.SnapshotID != "" {
				if p.TenantID == "" {
					return o, api.Fail(api.Invalid, "target tenant required")
				}
				if e := requireSnapshotOwner(
					ctx,
					tx,
					t.SnapshotID,
					p.TenantID,
				); e != nil {
					if !p.Root || !isCode(e, api.NotFound) {
						return o, e
					}
					if e = recordSnapshotOwner(
						ctx,
						tx,
						t.SnapshotID,
						p.TenantID,
						o.ID,
						"external_import",
					); e != nil {
						return o, e
					}
				}
				if e := recheckTemplateSource(
					ctx,
					tx,
					snapshotSource,
					t,
					p.TenantID,
				); e != nil {
					return o, e
				}
				if e := tx.QueryRowContext(ctx, `SELECT source_operation_id,source_kind FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=?`, t.SnapshotID, p.TenantID).
					Scan(&snapshotSource.SourceOperation, &snapshotSource.SourceKind); e != nil {
					return o, e
				}
			}
			if t.SnapshotID == "" && t.Image == "" {
				return o, api.Fail(
					api.Invalid,
					"template requires image or snapshot",
				)
			}
		}
		o.Command.Template = &t
		if c.Action == "template_build" {
			if err = s.quota(
				ctx,
				tx,
				p,
				1,
				2*t.Shape.DiskMiB+t.Shape.MemoryMiB,
			); err != nil {
				return o, err
			}
			if err = s.admitBuildTx(ctx, tx, o, t); err != nil {
				return o, err
			}
		} else {
			cost, e := quoteSnapshot(ctx, tx, p.TenantID, t.SnapshotID, t.Shape)
			if e != nil {
				return o, e
			}
			if t.SnapshotID != "" {
				if err = s.quota(ctx, tx, p, 0, cost); err != nil {
					return o, err
				}
				if err = quotaSnapshot(
					ctx,
					tx,
					o,
					t.SnapshotID,
					t.Shape,
					quotaRef{
						Kind: "engine_dependency",
						ID:   "operation:" + o.ID,
					},
				); err != nil {
					return o, err
				}
			}
			r := newAllocationGroup(
				o,
				allocationSubject{Kind: "build", ID: o.ID},
				t,
			)
			r.Group.SnapshotSource = snapshotSource
			if err = insertRecovery(ctx, tx, r); err != nil {
				return o, err
			}
		}
	default:
		return o, api.Fail(api.Invalid, "unsupported action %q", c.Action)
	}
	if err = saveOp(ctx, tx, o); err != nil {
		return o, err
	}
	if err = tx.Commit(); err != nil {
		return o, fmt.Errorf("operation admission commit: %w", err)
	}
	if c.Action == "configure" {
		// Revoke any binding published by a worker before this admission.
		// The durable fence prevents that worker from publishing again.
		s.closeConnections(c.SandboxID)
		_ = s.revoke(c.SandboxID)
	}
	s.mu.Lock()
	for _, id := range o.IDs {
		if strings.HasPrefix(id, "sbx_") {
			s.secrets[id] = secrets
		}
	}
	if c.Action == "configure" {
		s.opSecrets[o.ID] = secrets
	}
	s.mu.Unlock()
	s.start(o)
	return o, nil
}
