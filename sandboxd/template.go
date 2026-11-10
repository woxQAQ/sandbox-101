package sandboxd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) template(o *Operation) error {
	t := *o.Command.Template
	unlock := s.lock("template:" + t.ID)
	defer unlock()
	owner, pointer, intentErr := templateIntent(s.ctx, s.store.db, t.ID)
	if intentErr != nil {
		return intentErr
	}
	if owner == "" {
		return Fail(
			ErrManualRecovery,
			"template ownership placeholder is missing",
		)
	}
	if owner != t.TenantID || pointer != o.ID {
		return Fail(ErrConflict, "template publication was superseded")
	}
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
			start, startErr := s.engine(
				*o,
				"build-start",
				api.Request{Action: "start", VMID: v.VMID},
			)
			if startErr != nil {
				return startErr
			}
			if start.Status != "succeeded" || start.ResultInstance == "" {
				return Fail(
					ErrManualRecovery,
					"build start has no successful generation witness",
				)
			}
			vm, err := s.opts.Engine.GetVM(s.ctx, v.VMID)
			if err != nil {
				return engineFailure(err)
			}
			group, e := s.loadRecovery(allocationGroupID(*o))
			if e != nil {
				return e
			}
			if e = validAllocationIdentity(group, vm); e != nil {
				return e
			}
			if err = s.opts.Agent.Info(s.ctx, vm); err != nil {
				return err
			}
			if t.Warmup != "" {
				// Guest execution results cannot be inferred after a crash. Record its start before dispatch.
				if o.Step == "warmup_inflight" {
					return Fail(
						ErrUnknown,
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
		if err = s.prepareBuildRetirement(*o); err != nil {
			return err
		}
		group, err := s.loadRecovery(allocationGroupID(*o))
		if err != nil {
			return err
		}
		if group.Group.Identity == nil {
			return Fail(ErrManualRecovery, "build identity is missing")
		}
		vm, lookupErr := s.opts.Engine.GetVM(s.ctx, group.Group.Identity.VM)
		if isEngineCode(lookupErr, api.NotFound) {
			err = s.retireAllocationAbsent(group.ID)
		} else if lookupErr != nil {
			return lookupErr
		} else {
			collector, e := s.handoffAllocationCleanup(group.ID, vm, false)
			if e != nil {
				return e
			}
			err = s.consumeAllocationCleanup(collector)
		}
		if err != nil {
			return err
		}
	}
	// Engine inspection stays outside the ownership guard. The admitted
	// operation retains the snapshot until its catalog reference is published.
	if t.SnapshotID != "" {
		snap, err := s.opts.Engine.GetSnapshot(s.ctx, t.SnapshotID)
		if err != nil {
			return engineFailure(err)
		}
		if snap.ID != t.SnapshotID || !snap.Published || snap.Deleting ||
			snap.Config.Shape != t.Shape {
			return Fail(
				ErrIncompatible,
				"template snapshot shape or publication does not match",
			)
		}
		group, e := s.findAllocationGroup(*o)
		if e != nil {
			return e
		}
		if group != nil && group.Group.SnapshotSource != nil &&
			group.Group.SnapshotSource.Config != nil &&
			!bytes.Equal(
				encode(snap.Config),
				encode(*group.Group.SnapshotSource.Config),
			) {
			return Fail(
				ErrIncompatible,
				"snapshot Config differs from frozen source quote",
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
	pool := s.lock("pool:" + t.ID)
	defer pool()
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	previous, previousErr := getTemplateTx(s.ctx, tx, t.ID)
	if previousErr != nil && !isCode(previousErr, ErrNotFound) {
		return previousErr
	}
	if previous.ID != "" && previous.SnapshotID != "" &&
		previous.SnapshotID != t.SnapshotID {
		if _, err = snapshotCollector(
			s.ctx,
			tx,
			previous.TenantID,
			previous.SnapshotID,
			o.ID,
		); err != nil {
			return err
		}
		if err = deleteQuotaRows(
			s.ctx,
			tx,
			`FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`,
			previous.TenantID,
			previous.SnapshotID,
			"template:"+t.ID,
		); err != nil {
			return err
		}
	}
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
		return Fail(
			ErrManualRecovery,
			"template ownership placeholder is missing",
		)
	}
	if owner != t.TenantID {
		return Fail(
			ErrConflict,
			"template ownership does not match this operation",
		)
	}
	if current != o.ID {
		return Fail(
			ErrConflict,
			"template publication was superseded",
		)
	}
	result, err := tx.ExecContext(
		s.ctx,
		"INSERT INTO templates(id,tenant,body) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET tenant=excluded.tenant,body=excluded.body",
		t.ID,
		t.TenantID,
		encode(t),
	)
	if err = requireSQLWrite(result, err); err != nil {
		return err
	}
	if t.SnapshotID != "" {
		if err = putQuotaRef(
			s.ctx,
			tx,
			t.TenantID,
			"snapshot",
			t.SnapshotID,
			quotaRef{Kind: "active_use", ID: "template:" + t.ID},
		); err != nil {
			return err
		}
	}
	group, err := loadRecoveryTx(s.ctx, tx, allocationGroupID(*o))
	if err != nil {
		return err
	}
	for index := range group.Allocations {
		a := &group.Allocations[index]
		if a.Kind == "snapshot" && a.Resolution.Proof != nil &&
			a.Resolution.Proof.SnapshotID == t.SnapshotID {
			a.Resolution.State = "active"
			a.Resolution.Destination = "template:" + t.ID + ":" + o.ID + ":" + t.SnapshotID
		}
	}
	group.Done = true
	if err = writeRecovery(s.ctx, tx, group); err != nil {
		return err
	}
	if err = deleteQuotaRows(
		s.ctx,
		tx,
		`FROM quota_refs WHERE tenant_id=? AND holder_kind='engine_dependency' AND holder_id=?`,
		t.TenantID,
		"operation:"+o.ID,
	); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("template publication commit: %w", err)
	}
	o.Result.Template = &t
	return nil
}

func (s *Service) admitBuildTx(
	ctx context.Context,
	tx *sql.Tx,
	o Operation,
	t Template,
) error {
	r := newAllocationGroup(o, allocationSubject{Kind: "build", ID: o.ID}, t)
	if err := putQuotaResource(
		ctx,
		tx,
		quotaResource{
			Tenant: o.ChargeTenantID,
			Kind:   "build_slot",
			Key:    o.ID,
			Slots:  1,
			Source: o.ID,
		},
		quotaRef{Kind: "active_use", ID: groupHolder(r)},
	); err != nil {
		return err
	}
	request := api.Request{
		Action: "create",
		Config: &api.Config{
			Image: t.Image,
			Shape: t.Shape,
			Labels: map[string]string{
				"sandboxd":     "build",
				"operation_id": o.ID,
			},
		},
	}
	if err := addGroupAllocation(
		ctx,
		tx,
		&r,
		o,
		"build-create",
		"vm_disk",
		t.Shape.DiskMiB,
		request,
		true,
	); err != nil {
		return err
	}
	if err := addGroupAllocation(
		ctx,
		tx,
		&r,
		o,
		"build-snapshot",
		"snapshot",
		t.Shape.DiskMiB+t.Shape.MemoryMiB,
		api.Request{Action: "snapshot"},
		false,
	); err != nil {
		return err
	}
	return insertRecovery(ctx, tx, r)
}

// Full shape is obtained from a durable capture or import, before admission's
// write lock. Authorization is checked before reporting missing provenance.
func groupSnapshotSource(r resourceRecovery, sid string) bool {
	if r.Group == nil {
		return false
	}
	if r.Group.Template.SnapshotID == sid {
		return true
	}
	for _, a := range r.Allocations {
		if a.Kind == "snapshot" && a.Resolution.Proof != nil &&
			a.Resolution.Proof.SnapshotID == sid {
			return true
		}
	}
	return false
}

func (s *Service) templateSource(
	ctx context.Context,
	p Principal,
	t Template,
) (*templateSnapshotSource, error) {
	ownerErr := requireSnapshotOwner(ctx, s.store.db, t.SnapshotID, p.TenantID)
	if ownerErr != nil && (!p.Root || !isCode(ownerErr, ErrNotFound)) {
		return nil, ownerErr
	}
	var foreign int
	if err := s.store.db.QueryRowContext(ctx, `SELECT count(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id!=?`, t.SnapshotID, p.TenantID).
		Scan(&foreign); err != nil {
		return nil, err
	}
	if foreign != 0 {
		return nil, Fail(ErrNotFound, "not found")
	}
	quote := &templateSnapshotSource{
		SnapshotID: t.SnapshotID,
		Owner:      p.TenantID,
		Shape:      t.Shape,
	}
	var raw []byte
	err := s.store.db.QueryRowContext(ctx, `SELECT 'checkpoint',id,body FROM checkpoints WHERE tenant=? AND json_extract(body,'$.snapshot_id')=? UNION ALL SELECT 'sleep',id,body FROM sandboxes WHERE tenant=? AND json_extract(body,'$.snapshot_id')=? LIMIT 1`, p.TenantID, t.SnapshotID, p.TenantID, t.SnapshotID).
		Scan(&quote.Kind, &quote.ID, &raw)
	if err == nil {
		var source struct {
			Shape api.Shape `json:"shape"`
		}
		if err = json.Unmarshal(raw, &source); err != nil {
			return nil, err
		}
		if source.Shape != t.Shape {
			return nil, Fail(
				ErrConflict,
				"template shape differs from trusted snapshot",
			)
		}
		return quote, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	groups, err := s.resourceRecoveries()
	if err != nil {
		return nil, err
	}
	for _, r := range groups {
		if r.TenantID != p.TenantID || !groupSnapshotSource(r, t.SnapshotID) {
			continue
		}
		if r.Group.Template.Shape != t.Shape {
			return nil, Fail(
				ErrConflict,
				"template shape differs from frozen capture",
			)
		}
		quote.Kind = "group"
		quote.ID = r.ID
		if r.Group.SnapshotSource != nil &&
			r.Group.SnapshotSource.Config != nil {
			config := *r.Group.SnapshotSource.Config
			config.Labels = maps.Clone(config.Labels)
			quote.Config = &config
		}
		if quote.Config == nil && r.Group.Identity != nil &&
			r.Group.Identity.Receipt.Request.Config != nil {
			config := *r.Group.Identity.Receipt.Request.Config
			config.Labels = maps.Clone(config.Labels)
			quote.Config = &config
		}
		return quote, nil
	}
	if !p.Root {
		return nil, Fail(
			ErrManualRecovery,
			"snapshot has no trusted full shape",
		)
	}
	snap, err := s.opts.Engine.GetSnapshot(ctx, t.SnapshotID)
	if err != nil {
		return nil, engineFailure(err)
	}
	if snap.ID != t.SnapshotID || !snap.Published || snap.Deleting ||
		snap.Config.Shape != t.Shape {
		return nil, Fail(
			ErrIncompatible,
			"import snapshot shape or publication differs",
		)
	}
	quote.Kind = "external"
	quote.ID = snap.ID
	config := snap.Config
	config.Labels = maps.Clone(config.Labels)
	quote.Config = &config
	return quote, nil
}

func recheckTemplateSource(
	ctx context.Context,
	q executor,
	source *templateSnapshotSource,
	t Template,
	owner string,
) error {
	if source == nil || source.Owner != owner ||
		source.SnapshotID != t.SnapshotID ||
		source.Shape != t.Shape {
		return Fail(
			ErrManualRecovery,
			"template source quote differs from admission",
		)
	}
	if source.Config != nil && source.Config.Shape != t.Shape {
		return Fail(
			ErrManualRecovery,
			"template source Config differs from its shape",
		)
	}
	var raw []byte
	var err error
	switch source.Kind {
	case "checkpoint":
		err = q.QueryRowContext(ctx, `SELECT body FROM checkpoints WHERE id=? AND tenant=?`, source.ID, owner).
			Scan(&raw)
	case "sleep":
		err = q.QueryRowContext(ctx, `SELECT body FROM sandboxes WHERE id=? AND tenant=?`, source.ID, owner).
			Scan(&raw)
	case "group":
		var r resourceRecovery
		r, err = loadRecoveryTx(ctx, q, source.ID)
		if err == nil &&
			(r.TenantID != owner || !groupSnapshotSource(r, t.SnapshotID) || r.Group.Template.Shape != t.Shape) {
			err = Fail(ErrConflict, "frozen snapshot source changed")
		}
		if err == nil {
			err = validateGroupSource(ctx, q, r)
		}
		return err
	case "external":
		if source.ID != t.SnapshotID || source.Config == nil {
			return Fail(
				ErrManualRecovery,
				"external snapshot quote is incomplete",
			)
		}
		return nil
	default:
		return Fail(ErrManualRecovery, "unknown template source")
	}
	if errors.Is(err, sql.ErrNoRows) {
		return Fail(
			ErrConflict,
			"template source retired during admission",
		)
	}
	if err != nil {
		return err
	}
	var current struct {
		TenantID   string    `json:"tenant_id"`
		SnapshotID string    `json:"snapshot_id"`
		Shape      api.Shape `json:"shape"`
	}
	if err = json.Unmarshal(raw, &current); err != nil {
		return err
	}
	if current.TenantID != owner || current.SnapshotID != source.SnapshotID ||
		current.Shape != source.Shape {
		return Fail(
			ErrConflict,
			"template source changed during admission",
		)
	}
	return nil
}

func (s *Service) prepareBuildRetirement(o Operation) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	r, err := loadRecoveryTx(s.ctx, tx, allocationGroupID(o))
	if err != nil {
		return err
	}
	for index := range r.Allocations {
		a := &r.Allocations[index]
		if a.Kind != "snapshot" || a.Resolution.delivered() {
			continue
		}
		if a.Resolution.Proof == nil ||
			a.Resolution.Proof.Status != "succeeded" ||
			a.Resolution.Proof.SnapshotID == "" {
			return Fail(
				ErrManualRecovery,
				"build capture has not delivered its identity",
			)
		}
		c, e := snapshotCollector(
			s.ctx,
			tx,
			r.TenantID,
			a.Resolution.Proof.SnapshotID,
			r.Source,
		)
		if e != nil {
			return e
		}
		a.Resolution.State = "cleanup"
		a.Resolution.Destination = c.ID
		if err = putQuotaRef(
			s.ctx,
			tx,
			r.TenantID,
			"snapshot",
			a.Resolution.Proof.SnapshotID,
			quotaRef{Kind: "engine_dependency", ID: "operation:" + o.ID},
		); err != nil {
			return err
		}
	}
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return err
	}
	return tx.Commit()
}

// allocationGroup freezes the original intent independently of public results.
// RecoveryGate limits actions; Resolution alone records responsibility delivery.
type templateSnapshotSource struct {
	Kind            string      `json:"kind"`
	ID              string      `json:"id"`
	SnapshotID      string      `json:"snapshot_id"`
	Owner           string      `json:"owner"`
	Shape           api.Shape   `json:"shape"`
	Config          *api.Config `json:"config,omitempty"`
	SourceOperation string      `json:"source_operation"`
	SourceKind      string      `json:"source_kind"`
}

func (s *Service) Templates(
	ctx context.Context,
	p Principal,
) ([]Template, error) {
	if p.SandboxID != "" {
		return nil, Fail(ErrConflict, "tenant access required")
	}
	all, err := s.store.templates(ctx)
	if err != nil {
		return nil, err
	}
	out := []Template{}
	for _, t := range all {
		if p.Root || t.TenantID == p.TenantID {
			out = append(out, t)
		}
	}
	return out, nil
}

func getTemplateTx(
	ctx context.Context,
	e executor,
	id string,
) (Template, error) {
	var b []byte
	err := e.QueryRowContext(ctx, "SELECT body FROM templates WHERE id=?", id).
		Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return Template{}, Fail(ErrNotFound, "not found")
	}
	var t Template
	if err == nil {
		err = json.Unmarshal(b, &t)
	}
	return t, err
}

// templateIntent returns the permanent owner of a template name and the
// operation currently holding its publication pointer. An empty owner means
// the name has never been admitted; an empty operation means no publication
// is in flight. Ownership is never released by failure or restart.
func templateIntent(
	ctx context.Context,
	e executor,
	id string,
) (owner, operation string, err error) {
	err = e.QueryRowContext(
		ctx,
		"SELECT tenant_id,operation_id FROM template_intents WHERE template_id=?",
		id,
	).Scan(&owner, &operation)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	return owner, operation, err
}

// reserveTemplateIntent records first ownership or moves the publication
// pointer. Every caller serializes its check and this write in one
// transaction under the resource-ownership lock, so the conditional claim is
// the preceding read; an existing row's ownership is never rewritten here.
func reserveTemplateIntent(
	ctx context.Context,
	e executor,
	id, tenant, operation string,
) error {
	result, err := e.ExecContext(
		ctx,
		"INSERT INTO template_intents(template_id,tenant_id,operation_id) VALUES(?,?,?) ON CONFLICT(template_id) DO UPDATE SET operation_id=excluded.operation_id",
		id,
		tenant,
		operation,
	)
	if err = requireSQLWrite(result, err); err != nil {
		return err
	}
	owner, current, err := templateIntent(ctx, e, id)
	if err == nil && (owner != tenant || current != operation) {
		return Fail(ErrManualRecovery, "template intent was not persisted")
	}
	return err
}

func (s *store) templates(ctx context.Context) ([]Template, error) {
	rows, err := s.db.QueryContext(
		ctx,
		"SELECT body FROM templates ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Template{}
	for rows.Next() {
		var b []byte
		var t Template
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *store) template(ctx context.Context, id string) (Template, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT body FROM templates WHERE id=?", id).
		Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Template{}, Fail(ErrNotFound, "not found")
	}
	var t Template
	if err == nil {
		err = json.Unmarshal(raw, &t)
	}
	return t, err
}

// Template describes a tenant's snapshot source and warm-pool target.
type Template struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	Image      string    `json:"image"`
	Shape      api.Shape `json:"shape"`
	SnapshotID string    `json:"snapshot_id,omitempty"`
	State      string    `json:"state"`
	TargetWarm int       `json:"target_warm"`
	Warmup     string    `json:"warmup,omitempty"`
}
