package sandboxd

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) activateWarm(o Operation, b *Sandbox) error {
	target := &activationTarget{
		Origin:   o.ID,
		Kind:     "selection",
		VM:       b.VMID,
		Instance: b.Instance,
	}
	if target.Instance == "" {
		return api.Fail(
			api.ManualRecovery,
			"warm selection lacks successful generation witness",
		)
	}
	o.Origin = o.ID
	o.ID += ":warm"
	return s.activateTarget(o, b, true, target)
}

func (s *Service) discardWarm(o Operation, b *Sandbox, t Template) error {
	if s.opts.Egress != nil {
		if err := s.revoke(b.ID); err != nil {
			return err
		}
	}
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		return err
	}
	if r.Group == nil || r.Group.Selection == nil {
		return api.Fail(api.ManualRecovery, "rejected warm has no selection")
	}
	selection := r.Group.Selection
	if selection.Stage == "fallback_reserved" {
		return nil
	}
	i := selection.Identity
	if b.Instance != "" && b.Instance != i.Instance {
		return api.Fail(api.Conflict, "warm selection instance changed")
	}
	if i.Instance == "" || i.Generation == nil ||
		i.Generation.Status != "succeeded" ||
		i.Generation.ResultInstance != i.Instance {
		return api.Fail(
			api.ManualRecovery,
			"rejected warm lacks successful witness",
		)
	}
	v, err := s.opts.Engine.GetVM(s.ctx, i.VM)
	absent := isCode(err, api.NotFound)
	if err != nil && !absent {
		return err
	}
	if !absent {
		if v.ID != i.VM || v.Instance != i.Instance || !v.Verified ||
			v.OperationID != "" ||
			v.Config.Shape != i.Shape {
			return api.Fail(
				api.ManualRecovery,
				"rejected warm instance changed",
			)
		}
		ownership := s.lockResourceOwnership()
		tx, e := s.store.db.BeginTx(s.ctx, nil)
		if e == nil {
			latest, le := loadRecoveryTx(s.ctx, tx, r.ID)
			e = le
			if e == nil {
				q := latest.Group.Selection
				if q.Deletion.Dispatched {
					// Receipt reconciliation happens outside ownership and SQL.
				} else {
					q.Stage = "rejected"
					q.Deletion = allocationStep{
						Request: api.Request{
							Key:              stepKey(o, "warm-reject:"+b.ID),
							Action:           "delete",
							VMID:             i.VM,
							ExpectedInstance: i.Instance,
						},
						Bound:      true,
						Dispatched: true,
					}
				}
				e = writeRecovery(s.ctx, tx, latest)
				r = latest
			}
			if e == nil {
				e = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
		}
		ownership()
		if e != nil {
			return e
		}
		q := r.Group.Selection
		if q.Deletion.Dispatched {
			proof, e := s.opts.Engine.FindOperation(
				s.ctx,
				q.Deletion.Request.Key,
			)
			if e != nil && !isCode(e, api.NotFound) {
				return e
			}
			if e == nil {
				if !reflect.DeepEqual(proof.Request, q.Deletion.Request) {
					return api.Fail(
						api.ManualRecovery,
						"warm delete receipt changed",
					)
				}
				if proof.Status == "failed" {
					ownership = s.lockResourceOwnership()
					tx, e = s.store.db.BeginTx(s.ctx, nil)
					if e == nil {
						latest, le := loadRecoveryTx(s.ctx, tx, r.ID)
						e = le
						if e == nil {
							next := latest.Group.Selection
							next.History = append(
								next.History,
								cleanupAttempt{
									Request: next.Deletion.Request,
									Receipt: proof,
								},
							)
							next.Attempt++
							next.Deletion.Request.Key = stepKey(
								Operation{
									ID: o.ID + ":warm-delete:" + fmt.Sprint(
										next.Attempt,
									),
								},
								"warm-reject:"+b.ID,
							)
							e = writeRecovery(s.ctx, tx, latest)
							r = latest
						}
						if e == nil {
							e = tx.Commit()
						} else {
							_ = tx.Rollback()
						}
					}
					ownership()
					if e != nil {
						return e
					}
				}
			}
		}
		if _, err = api.Wait(
			s.ctx,
			s.opts.Engine,
			r.Group.Selection.Deletion.Request,
		); err != nil {
			return err
		}
		_, err = s.opts.Engine.GetVM(s.ctx, i.VM)
		if !isCode(err, api.NotFound) {
			if err != nil {
				return err
			}
			return api.Fail(
				api.ManualRecovery,
				"warm deletion did not prove absence",
			)
		}
	}
	return s.transferWarmFallback(o, b, t)
}

// The old disk and replacement reservation change in one transaction. Quota
// competitors never observe refunded storage between these responsibilities.
func (s *Service) transferWarmFallback(
	o Operation,
	b *Sandbox,
	t Template,
) error {
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
	q := r.Group.Selection
	if q.Stage == "fallback_reserved" || q.Stage == "cleanup_done" {
		current, e := getSandbox(s.ctx, tx, b.ID)
		if e == nil {
			*b = current
		}
		return e
	}
	current, err := getSandbox(s.ctx, tx, b.ID)
	if err != nil {
		return err
	}
	i := q.Identity
	if current.VMID != i.VM || current.TenantID != r.TenantID ||
		current.Shape != i.Shape ||
		current.Instance != i.Instance ||
		i.Generation == nil ||
		i.Generation.ResultInstance != i.Instance ||
		i.Instance == "" {
		return api.Fail(
			api.ManualRecovery,
			"fallback lost fixed selection identity",
		)
	}
	if err = deleteQuotaRows(
		s.ctx,
		tx,
		`FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND holder_id=?`,
		r.TenantID,
		"backing:"+i.VM,
	); err != nil {
		return err
	}
	if err = deleteQuotaRows(
		s.ctx,
		tx,
		`FROM quota_resources WHERE tenant_id=? AND kind='vm_disk' AND (resource_key=? OR EXISTS(SELECT 1 FROM quota_refs q WHERE q.tenant_id=quota_resources.tenant_id AND q.kind=quota_resources.kind AND q.resource_key=quota_resources.resource_key AND q.holder_id=?))`,
		r.TenantID,
		i.VM,
		"vm:"+i.VM,
	); err != nil {
		return err
	}
	current.VMID = ""
	current.Instance = ""
	current.ProvisionSource = "golden"
	if t.SnapshotID == "" {
		current.ProvisionSource = "cold"
	}
	if current.State == "terminated" {
		q.Stage = "cleanup_done"
		r.Done = groupStepsTerminal(r)
		if err = writeRecovery(s.ctx, tx, r); err != nil {
			return err
		}
		if err = deleteQuotaRows(
			s.ctx,
			tx,
			`FROM quota_refs WHERE tenant_id=? AND holder_kind='engine_dependency' AND holder_id=?`,
			r.TenantID,
			"operation:"+r.Source,
		); err != nil {
			return err
		}
		if err = quotaTerminate(s.ctx, tx, current); err != nil {
			return err
		}
	} else {
		if current.State != "provisioning" || current.OperationID != o.ID {
			return api.Fail(api.Conflict, "fallback lifecycle changed")
		}
		q.Stage = "fallback_reserved"
		request := api.Request{Action: "clone", SnapshotID: t.SnapshotID}
		if t.SnapshotID == "" {
			request = api.Request{
				Action: "create",
				Config: &api.Config{
					Image: t.Image,
					Shape: t.Shape,
					Labels: map[string]string{
						"sandboxd":   "sandbox",
						"sandbox_id": b.ID,
					},
				},
			}
		}
		if err = addGroupAllocation(
			s.ctx,
			tx,
			&r,
			o,
			"provision",
			"vm_disk",
			i.Shape.DiskMiB,
			request,
			true,
		); err != nil {
			return err
		}
	}
	if err = saveSandbox(s.ctx, tx, current); err != nil {
		return err
	}
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*b = current
	return nil
}

func (s *Service) resolveWarmSelection(o Operation, r *resourceRecovery) error {
	q := r.Group.Selection
	if q.Stage == "active" {
		return nil
	}
	if q.Stage == "cleanup_done" {
		if !groupStepsTerminal(*r) {
			return nil
		}
		ownership := s.lockResourceOwnership()
		defer ownership()
		tx, err := s.store.db.BeginTx(s.ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		latest, err := loadRecoveryTx(s.ctx, tx, r.ID)
		if err != nil {
			return err
		}
		if !groupStepsTerminal(latest) {
			return nil
		}
		b, err := getSandbox(s.ctx, tx, latest.Group.Subject.ID)
		if err != nil {
			return err
		}
		if b.State != "terminated" || b.VMID != "" ||
			b.TenantID != latest.TenantID {
			return api.Fail(
				api.ManualRecovery,
				"completed warm cleanup lost its sandbox destination",
			)
		}
		latest.Done = true
		if err = writeRecovery(s.ctx, tx, latest); err != nil {
			return err
		}
		if err = quotaTerminate(s.ctx, tx, b); err != nil {
			return err
		}
		return tx.Commit()
	}
	b, err := getSandbox(s.ctx, s.store.db, r.Group.Subject.ID)
	if err != nil {
		return err
	}
	if !recoveryTerminal(o.Status) && b.State != "terminated" {
		return nil
	}
	if o.Status == "failed" && b.State != "terminated" {
		b.State = "terminated"
		if err = s.saveQuotaSandbox(b); err != nil {
			return err
		}
	}
	return s.discardWarm(o, &b, r.Group.Template)
}

func (s *Service) releaseAllocation(b *Sandbox) (bool, error) {
	var raw []byte
	err := s.store.db.QueryRowContext(s.ctx, `SELECT body FROM resource_recovery WHERE json_extract(body,'$.kind')='allocation_group' AND json_extract(body,'$.allocation_group.subject.kind')='sandbox' AND json_extract(body,'$.allocation_group.subject.id')=? ORDER BY id LIMIT 1`, b.ID).
		Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	var r resourceRecovery
	if err = json.Unmarshal(raw, &r); err != nil {
		return true, err
	}
	for _, a := range r.Allocations {
		if a.Kind == "vm_disk" && a.Resolution.State == "active" &&
			a.Resolution.Destination == "sandbox:"+b.ID &&
			r.TenantID == b.TenantID &&
			r.Group.Subject.ID == b.ID {
			return false, nil
		}
	}
	if r.Done && !hasUnresolvedAllocations(r) && b.State == "terminated" &&
		b.VMID == "" {
		return false, nil
	}
	if r.Group.Selection == nil || r.Group.Selection.Stage == "active" ||
		r.Group.Selection.Stage == "cleanup_done" {
		return false, nil
	}
	b.State = "terminated"
	if err = s.saveQuotaSandbox(*b); err != nil {
		return true, err
	}
	if r.Group.Selection.Stage == "fallback_reserved" {
		return true, api.Fail(
			api.ManualRecovery,
			"replacement allocation still requires original receipt resolution",
		)
	}
	o, err := s.store.operation(s.ctx, r.Source)
	if err != nil {
		return true, err
	}
	return true, s.discardWarm(o, b, r.Group.Template)
}
