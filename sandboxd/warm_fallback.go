package sandboxd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/sandbox-engine/api"
)

// validateWarmSelection follows the original refill, even after the claim has
// delivered a different replacement generation. Only frozen receipts authorize
// cleanup; current Engine state never repairs a persisted request.
func validateWarmSelection(
	ctx context.Context,
	e executor,
	r resourceRecovery,
) error {
	if r.Group == nil || r.Group.Selection == nil {
		return nil
	}
	bad := func() error {
		return api.Fail(
			api.ManualRecovery,
			"warm selection differs from its frozen responsibility",
		)
	}
	g, q := r.Group, r.Group.Selection
	if r.Kind != "allocation_group" || r.ID != "allocation:"+r.Source ||
		r.Source == "" ||
		g.Subject.Kind != "sandbox" ||
		g.Subject.ID == "" ||
		r.SandboxID != g.Subject.ID ||
		r.TenantID == "" ||
		g.Template.TenantID != r.TenantID {
		return bad()
	}
	if err := validateGroupSource(ctx, e, r); err != nil {
		return errors.Join(bad(), err)
	}
	b, err := getSandbox(ctx, e, g.Subject.ID)
	if err != nil {
		return errors.Join(bad(), err)
	}
	if b.TenantID != r.TenantID || b.Shape != g.Template.Shape ||
		b.TemplateID != g.Template.ID {
		return bad()
	}
	var raw []byte
	if err = e.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE id=?`, q.Refill).
		Scan(&raw); err != nil {
		return errors.Join(bad(), err)
	}
	var origin resourceRecovery
	if err = json.Unmarshal(raw, &origin); err != nil {
		return errors.Join(bad(), err)
	}
	if origin.Kind != "allocation_group" || origin.Source == "" ||
		origin.ID != "allocation:"+origin.Source ||
		q.Refill != origin.ID ||
		origin.TenantID != r.TenantID ||
		origin.Group == nil ||
		origin.Group.Subject != (allocationSubject{Kind: "warm", ID: origin.Source}) ||
		origin.Group.Selection != nil ||
		origin.Group.Identity == nil ||
		origin.Group.Gate != nil ||
		!reflect.DeepEqual(*origin.Group.Identity, q.Identity) ||
		origin.Group.Template.ID != g.Template.ID ||
		origin.Group.Template.TenantID != r.TenantID ||
		origin.Group.Template.Shape != g.Template.Shape ||
		origin.Group.Template.SnapshotID != g.Template.SnapshotID ||
		origin.Group.Template.Image != g.Template.Image ||
		len(origin.Allocations) != 1 {
		return bad()
	}
	if err = validateGroupSource(ctx, e, origin); err != nil {
		return errors.Join(bad(), err)
	}
	if err = e.QueryRowContext(ctx, `SELECT body FROM operations WHERE id=?`, origin.Source).
		Scan(&raw); err != nil {
		return errors.Join(bad(), err)
	}
	var refill Operation
	if err = json.Unmarshal(raw, &refill); err != nil {
		return errors.Join(bad(), err)
	}
	if refill.Command.Action != "refill" ||
		refill.Command.TemplateID != g.Template.ID {
		return bad()
	}
	if err = validateAllocationProvenance(origin); err != nil {
		return err
	}
	i, a := q.Identity, origin.Allocations[0]
	if i.Owner != r.TenantID || i.Shape != g.Template.Shape ||
		i.Source != origin.Source ||
		i.Instance == "" ||
		i.Generation == nil ||
		i.Generation.VMID != i.VM ||
		i.Generation.Status != "succeeded" ||
		i.Generation.ResultInstance != i.Instance ||
		i.Receipt.Status != "succeeded" ||
		i.Receipt.Key != i.Receipt.Request.Key ||
		(i.Receipt.Request.Action != "create" && i.Receipt.Request.Action != "clone") ||
		(i.Generation.Request.Action != "start" && i.Generation.Request.Action != "clone") ||
		a.OperationID != origin.Source ||
		a.TenantID != refill.TenantID ||
		a.ChargeTenantID != r.TenantID ||
		a.Subject != origin.Group.Subject ||
		a.Shape != i.Shape ||
		a.Kind != "vm_disk" ||
		!a.RequestBound ||
		a.Resolution.State != "active" ||
		a.Resolution.Destination != "sandbox:"+b.ID ||
		a.Resolution.Proof == nil ||
		!reflect.DeepEqual(*a.Resolution.Proof, i.Receipt) ||
		!reflect.DeepEqual(a.Request, i.Receipt.Request) ||
		a.Request.Key != stepKey(refill, a.Step) {
		return bad()
	}
	for name, step := range origin.Group.Steps {
		if !step.Bound || !step.Dispatched ||
			step.Request.Key != stepKey(refill, name) ||
			step.Proof == nil ||
			step.Proof.ID == "" ||
			step.Proof.Key != step.Request.Key ||
			step.Proof.Status != "succeeded" ||
			step.Proof.Error != nil ||
			!reflect.DeepEqual(step.Proof.Request, step.Request) {
			return bad()
		}
	}
	if i.Generation.Error != nil || i.Receipt.Error != nil {
		return bad()
	}
	if i.Receipt.Request.Action == "clone" {
		expected := api.Request{
			Key:        stepKey(refill, a.Step),
			Action:     "clone",
			SnapshotID: g.Template.SnapshotID,
		}
		if g.Template.SnapshotID == "" || len(origin.Group.Steps) != 1 ||
			!reflect.DeepEqual(a.Request, expected) ||
			!reflect.DeepEqual(i.Receipt, *i.Generation) {
			return bad()
		}
	} else if len(origin.Group.Steps) != 2 ||
		i.Receipt.Request.Config == nil ||
		i.Receipt.Request.Config.Image != g.Template.Image ||
		!reflect.DeepEqual(
			i.Receipt.Request,
			api.Request{
				Key:    i.Receipt.Key,
				Action: "create",
				Config: i.Receipt.Request.Config,
			},
		) ||
		!reflect.DeepEqual(
			i.Generation.Request,
			api.Request{Key: i.Generation.Key, Action: "start", VMID: i.VM},
		) ||
		g.Template.SnapshotID != "" {
		return bad()
	}
	if uint64(len(q.History)) != q.Attempt {
		return bad()
	}
	switch q.Stage {
	case "selected", "active":
		if r.Done != (q.Stage == "active") ||
			!reflect.DeepEqual(q.Deletion, allocationStep{}) ||
			q.Attempt != 0 {
			return bad()
		}
	case "rejected":
		if r.Done || !q.Deletion.Bound || !q.Deletion.Dispatched {
			return bad()
		}
	case "fallback_reserved", "cleanup_done":
		// Actual absence may transfer responsibility before any delete dispatch.
	default:
		return bad()
	}
	if !q.Deletion.Bound {
		if !reflect.DeepEqual(q.Deletion, allocationStep{}) || q.Attempt != 0 {
			return bad()
		}
		return nil
	}
	if !q.Deletion.Dispatched ||
		!reflect.DeepEqual(
			q.Deletion.Request,
			warmSelectionDelete(r, q.Attempt),
		) {
		return bad()
	}
	if q.Deletion.Proof != nil {
		if err = validateWarmDeleteReceipt(
			q.Deletion.Request,
			*q.Deletion.Proof,
		); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for n, h := range q.History {
		if !reflect.DeepEqual(h.Request, warmSelectionDelete(r, uint64(n))) ||
			h.Receipt.Status != "failed" || seen[h.Receipt.ID] {
			return bad()
		}
		if err = validateWarmDeleteReceipt(h.Request, h.Receipt); err != nil {
			return err
		}
		seen[h.Receipt.ID] = true
	}
	if q.Deletion.Proof != nil && seen[q.Deletion.Proof.ID] {
		return bad()
	}
	return nil
}

func warmSelectionDelete(r resourceRecovery, attempt uint64) api.Request {
	o := Operation{ID: r.Source}
	if attempt != 0 {
		o.ID += ":warm-delete:" + fmt.Sprint(attempt)
	}
	return api.Request{
		Key:              stepKey(o, "warm-reject:"+r.Group.Subject.ID),
		Action:           "delete",
		VMID:             r.Group.Selection.Identity.VM,
		ExpectedInstance: r.Group.Selection.Identity.Instance,
	}
}

func validateWarmDeleteReceipt(request api.Request, proof api.Operation) error {
	if proof.ID == "" || proof.Key != request.Key ||
		!reflect.DeepEqual(proof.Request, request) ||
		proof.VMID != request.VMID ||
		proof.SnapshotID != "" ||
		proof.ResultInstance != "" ||
		(proof.Status != "pending" && proof.Status != "running" && proof.Status != "succeeded" && proof.Status != "failed") ||
		(proof.Status == "failed" && proof.Error == nil) ||
		(proof.Status != "failed" && proof.Error != nil) {
		return api.Fail(
			api.ManualRecovery,
			"warm delete receipt differs from its fixed generation request",
		)
	}
	return nil
}

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
	if s.opts.Egress != nil {
		if err = s.revoke(b.ID); err != nil {
			return err
		}
	}
	i := selection.Identity
	if b.Instance != "" && b.Instance != i.Instance {
		return api.Fail(api.Conflict, "warm selection instance changed")
	}
	v, err := s.opts.Engine.GetVM(s.ctx, i.VM)
	absent := isCode(err, api.NotFound)
	if err != nil && !absent {
		return err
	}
	if !absent {
		if v.ID != i.VM || v.Instance != i.Instance || !v.Verified ||
			v.OperationID != "" || v.Config.Shape != i.Shape || v.Config.Image != r.Group.Template.Image {
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
				if !q.Deletion.Bound {
					q.Stage = "rejected"
					q.Deletion = allocationStep{
						Request:    warmSelectionDelete(latest, 0),
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
		// A fresh durable read qualifies even an already dispatched request
		// before receipt lookup; NotFound never authorizes a new attempt.
		r, err = s.loadRecovery(r.ID)
		if err != nil {
			return err
		}
		q := r.Group.Selection
		proof, e := s.opts.Engine.FindOperation(s.ctx, q.Deletion.Request.Key)
		if e != nil && !isCode(e, api.NotFound) {
			return e
		}
		if e == nil {
			if err = validateWarmDeleteReceipt(
				q.Deletion.Request,
				proof,
			); err != nil {
				return err
			}
			if q.Deletion.Proof != nil &&
				recoveryTerminal(q.Deletion.Proof.Status) &&
				!reflect.DeepEqual(*q.Deletion.Proof, proof) {
				return api.Fail(
					api.ManualRecovery,
					"terminal warm delete receipt changed",
				)
			}
			if proof.Status == "failed" {
				ownership = s.lockResourceOwnership()
				tx, e = s.store.db.BeginTx(s.ctx, nil)
				if e == nil {
					latest, le := loadRecoveryTx(s.ctx, tx, r.ID)
					e = le
					if e == nil &&
						!reflect.DeepEqual(
							latest.Group.Selection.Deletion.Request,
							proof.Request,
						) {
						e = api.Fail(
							api.ManualRecovery,
							"warm delete attempt changed during receipt lookup",
						)
					}
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
						next.Deletion = allocationStep{
							Request: warmSelectionDelete(
								latest,
								next.Attempt,
							),
							Bound:      true,
							Dispatched: true,
						}
						e = writeRecovery(s.ctx, tx, latest)
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
		r, err = s.loadRecovery(r.ID)
		if err != nil {
			return err
		}
		request := r.Group.Selection.Deletion.Request
		proof, err = api.Wait(s.ctx, s.opts.Engine, request)
		if proof.ID != "" {
			if e = validateWarmDeleteReceipt(request, proof); e != nil {
				return e
			}
			latest, readErr := s.loadRecovery(r.ID)
			if readErr != nil {
				return readErr
			}
			if !reflect.DeepEqual(
				latest.Group.Selection.Deletion.Request,
				request,
			) {
				return api.Fail(
					api.ManualRecovery,
					"warm delete receipt has a different durable attempt",
				)
			}
			stored := latest.Group.Selection.Deletion.Proof
			if stored != nil && recoveryTerminal(stored.Status) &&
				!reflect.DeepEqual(*stored, proof) {
				return api.Fail(
					api.ManualRecovery,
					"terminal warm delete receipt changed",
				)
			}
		}
		if err != nil {
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
	if err := validateWarmSelection(s.ctx, s.store.db, *r); err != nil {
		return err
	}
	if q.Stage == "active" {
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
		if err = retireSelectionReaderTx(s.ctx, tx, latest); err != nil {
			return err
		}
		return tx.Commit()
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
	if err = validateWarmSelection(s.ctx, s.store.db, r); err != nil {
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
