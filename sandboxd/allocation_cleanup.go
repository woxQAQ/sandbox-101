package sandboxd

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/sandbox-engine/api"
)

type allocationDeletion struct {
	Group    string             `json:"group"`
	Subject  allocationSubject  `json:"subject"`
	Identity allocationIdentity `json:"identity"`
	Step     allocationStep     `json:"step"`
	History  []cleanupAttempt   `json:"history,omitempty"`
	Absent   bool               `json:"absent,omitempty"`
}

func allocationCollectorID(
	tenant string,
	subject allocationSubject,
	vm, instance string,
) string {
	h := sha256.New()
	for _, part := range []string{"plan013-allocation-cleanup-v1", tenant, subject.Kind, subject.ID, vm, instance} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	return "allocation-cleanup:" + hex.EncodeToString(h.Sum(nil))
}

func (s *Service) allocationUsersTx(tx *sql.Tx, r resourceRecovery) error {
	i := r.Group.Identity
	var users int
	excluded := ""
	if r.Group.Subject.Kind == "sandbox" {
		b, err := getSandbox(s.ctx, tx, r.Group.Subject.ID)
		if err != nil {
			return err
		}
		if b.TenantID != r.TenantID || b.State != "terminated" ||
			(b.VMID != i.VM && b.VMID != "") {
			return Fail(
				ErrManualRecovery,
				"sandbox allocation is still live or changed",
			)
		}
		excluded = b.ID
	}
	if err := tx.QueryRowContext(s.ctx, `SELECT count(*) FROM sandboxes WHERE json_extract(body,'$.vm_id')=? AND id!=?`, i.VM, excluded).
		Scan(&users); err != nil {
		return err
	}
	if users != 0 {
		return Fail(ErrConflict, "allocation VM has a sandbox user")
	}
	if err := tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs q WHERE q.kind='vm_disk' AND (q.resource_key=? OR EXISTS(SELECT 1 FROM quota_refs v WHERE v.tenant_id=q.tenant_id AND v.kind=q.kind AND v.resource_key=q.resource_key AND v.holder_id=?)) AND (q.tenant_id!=? OR q.holder_kind='engine_dependency' OR (q.holder_kind='active_use' AND q.holder_id NOT IN (?,?)))`, i.VM, "vm:"+i.VM, r.TenantID, "warm:"+i.VM, "sandbox:"+excluded).
		Scan(&users); err != nil {
		return err
	}
	if users != 0 {
		return Fail(
			ErrConflict,
			"allocation VM has another owner or dependency",
		)
	}
	return nil
}

func (s *Service) handoffAllocationCleanup(
	id string,
	v api.VM,
	evict bool,
) (string, error) {
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	r, err := loadRecoveryTx(s.ctx, tx, id)
	if err != nil {
		return "", err
	}
	if err = validateGroupSource(s.ctx, tx, r); err != nil {
		return "", err
	}
	if err = validAllocationIdentity(r, v); err != nil {
		return "", err
	}
	if err = s.allocationUsersTx(tx, r); err != nil {
		return "", err
	}
	for _, a := range r.Allocations {
		if a.Kind == "snapshot" && !a.Resolution.delivered() &&
			(a.Resolution.Proof == nil || !recoveryTerminal(a.Resolution.Proof.Status)) {
			return "", Fail(
				ErrConflict,
				"capture still owns the VM source",
			)
		}
	}
	i := *r.Group.Identity
	collector := resourceRecovery{
		ID: allocationCollectorID(
			r.TenantID,
			r.Group.Subject,
			i.VM,
			i.Instance,
		),
		Source:   r.Source,
		TenantID: r.TenantID,
		Kind:     "allocation_cleanup",
		VMID:     i.VM,
		Instance: i.Instance,
		Deletion: &allocationDeletion{
			Group:    r.ID,
			Subject:  r.Group.Subject,
			Identity: i,
		},
	}
	collector.Deletion.Step = allocationStep{
		Request: api.Request{
			Key:              stepKey(collector.operation(), "delete"),
			Action:           "delete",
			VMID:             i.VM,
			ExpectedInstance: i.Instance,
		},
		Bound: true,
	}
	if r.Group.Subject.Kind == "build" {
		collector.Deletion.Step.Request.Key = stepKey(
			Operation{ID: r.Source},
			"build-delete",
		)
	}
	old, e := loadRecoveryTx(s.ctx, tx, collector.ID)
	if e == nil {
		if old.Deletion == nil || old.Deletion.Group != r.ID ||
			!reflect.DeepEqual(old.Deletion.Identity, i) ||
			old.Deletion.Subject != r.Group.Subject {
			return "", Fail(ErrManualRecovery, "cleanup tuple collision")
		}
		collector = old
	} else if !errors.Is(e, sql.ErrNoRows) {
		return "", e
	} else if err = insertRecovery(s.ctx, tx, collector); err != nil {
		return "", err
	}
	if evict {
		result, e := tx.ExecContext(
			s.ctx,
			`DELETE FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=?`,
			i.VM,
			r.Group.Template.ID,
			r.Group.Template.SnapshotID,
		)
		if err = requireSQLWrite(result, e); err != nil {
			return "", err
		}
		if err = deleteQuotaRows(
			s.ctx,
			tx,
			`FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`,
			r.TenantID,
			i.VM,
			"warm:"+i.VM,
		); err != nil {
			return "", err
		}
	}
	if err = putQuotaRef(
		s.ctx,
		tx,
		r.TenantID,
		"vm_disk",
		i.VM,
		quotaRef{Kind: "cleanup", ID: "recovery:" + collector.ID},
	); err != nil {
		return "", err
	}
	for index := range r.Allocations {
		a := &r.Allocations[index]
		if a.Kind == "vm_disk" && a.Resolution.State != "reclaimed" {
			a.Resolution.State = "cleanup"
			a.Resolution.Destination = collector.ID
		}
	}
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return "", err
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return collector.ID, nil
}

// Absence retirement is a ledger transaction. It never authorizes a physical
// delete, and it never cancels an unknown capture simply because its VM is gone.
func (s *Service) retireAllocationAbsent(
	id string,
	collectors ...string,
) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	r, err := loadRecoveryTx(s.ctx, tx, id)
	if err != nil {
		return err
	}
	if err = validateGroupSource(s.ctx, tx, r); err != nil {
		return err
	}
	if r.Group == nil || r.Group.Identity == nil {
		return Fail(ErrManualRecovery, "absent VM has no durable identity")
	}
	i := r.Group.Identity
	if err = validateAllocationProvenance(r); err != nil {
		return err
	}
	if !recoveryTerminal(i.Receipt.Status) {
		return Fail(
			ErrConflict,
			"nonterminal allocation cannot retire its known disk",
		)
	}
	if i.Owner != r.TenantID || i.Source != r.Source ||
		i.Shape != r.Group.Template.Shape ||
		i.Receipt.VMID != i.VM ||
		i.Receipt.ID == "" {
		return Fail(ErrManualRecovery, "absence provenance mismatch")
	}
	if err = s.allocationUsersTx(tx, r); err != nil {
		return err
	}
	var other int
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM sandboxes WHERE json_extract(body,'$.vm_id')=? AND id!=?`, i.VM, r.SandboxID).
		Scan(&other); err != nil {
		return err
	}
	if other != 0 {
		return Fail(
			ErrManualRecovery,
			"absent VM belongs to another sandbox",
		)
	}
	// Undispatched steps can be canceled individually; known allocations
	// retain their own destination regardless of unrelated step markers.
	for index := range r.Allocations {
		a := &r.Allocations[index]
		frozen := r.Group.Steps[a.Step]
		if a.Kind == "vm_disk" && a.Resolution.Proof != nil &&
			a.Resolution.Proof.VMID == i.VM {
			a.Resolution.State = "reclaimed"
			a.Resolution.Destination = "vm-absent:" + i.VM
			continue
		}
		if !a.Resolution.delivered() && !frozen.Dispatched &&
			frozen.Proof == nil {
			if err = deleteQuotaRows(
				s.ctx,
				tx,
				`FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				r.TenantID,
				a.Request.Key,
			); err != nil {
				return err
			}
			a.Resolution.State = "cancelled"                               //nolint:misspell // Preserve the durable Resolution token.
			a.Resolution.Destination = "never-dispatched:" + a.Request.Key //nolint:misspell
		}
	}
	var dependency int
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id!=?`, r.TenantID, i.VM, "operation:"+r.Source).
		Scan(&dependency); err != nil {
		return err
	}
	if dependency != 0 {
		return Fail(
			ErrConflict,
			"absent VM still has independent dependencies",
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
	settled := groupStepsTerminal(r)
	for _, a := range r.Allocations {
		if !a.Resolution.delivered() {
			settled = false
		}
	}
	if settled {
		var sourceStatus string
		if err = tx.QueryRowContext(s.ctx, `SELECT status FROM operations WHERE id=?`, r.Source).
			Scan(&sourceStatus); err != nil {
			return err
		}
		// A successful build still needs its captured SID between VM retirement
		// and catalog publication. Only publication or terminal cleanup releases it.
		if r.Group.Subject.Kind != "build" || recoveryTerminal(sourceStatus) {
			if err = deleteQuotaRows(
				s.ctx,
				tx,
				`FROM quota_refs WHERE tenant_id=? AND holder_kind='engine_dependency' AND holder_id=?`,
				r.TenantID,
				"operation:"+r.Source,
			); err != nil {
				return err
			}
		}
		if r.Group.Subject.Kind == "build" {
			if err = deleteQuotaRows(
				s.ctx,
				tx,
				`FROM quota_resources WHERE tenant_id=? AND kind='build_slot' AND resource_key=?`,
				r.TenantID,
				r.Source,
			); err != nil {
				return err
			}
		}
		r.Group.Gate = nil
		r.Done = true
	}
	if !settled {
		r.Done = false
	}
	if r.Group.Subject.Kind == "sandbox" {
		b, e := getSandbox(s.ctx, tx, r.Group.Subject.ID)
		if e != nil {
			return e
		}
		if b.TenantID != r.TenantID || (b.VMID != "" && b.VMID != i.VM) {
			return Fail(ErrManualRecovery, "absent sandbox target changed")
		}
		if b.VMID == i.VM {
			b.VMID = ""
			b.Instance = ""
			if b.State != "running" {
				b.State = "terminated"
			}
			if err = saveSandbox(s.ctx, tx, b); err != nil {
				return err
			}
		}
		if err = writeRecovery(s.ctx, tx, r); err != nil {
			return err
		}
		if settled {
			if err = quotaTerminate(s.ctx, tx, b); err != nil {
				return err
			}
		}
	}
	if r.Group.Subject.Kind == "warm" {
		if err = deleteQuotaRows(
			s.ctx,
			tx,
			`FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=?`,
			i.VM,
			r.Group.Template.ID,
			r.Group.Template.SnapshotID,
		); err != nil {
			return err
		}
	}
	for _, collectorID := range collectors {
		collector, e := loadRecoveryTx(s.ctx, tx, collectorID)
		if e != nil {
			return e
		}
		if collector.Deletion == nil || collector.Deletion.Group != r.ID ||
			collector.VMID != i.VM ||
			collector.Instance == "" {
			return Fail(
				ErrManualRecovery,
				"absence collector destination changed",
			)
		}
		collector.Done = true
		collector.Deletion.Absent = true
		if err = writeRecovery(s.ctx, tx, collector); err != nil {
			return err
		}
	}
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("allocation absence commit: %w", err)
	}
	return nil
}

func validateAllocationDeletion(c, r resourceRecovery) error {
	if c.Deletion == nil || r.Group == nil || r.Group.Identity == nil {
		return Fail(
			ErrManualRecovery,
			"cleanup has no allocation provenance",
		)
	}
	i := r.Group.Identity
	d := c.Deletion
	if c.Source != r.Source || c.TenantID != r.TenantID || d.Group != r.ID ||
		d.Subject != r.Group.Subject ||
		!reflect.DeepEqual(d.Identity, *i) ||
		c.VMID != i.VM ||
		c.Instance != i.Instance ||
		i.Instance == "" ||
		c.ID != allocationCollectorID(r.TenantID, d.Subject, i.VM, i.Instance) {
		return Fail(
			ErrManualRecovery,
			"cleanup tuple differs from its frozen allocation",
		)
	}
	request := d.Step.Request
	key := stepKey(c.operation(), "delete")
	if c.Attempt == 0 && d.Subject.Kind == "build" {
		key = stepKey(Operation{ID: r.Source}, "build-delete")
	}
	if !d.Step.Bound || request.Action != "delete" || request.Key != key ||
		request.VMID != i.VM ||
		request.ExpectedInstance != i.Instance ||
		request.SnapshotID != "" ||
		request.Config != nil ||
		request.Policy != nil ||
		request.NetworkEnabled {
		return Fail(
			ErrManualRecovery,
			"cleanup request differs from exact generation destination",
		)
	}
	if uint64(len(d.History)) != c.Attempt {
		return Fail(
			ErrManualRecovery,
			"cleanup attempt lacks terminal failure history",
		)
	}
	for index, history := range d.History {
		attempt := c
		attempt.Attempt = uint64(index)
		oldKey := stepKey(attempt.operation(), "delete")
		if index == 0 && d.Subject.Kind == "build" {
			oldKey = stepKey(Operation{ID: r.Source}, "build-delete")
		}
		expected := api.Request{
			Key:              oldKey,
			Action:           "delete",
			VMID:             i.VM,
			ExpectedInstance: i.Instance,
		}
		if !reflect.DeepEqual(history.Request, expected) ||
			history.Receipt.ID == "" ||
			history.Receipt.Key != oldKey ||
			history.Receipt.Status != "failed" ||
			!reflect.DeepEqual(history.Receipt.Request, expected) {
			return Fail(
				ErrManualRecovery,
				"cleanup history differs from fixed generation request",
			)
		}
	}
	return validateAllocationProvenance(r)
}

func (s *Service) consumeAllocationCleanup(id string) error {
	c, err := s.loadRecovery(id)
	if err != nil {
		return err
	}
	if c.Done {
		return nil
	}
	if c.Deletion == nil || c.Instance == "" {
		return Fail(ErrManualRecovery, "cleanup has no successful witness")
	}
	r, err := s.loadRecovery(c.Deletion.Group)
	if err != nil {
		return err
	}
	if err = validateAllocationDeletion(c, r); err != nil {
		return err
	}
	v, err := s.opts.Engine.GetVM(s.ctx, c.VMID)
	if isEngineCode(err, api.NotFound) {
		return s.finishAllocationCleanup(c)
	}
	if err != nil {
		return err
	}
	if err = validAllocationIdentity(r, v); err != nil {
		return err
	}
	if c.Deletion.Step.Dispatched {
		proof, e := s.opts.Engine.FindOperation(
			s.ctx,
			c.Deletion.Step.Request.Key,
		)
		if e != nil && !isEngineCode(e, api.NotFound) {
			return e
		}
		if e == nil {
			if !reflect.DeepEqual(proof.Request, c.Deletion.Step.Request) {
				return Fail(ErrManualRecovery, "cleanup receipt mismatch")
			}
			if proof.Status == "failed" {
				ownership := s.lockResourceOwnership()
				tx, e := s.store.db.BeginTx(s.ctx, nil)
				if e == nil {
					latest, le := loadRecoveryTx(s.ctx, tx, c.ID)
					e = le
					if e == nil {
						latest.Deletion.History = append(
							latest.Deletion.History,
							cleanupAttempt{
								Request: latest.Deletion.Step.Request,
								Receipt: proof,
							},
						)
						latest.Attempt++
						latest.Deletion.Step = allocationStep{
							Request: api.Request{
								Key: stepKey(
									latest.operation(),
									"delete",
								),
								Action:           "delete",
								VMID:             latest.VMID,
								ExpectedInstance: latest.Instance,
							},
							Bound: true,
						}
						e = writeRecovery(s.ctx, tx, latest)
						c = latest
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
	ownership := s.lockResourceOwnership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err == nil {
		latestGroup, e := loadRecoveryTx(s.ctx, tx, c.Deletion.Group)
		if e != nil {
			err = e
		} else if e = validAllocationIdentity(latestGroup, v); e != nil {
			err = e
		} else {
			err = s.allocationUsersTx(tx, latestGroup)
		}
		latest, e := loadRecoveryTx(s.ctx, tx, c.ID)
		if err == nil {
			err = e
		}
		if err == nil {
			err = validateAllocationDeletion(latest, latestGroup)
		}
		if err == nil {
			latest.Deletion.Step.Dispatched = true
			err = writeRecovery(s.ctx, tx, latest)
			c = latest
		}
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}
	ownership()
	if err != nil {
		return err
	}
	if _, err = api.Wait(
		s.ctx,
		s.opts.Engine,
		c.Deletion.Step.Request,
	); err != nil {
		return err
	}
	_, err = s.opts.Engine.GetVM(s.ctx, c.VMID)
	if !isEngineCode(err, api.NotFound) {
		if err != nil {
			return err
		}
		return Fail(ErrManualRecovery, "delete did not prove VM absence")
	}
	return s.finishAllocationCleanup(c)
}

func (s *Service) finishAllocationCleanup(c resourceRecovery) error {
	return s.retireAllocationAbsent(c.Deletion.Group, c.ID)
}
