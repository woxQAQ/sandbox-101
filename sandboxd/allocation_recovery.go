package sandboxd

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
)

// Recovery billing never reconstructs shape or owner from mutable public state.
func (s *Service) quotaRecoveryAllocation(
	o Operation,
	step string,
) (*recoveryAllocation, error) {
	var raw []byte
	var matches int
	err := s.store.db.QueryRowContext(s.ctx, `SELECT a.value,count(*) OVER() FROM resource_recovery r, json_each(r.body,'$.allocations') a WHERE json_extract(a.value,'$.operation_id')=? AND json_extract(a.value,'$.step')=?`, o.ID, step).
		Scan(&raw, &matches)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if matches != 1 {
		return nil, api.Fail(
			api.ManualRecovery,
			"allocation step has multiple descriptors",
		)
	}
	var a recoveryAllocation
	if err = json.Unmarshal(raw, &a); err != nil {
		return nil, err
	}
	return &a, nil
}

func (a recoveryAllocation) validate(proof api.Operation) error {
	if proof.Key != a.Request.Key ||
		!bytes.Equal(encode(proof.Request), encode(a.Request)) ||
		(a.Request.Action == "restore" && proof.VMID != "" && proof.VMID != a.Request.VMID) ||
		(a.Request.Action == "restore" && proof.Status == "succeeded" &&
			(proof.VMID == "" || proof.VMID != a.Request.VMID || proof.ResultInstance == "")) {
		return api.Fail(
			api.ManualRecovery,
			"recovery receipt does not match frozen allocation request",
		)
	}
	return nil
}

// Receipt resolution grants lifecycle progress separately from parent completion.
func (s *Service) reconcileRecoveryAllocations(
	r *resourceRecovery,
) (recoveryProgress, error) {
	current, err := s.loadRecovery(r.ID)
	if err != nil {
		return recoveryProgress{}, err
	}
	*r = current
	progress := recoveryProgress{
		AllowedLifecycleWork: true,
		AllowedParentDone:    true,
	}
	for i := range r.Allocations {
		a := r.Allocations[i]
		if a.Resolution.delivered() {
			continue
		}
		if a.Resolution.State != "unresolved" &&
			a.Resolution.State != "result_known" {
			return recoveryProgress{}, api.Fail(
				api.ManualRecovery,
				"invalid allocation resolution",
			)
		}
		proof, err := s.findResourceStep(
			a.operation(),
			a.Step,
			a.Request.Action,
		)
		if err != nil {
			return recoveryProgress{}, err
		}
		if proof == nil {
			var dispatched int
			if err = s.store.db.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE holder_id=?`, "dispatch:"+a.Request.Key).
				Scan(&dispatched); err != nil {
				return recoveryProgress{}, err
			}
			if dispatched != 0 {
				progress.AllowedLifecycleWork = false
				progress.AllowedParentDone = false
				continue
			}
			b, e := getSandbox(s.ctx, s.store.db, a.SandboxID)
			if e != nil && !isCode(e, api.NotFound) {
				return recoveryProgress{}, e
			}
			if e == nil &&
				(b.OperationID == r.Source || strings.HasPrefix(b.OperationID, "recovery:"+r.ID+":")) &&
				(b.State == "resuming" || b.State == "pausing") {
				progress.AllowedParentDone = false
				continue
			}
			// The durable marker is written before Submit. Its absence is the only
			// cancellation proof; a receipt query's NotFound alone never suffices.
			tx, err := s.store.db.BeginTx(s.ctx, nil)
			if err != nil {
				return recoveryProgress{}, err
			}
			next := *r
			next.Allocations = append(
				[]recoveryAllocation(nil),
				r.Allocations...,
			)
			next.Allocations[i].Resolution = allocationResolution{
				State:       "cancelled", //nolint:misspell // Preserve the durable Resolution state token.
				Destination: "never-dispatched:" + a.Request.Key,
			}
			_, err = tx.ExecContext(
				s.ctx,
				`DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				a.ChargeTenantID,
				a.Request.Key,
			)
			if err == nil {
				_, err = tx.ExecContext(
					s.ctx,
					`DELETE FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=? AND NOT EXISTS(SELECT 1 FROM quota_resources WHERE source_operation_id=? AND kind='pending_storage')`,
					"operation:"+a.OperationID,
					a.OperationID,
				)
			}
			if err == nil {
				err = writeRecovery(s.ctx, tx, next)
			}
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
			if err != nil {
				return recoveryProgress{}, err
			}
			*r = next
			continue
		}
		if err = a.validate(*proof); err != nil {
			return recoveryProgress{}, err
		}
		if !recoveryTerminal(proof.Status) {
			progress.AllowedLifecycleWork = false
			progress.AllowedParentDone = false
			continue
		}
		if err = s.acceptQuotaReceipt(
			a.operation(),
			a.Step,
			a.Request,
			*proof,
		); err != nil {
			return recoveryProgress{}, err
		}
		*r, err = s.loadRecovery(r.ID)
		if err != nil {
			return recoveryProgress{}, err
		}
		a = r.Allocations[i]
		if a.Resolution.delivered() {
			continue
		}
		if a.Kind == "snapshot" && proof.SnapshotID != "" {
			ownership := s.lockResourceOwnership()
			tx, err := s.store.db.BeginTx(s.ctx, nil)
			if err == nil {
				next := *r
				next.Allocations = append(
					[]recoveryAllocation(nil),
					r.Allocations...,
				)
				cleanup := resourceRecovery{
					ID:         "snapshot:" + a.OperationID + ":" + a.Step + ":" + proof.SnapshotID,
					Source:     r.Source,
					Step:       a.Step,
					Kind:       "snapshot",
					Snapshot:   proof.SnapshotID,
					TenantID:   a.ChargeTenantID,
					Allocation: &a,
				}
				err = recordSnapshotOwner(
					s.ctx,
					tx,
					proof.SnapshotID,
					a.ChargeTenantID,
					a.OperationID,
					"captured",
				)
				if err == nil {
					_, err = tx.ExecContext(
						s.ctx,
						`INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO NOTHING`,
						cleanup.ID,
						encode(cleanup),
					)
				}
				if err == nil {
					err = putQuotaRef(
						s.ctx,
						tx,
						a.ChargeTenantID,
						"snapshot",
						proof.SnapshotID,
						quotaRef{Kind: "cleanup", ID: "recovery:" + cleanup.ID},
					)
				}
				next.Allocations[i].Resolution.State = "cleanup"
				next.Allocations[i].Resolution.Destination = cleanup.ID
				if err == nil {
					_, err = tx.ExecContext(
						s.ctx,
						`DELETE FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?`,
						"operation:"+a.OperationID,
					)
				}
				if err == nil {
					err = writeRecovery(s.ctx, tx, next)
				}
				if err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
				if err == nil {
					*r = next
				}
			}
			ownership()
			if err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		if proof.VMID == "" && proof.SnapshotID == "" {
			if err = s.deliverKnownAllocation(
				r,
				i,
				"reclaimed",
				"terminal-no-allocation:"+proof.ID,
				"",
			); err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		b, err := getSandbox(s.ctx, s.store.db, a.SandboxID)
		if err != nil {
			return recoveryProgress{}, err
		}
		v, err := s.opts.Engine.GetVM(s.ctx, proof.VMID)
		if isCode(err, api.NotFound) &&
			(proof.Status == "failed" || proof.Status == "succeeded") &&
			b.TenantID == a.ChargeTenantID &&
			(b.VMID == proof.VMID || b.VMID == "") {
			if err = s.deliverKnownAllocation(
				r,
				i,
				"reclaimed",
				"vm-absent:"+proof.VMID,
				proof.VMID,
			); err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		if err != nil {
			return recoveryProgress{}, err
		}
		if b.TenantID != a.ChargeTenantID || b.VMID != proof.VMID ||
			!v.Verified ||
			v.OperationID != "" ||
			v.ID != proof.VMID {
			return recoveryProgress{}, api.Fail(
				api.ManualRecovery,
				"allocation destination is unverified or owned elsewhere",
			)
		}
		if proof.Status == "failed" {
			// An accepted failure's VMID conservatively bills a disk version. It is
			// handed to proven existing V responsibility, never to a guessed candidate.
			if v.Instance == "" || v.Instance != a.Request.ExpectedInstance ||
				b.Instance != v.Instance {
				return recoveryProgress{}, api.Fail(
					api.ManualRecovery,
					"failed allocation has no proven existing VM holder",
				)
			}
			if err = s.deliverKnownAllocation(
				r,
				i,
				"cleanup",
				"vm:"+v.ID,
				v.ID,
			); err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		if proof.ResultInstance == "" || v.Instance != proof.ResultInstance ||
			(b.Instance != a.Request.ExpectedInstance && b.Instance != proof.ResultInstance) {
			return recoveryProgress{}, api.Fail(
				api.ManualRecovery,
				"successful allocation lacks exact result instance proof",
			)
		}
		if b.State == "terminated" {
			if err = s.handoffInstanceCleanup(r, i, b, v); err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		owned := b.OperationID == r.Source ||
			strings.HasPrefix(b.OperationID, "recovery:"+r.ID+":")
		if !owned || b.State != "resuming" {
			return recoveryProgress{}, api.Fail(
				api.ManualRecovery,
				"result has no lawful lifecycle continuation",
			)
		}
		progress.AllowedParentDone = false
	}
	return progress, nil
}

// Terminal receipt dependency retirement shares the destination transaction.
// Existing-VM delivery retains every conservative D version until V is reclaimed.
func (s *Service) deliverKnownAllocation(
	r *resourceRecovery,
	index int,
	state, destination, vm string,
) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	a := r.Allocations[index]
	if a.Resolution.Proof == nil ||
		!recoveryTerminal(a.Resolution.Proof.Status) {
		return api.Fail(api.ManualRecovery, "allocation is not terminal")
	}
	if vm != "" && state == "cleanup" {
		var holders int
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources q JOIN quota_refs r USING(tenant_id,kind,resource_key) WHERE q.tenant_id=? AND q.kind='vm_disk' AND q.source_operation_id=? AND r.holder_kind='cleanup' AND r.holder_id=?`, a.ChargeTenantID, a.OperationID, "vm:"+vm).
			Scan(&holders); err != nil {
			return err
		}
		b, err := getSandbox(s.ctx, tx, a.SandboxID)
		if err != nil {
			return err
		}
		if holders == 0 || b.VMID != vm ||
			b.Instance != a.Request.ExpectedInstance ||
			b.TenantID != a.ChargeTenantID {
			return api.Fail(
				api.ManualRecovery,
				"terminal allocation lost its VM destination",
			)
		}
	}
	if vm != "" && state == "reclaimed" &&
		a.Resolution.Proof.Status == "succeeded" {
		if err = s.validateAbsentRestoreTx(tx, *r, a, vm); err != nil {
			return err
		}
	}
	next := *r
	next.Allocations = append([]recoveryAllocation(nil), r.Allocations...)
	next.Allocations[index].Resolution.State = state
	next.Allocations[index].Resolution.Destination = destination
	if err = writeRecovery(s.ctx, tx, next); err != nil {
		return err
	}
	var pending int
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE source_operation_id=? AND kind='pending_storage'`, a.OperationID).
		Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return api.Fail(
			api.Conflict,
			"terminal allocation still has pending storage",
		)
	}
	if err = deleteQuotaRows(
		s.ctx,
		tx,
		`FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?`,
		"operation:"+a.OperationID,
	); err != nil {
		return err
	}
	if vm != "" && state == "reclaimed" {
		b, err := getSandbox(s.ctx, tx, a.SandboxID)
		if err != nil {
			return err
		}
		if b.TenantID != a.ChargeTenantID || (b.VMID != vm && b.VMID != "") {
			return api.Fail(
				api.ManualRecovery,
				"absent allocation has another sandbox target",
			)
		}
		if err = s.reclaimQuotaTx(tx, "vm_disk", vm); err != nil {
			return err
		}
		var remaining int
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND holder_id=?`, a.ChargeTenantID, "vm:"+vm).
			Scan(&remaining); err != nil {
			return err
		}
		if remaining > 0 {
			return api.Fail(
				api.Conflict,
				"absent VM still has unresolved allocation dependencies",
			)
		}
		if a.Resolution.Proof.Status == "succeeded" && b.VMID == vm {
			b.VMID, b.Instance = "", ""
			if err = saveSandbox(s.ctx, tx, b); err != nil {
				return err
			}
			current, err := getSandbox(s.ctx, tx, b.ID)
			if err != nil {
				return err
			}
			if current.VMID != "" || current.Instance != "" ||
				current.State != b.State ||
				current.TenantID != b.TenantID {
				return api.Fail(
					api.ManualRecovery,
					"absent VM metadata was not retired",
				)
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*r = next
	return nil
}

// The frozen restore intent and its immutable public source remain provenance
// after a successful release clears V. Current metadata cannot supply a witness.
func (s *Service) validateAbsentRestoreTx(
	tx *sql.Tx,
	r resourceRecovery,
	a recoveryAllocation,
	vm string,
) error {
	proof := a.Resolution.Proof
	if proof == nil || proof.Status != "succeeded" ||
		proof.ID == "" || proof.ResultInstance == "" ||
		a.Kind != "vm_disk" || a.Step != "rollback-restore" ||
		a.Request.Action != "restore" || a.Request.VMID != vm ||
		proof.VMID != vm || a.Request.ExpectedInstance == "" ||
		a.Request.SnapshotID == "" || r.Kind != "sandbox" ||
		r.SandboxID != a.SandboxID ||
		!strings.HasPrefix(a.OperationID, "recovery:"+r.ID+":") ||
		a.Request.Key != stepKey(a.operation(), a.Step) {
		return api.Fail(
			api.ManualRecovery,
			"absent restore has no exact successful history",
		)
	}
	if err := a.validate(*proof); err != nil {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(s.ctx, `SELECT body FROM operations WHERE id=?`, r.Source).
		Scan(&raw); err != nil {
		return err
	}
	var source Operation
	if err := json.Unmarshal(raw, &source); err != nil {
		return err
	}
	b, err := getSandbox(s.ctx, tx, a.SandboxID)
	if err != nil {
		return err
	}
	if source.ID != r.Source || source.Status != "failed" ||
		source.Command.Action != "rollback" || source.Step != "rollback_restore" ||
		source.SourceInstance != a.Request.ExpectedInstance ||
		source.Command.SandboxID != a.SandboxID || source.Command.CheckpointID == "" ||
		source.TenantID != a.TenantID || source.ChargeTenantID != a.ChargeTenantID ||
		b.TenantID != a.ChargeTenantID ||
		b.Shape != a.Shape ||
		b.State != "terminated" ||
		(b.VMID != vm && b.VMID != "") ||
		(b.VMID == vm && b.Instance != a.Request.ExpectedInstance && b.Instance != proof.ResultInstance) ||
		(b.VMID == "" && b.Instance != "") {
		return api.Fail(
			api.ManualRecovery,
			"absent restore history belongs to another lifecycle",
		)
	}
	return nil
}

// Each allocation freezes its engine identity and billing inputs before dispatch.
// It outlives sandbox state/operation ownership and every compensation attempt.
type recoveryAllocation struct {
	OperationID    string               `json:"operation_id"`
	TenantID       string               `json:"tenant_id"`
	ChargeTenantID string               `json:"charge_tenant_id"`
	SandboxID      string               `json:"sandbox_id"`
	Shape          api.Shape            `json:"shape"`
	Step           string               `json:"step"`
	Kind           string               `json:"kind"`
	Request        api.Request          `json:"request"`
	Subject        allocationSubject    `json:"subject"`
	RequestBound   bool                 `json:"request_bound,omitempty"`
	Resolution     allocationResolution `json:"resolution"`
}

func (a recoveryAllocation) operation() Operation {
	return Operation{
		ID:             a.OperationID,
		TenantID:       a.TenantID,
		ChargeTenantID: a.ChargeTenantID,
		Status:         "failed",
	}
}

// Resolution is the sole durable allocation responsibility. A known receipt is
// progress, while a committed destination is a completed handoff.
type allocationResolution struct {
	State       string         `json:"state"`
	Proof       *api.Operation `json:"proof,omitempty"`
	Destination string         `json:"destination,omitempty"`
}

//nolint:misspell // Preserve the durable Resolution state token.
func (r allocationResolution) delivered() bool {
	return r.Destination != "" &&
		(r.State == "active" || r.State == "cleanup" || r.State == "reclaimed" || r.State == "cancelled")
}

type recoveryProgress struct {
	AllowedLifecycleWork bool
	AllowedParentDone    bool
}

func (s *Service) resolveAllocationGroup(id string) error {
	r, err := s.loadRecovery(id)
	if err != nil {
		return err
	}
	if r.Group == nil {
		return api.Fail(api.ManualRecovery, "missing allocation group")
	}
	if r.Done && r.Group.Gate == nil && !hasUnresolvedAllocations(r) &&
		(r.Group.Selection == nil || r.Group.Selection.Stage == "active" || r.Group.Selection.Stage == "cleanup_done") {
		var dependencies int
		if err = s.store.db.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND holder_kind='engine_dependency' AND holder_id=?`, r.TenantID, "operation:"+r.Source).
			Scan(&dependencies); err != nil {
			return err
		}
		if dependencies == 0 {
			return nil
		}
	}
	lock := r.Group.Subject.ID
	if r.Group.Subject.Kind != "sandbox" {
		lock = "template:" + r.Group.Template.ID
	}
	unlock := s.lock(lock)
	defer unlock()
	r, err = s.loadRecovery(id)
	if err != nil {
		return err
	}
	public, err := s.store.operation(s.ctx, r.Source)
	if err != nil {
		return err
	}
	if !recoveryTerminal(public.Status) {
		return nil
	}
	for step, frozen := range r.Group.Steps {
		if !frozen.Dispatched ||
			(frozen.Proof != nil && recoveryTerminal(frozen.Proof.Status)) {
			continue
		}
		proof, e := s.opts.Engine.FindOperation(s.ctx, frozen.Request.Key)
		if e != nil {
			if isCode(e, api.NotFound) {
				continue
			}
			return e
		}
		if err = s.adoptGroupReceipt(r.ID, step, proof); err != nil {
			return err
		}
	}
	r, err = s.loadRecovery(id)
	if err != nil {
		return err
	}
	if r.Group.Selection != nil &&
		r.Group.Selection.Stage != "fallback_reserved" {
		return s.resolveWarmSelection(public, &r)
	}
	ownership := s.lockResourceOwnership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err == nil {
		r, err = loadRecoveryTx(s.ctx, tx, id)
		if err == nil {
			for index := range r.Allocations {
				a := &r.Allocations[index]
				frozen := r.Group.Steps[a.Step]
				if a.Resolution.delivered() {
					continue
				}
				if !frozen.Dispatched && frozen.Proof == nil {
					err = deleteQuotaRows(
						s.ctx,
						tx,
						`FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
						r.TenantID,
						a.Request.Key,
					)
					a.Resolution.State = "cancelled"                               //nolint:misspell // Preserve the durable Resolution token.
					a.Resolution.Destination = "never-dispatched:" + a.Request.Key //nolint:misspell
				} else if a.Kind == "snapshot" && a.Resolution.Proof != nil && recoveryTerminal(a.Resolution.Proof.Status) && a.Resolution.Proof.SnapshotID != "" {
					collector, e := snapshotCollector(
						s.ctx,
						tx,
						r.TenantID,
						a.Resolution.Proof.SnapshotID,
						r.Source,
					)
					err = e
					if err == nil {
						a.Resolution.State = "cleanup"
						a.Resolution.Destination = collector.ID
						err = deleteQuotaRows(
							s.ctx,
							tx,
							`FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`,
							r.TenantID,
							a.Resolution.Proof.SnapshotID,
							"operation:"+r.Source,
						)
					}
				}
				if err != nil {
					break
				}
			}
		}
		if err == nil && r.Group.Identity != nil &&
			r.Group.Identity.Instance == "" {
			r.Group.Gate = &allocationRecoveryGate{
				Reason: "generation_witness_missing",
				Mode:   "observe_absence_only",
			}
			r.Done = false
		} else if err == nil && r.Group.Identity != nil && r.Group.Identity.Generation != nil && r.Group.Identity.Generation.Status == "succeeded" &&
			r.Group.Identity.Generation.ResultInstance == r.Group.Identity.Instance {
			r.Group.Gate = nil
		}
		if err == nil {
			err = writeRecovery(s.ctx, tx, r)
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
	if r.Group.Identity == nil {
		settled := groupStepsTerminal(r)
		for _, a := range r.Allocations {
			if !a.Resolution.delivered() {
				settled = false
			}
		}
		if settled {
			ownership = s.lockResourceOwnership()
			defer ownership()
			tx, err = s.store.db.BeginTx(s.ctx, nil)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback() }()
			latest, e := loadRecoveryTx(s.ctx, tx, r.ID)
			if e != nil {
				return e
			}
			if latest.Group.Identity != nil || !groupStepsTerminal(latest) {
				return api.Fail(
					api.Conflict,
					"terminal allocation responsibility changed",
				)
			}
			for _, a := range latest.Allocations {
				if !a.Resolution.delivered() {
					return api.Fail(
						api.Conflict,
						"terminal allocation still has pending responsibility",
					)
				}
			}
			r = latest
			if r.Group.Template.SnapshotID != "" {
				if _, err = snapshotCollector(
					s.ctx,
					tx,
					r.TenantID,
					r.Group.Template.SnapshotID,
					r.Source,
				); err != nil {
					return err
				}
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
			if r.Group.Subject.Kind == "build" {
				err = deleteQuotaRows(
					s.ctx,
					tx,
					`FROM quota_resources WHERE tenant_id=? AND kind='build_slot' AND resource_key=?`,
					r.TenantID,
					r.Source,
				)
			}
			r.Done = true
			if err == nil {
				err = writeRecovery(s.ctx, tx, r)
			}
			if err == nil && r.Group.Subject.Kind == "sandbox" {
				b, e := getSandbox(s.ctx, tx, r.Group.Subject.ID)
				err = e
				if err == nil && b.State == "terminated" {
					err = quotaTerminate(s.ctx, tx, b)
				}
			}
			if err == nil {
				err = tx.Commit()
			}
			return err
		}
		return nil
	}
	v, err := s.opts.Engine.GetVM(s.ctx, r.Group.Identity.VM)
	if isCode(err, api.NotFound) {
		return s.retireAllocationAbsent(r.ID)
	}
	if err != nil {
		return err
	}
	if r.Group.Gate != nil {
		return nil
	}
	if public.Status == "succeeded" && r.Group.Subject.Kind == "warm" {
		return nil
	}
	if public.Status == "succeeded" && r.Done {
		return nil
	}
	collector, err := s.handoffAllocationCleanup(r.ID, v, false)
	if err != nil {
		return err
	}
	return s.consumeAllocationCleanup(collector)
}
