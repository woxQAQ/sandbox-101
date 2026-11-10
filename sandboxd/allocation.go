package sandboxd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/woxQAQ/sandbox-engine/api"
)

// Subjects identify responsibility without manufacturing a user sandbox.
type allocationSubject struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Allocation groups share the ledger with sandbox recovery, but retain their
// real subject and frozen inputs instead of borrowing a sandbox identity.
func allocationGroupID(o Operation) string {
	if o.origin != "" {
		return "allocation:" + o.origin
	}
	return "allocation:" + o.ID
}

func (s *Service) findAllocationGroup(o Operation) (*resourceRecovery, error) {
	r, err := loadRecoveryTx(s.ctx, s.store.db, allocationGroupID(o))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if r.Kind != "allocation_group" || r.Group == nil {
		return nil, Fail(
			ErrManualRecovery,
			"allocation group is malformed",
		)
	}
	return &r, nil
}

func newAllocationGroup(
	o Operation,
	subject allocationSubject,
	t Template,
) resourceRecovery {
	return resourceRecovery{
		ID: allocationGroupID(
			o,
		),
		Source:    o.ID,
		TenantID:  o.ChargeTenantID,
		Kind:      "allocation_group",
		SandboxID: o.Command.SandboxID,
		Group: &allocationGroup{
			Subject:  subject,
			Template: t,
			Steps:    map[string]allocationStep{},
		},
	}
}

func validateGroupSource(
	ctx context.Context,
	e executor,
	r resourceRecovery,
) error {
	var raw []byte
	if err := e.QueryRowContext(ctx, `SELECT body FROM operations WHERE id=?`, r.Source).
		Scan(&raw); err != nil {
		return err
	}
	var source Operation
	if err := json.Unmarshal(raw, &source); err != nil {
		return err
	}
	if r.Group == nil || source.ID != r.Source ||
		source.ChargeTenantID != r.TenantID ||
		source.Command.Template == nil ||
		!bytes.Equal(
			encode(*source.Command.Template),
			encode(r.Group.Template),
		) {
		return Fail(
			ErrManualRecovery,
			"allocation group differs from original public intent",
		)
	}
	if r.Group.Subject.Kind == "sandbox" {
		found := false
		for _, id := range source.IDs {
			if id == r.Group.Subject.ID {
				found = true
			}
		}
		if source.Command.Action != "claim" || !found {
			return Fail(
				ErrManualRecovery,
				"allocation subject is not its original sandbox",
			)
		}
	}
	if source.Command.Action == "template_put" &&
		r.Group.Template.SnapshotID != "" {
		q := r.Group.SnapshotSource
		if q == nil || q.ID == "" || q.Owner != r.TenantID ||
			q.SnapshotID != r.Group.Template.SnapshotID ||
			q.Shape != r.Group.Template.Shape ||
			q.SourceOperation == "" ||
			q.SourceKind == "" ||
			(q.Kind != "checkpoint" && q.Kind != "sleep" && q.Kind != "group" && q.Kind != "external") ||
			(q.Config != nil && q.Config.Shape != q.Shape) ||
			(q.Kind == "external" && (q.Config == nil || q.ID != q.SnapshotID)) {
			return Fail(
				ErrManualRecovery,
				"template quote lost its frozen source",
			)
		}
		var owners int
		if err := e.QueryRowContext(ctx, `SELECT count(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=? AND source_operation_id=? AND source_kind=?`, q.SnapshotID, q.Owner, q.SourceOperation, q.SourceKind).
			Scan(&owners); err != nil {
			return err
		}
		if owners != 1 {
			return Fail(
				ErrManualRecovery,
				"template source provenance changed",
			)
		}
	}
	return nil
}

func groupHolder(
	r resourceRecovery,
) string {
	return r.Group.Subject.Kind + ":" + r.Group.Subject.ID
}

func addGroupAllocation(
	ctx context.Context,
	tx *sql.Tx,
	r *resourceRecovery,
	o Operation,
	step, kind string,
	storage int64,
	request api.Request,
	bound bool,
) error {
	request.Key = stepKey(o, step)
	a := recoveryAllocation{
		OperationID:    o.ID,
		TenantID:       o.TenantID,
		ChargeTenantID: o.ChargeTenantID,
		Shape:          r.Group.Template.Shape,
		Subject:        r.Group.Subject,
		Step:           step,
		Kind:           kind,
		Request:        request,
		RequestBound:   bound,
		Resolution:     allocationResolution{State: "unresolved"},
	}
	if r.Group.Subject.Kind == "sandbox" {
		a.SandboxID = r.Group.Subject.ID
	}
	if err := putQuotaResource(
		ctx,
		tx,
		quotaResource{
			Tenant:  o.ChargeTenantID,
			Kind:    "pending_storage",
			Key:     request.Key,
			Storage: storage,
			Source:  o.ID,
		},
		quotaRef{Kind: "engine_dependency", ID: groupHolder(*r)},
	); err != nil {
		return err
	}
	r.Allocations = append(r.Allocations, a)
	r.Group.Steps[step] = allocationStep{Request: request, Bound: bound}
	return nil
}

func groupAllocation(r *resourceRecovery, step string) *recoveryAllocation {
	for i := range r.Allocations {
		if r.Allocations[i].Step == step {
			return &r.Allocations[i]
		}
	}
	return nil
}

func (s *Service) allocationEngine(
	o Operation,
	step string,
	request api.Request,
) (api.Operation, error) {
	request.Key = stepKey(o, step)
	if o.origin != "" {
		step = request.Key
	}
	ownership := s.lockResourceOwnership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		ownership()
		return api.Operation{}, err
	}
	r, err := loadRecoveryTx(s.ctx, tx, allocationGroupID(o))
	if err == nil {
		var raw []byte
		err = tx.QueryRowContext(s.ctx, `SELECT body FROM operations WHERE id=?`, r.Source).
			Scan(&raw)
		var current Operation
		if err == nil {
			err = json.Unmarshal(raw, &current)
		}
		if err == nil && recoveryTerminal(current.Status) {
			err = Fail(
				ErrManualRecovery,
				"terminal allocation intent cannot dispatch lifecycle work",
			)
		}
	}
	if err == nil && (r.Group == nil || r.Group.Gate != nil) {
		err = Fail(ErrManualRecovery, "allocation lifecycle is gated")
	}
	if err == nil && request.Action == "delete" {
		err = Fail(
			ErrManualRecovery,
			"allocation deletion requires a qualified durable destination",
		)
	}
	if err == nil &&
		(request.Action == "create" || request.Action == "clone" || request.Action == "snapshot" || request.Action == "restore") {
		a := groupAllocation(&r, step)
		if a == nil {
			err = Fail(
				ErrManualRecovery,
				"allocation step has no reservation descriptor",
			)
		} else {
			var pending int
			err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`, r.TenantID, a.Request.Key).
				Scan(&pending)
			if err == nil && pending != 1 && a.Resolution.Proof == nil {
				err = Fail(
					ErrManualRecovery,
					"allocation step has no durable pending responsibility",
				)
			}
		}
	}
	var frozen allocationStep
	if err == nil {
		var exists bool
		frozen, exists = r.Group.Steps[step]
		if exists && frozen.Bound &&
			!bytes.Equal(encode(frozen.Request), encode(request)) {
			err = Fail(ErrManualRecovery, "allocation request changed")
		}
		if err == nil && (!exists || !frozen.Bound) {
			if request.Action == "snapshot" || request.Action == "start" {
				identity := r.Group.Identity
				if identity == nil || identity.VM != request.VMID {
					err = Fail(
						ErrManualRecovery,
						"step has no frozen VM identity",
					)
				}
				if err == nil && request.Action == "snapshot" &&
					(identity.Instance == "" || request.ExpectedInstance != identity.Instance) {
					err = Fail(
						ErrManualRecovery,
						"capture has no successful generation witness",
					)
				}
				if err == nil && request.Action == "snapshot" {
					var disks int
					err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND storage_mib=?`, r.TenantID, identity.VM, identity.Shape.DiskMiB).
						Scan(&disks)
					if err == nil && disks != 1 {
						err = Fail(
							ErrManualRecovery,
							"capture lost its source disk responsibility",
						)
					}
				}
			}
			frozen = allocationStep{Request: request, Bound: true}
			if a := groupAllocation(&r, step); a != nil {
				a.Request = request
				a.RequestBound = true
			}
		}
		if err == nil {
			frozen.Dispatched = true
			r.Group.Steps[step] = frozen
			err = writeRecovery(s.ctx, tx, r)
		}
	}
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	ownership()
	if err != nil {
		return api.Operation{}, err
	}
	proof, engineErr := api.Wait(s.ctx, s.opts.Engine, request)
	engineErr = engineFailure(engineErr)
	if proof.Key == "" {
		return proof, engineErr
	}
	if err = s.adoptGroupReceipt(r.ID, step, proof); err != nil {
		return proof, errors.Join(engineErr, err)
	}
	return proof, engineErr
}

func (s *Service) adoptGroupReceipt(
	id, step string,
	proof api.Operation,
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
	frozen, ok := r.Group.Steps[step]
	if !ok || !frozen.Bound || proof.ID == "" ||
		proof.Key != frozen.Request.Key ||
		!bytes.Equal(encode(proof.Request), encode(frozen.Request)) {
		return Fail(
			ErrManualRecovery,
			"receipt differs from frozen group request",
		)
	}
	if frozen.Proof != nil && recoveryTerminal(frozen.Proof.Status) &&
		!bytes.Equal(encode(*frozen.Proof), encode(proof)) {
		return Fail(
			ErrManualRecovery,
			"terminal allocation receipt changed",
		)
	}
	frozen.Proof = &proof
	r.Group.Steps[step] = frozen
	a := groupAllocation(&r, step)
	if a != nil && !a.Resolution.delivered() {
		key := proof.VMID
		if a.Kind == "snapshot" {
			key = proof.SnapshotID
		}
		var pending int
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`, r.TenantID, a.Request.Key).
			Scan(&pending); err != nil {
			return err
		}
		if key != "" && pending == 1 {
			cost := a.Shape.DiskMiB
			if a.Kind == "snapshot" {
				cost += a.Shape.MemoryMiB
			}
			if err = putQuotaResource(
				s.ctx,
				tx,
				quotaResource{
					Tenant:  r.TenantID,
					Kind:    a.Kind,
					Key:     key,
					Storage: cost,
					Source:  a.OperationID,
				},
				quotaRef{Kind: "cleanup", ID: groupHolder(r)},
			); err != nil {
				return err
			}
			if a.Kind == "vm_disk" {
				if err = putQuotaRef(
					s.ctx,
					tx,
					r.TenantID,
					a.Kind,
					key,
					quotaRef{Kind: "cleanup", ID: "vm:" + key},
				); err != nil {
					return err
				}
				if a.Request.SnapshotID != "" {
					if err = putQuotaRef(
						s.ctx,
						tx,
						r.TenantID,
						"snapshot",
						a.Request.SnapshotID,
						quotaRef{Kind: "active_use", ID: "backing:" + key},
					); err != nil {
						return err
					}
				}
			} else {
				if err = recordSnapshotOwner(
					s.ctx,
					tx,
					key,
					r.TenantID,
					a.OperationID,
					"captured",
				); err != nil {
					return err
				}
				if err = putQuotaRef(
					s.ctx,
					tx,
					r.TenantID,
					"snapshot",
					key,
					quotaRef{
						Kind: "engine_dependency",
						ID:   "operation:" + r.Source,
					},
				); err != nil {
					return err
				}
			}
			if err = deleteQuotaRows(
				s.ctx,
				tx,
				`FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				r.TenantID,
				a.Request.Key,
			); err != nil {
				return err
			}
		}
		if key == "" && recoveryTerminal(proof.Status) {
			if err = deleteQuotaRows(
				s.ctx,
				tx,
				`FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				r.TenantID,
				a.Request.Key,
			); err != nil {
				return err
			}
			a.Resolution = allocationResolution{
				State:       "reclaimed",
				Proof:       &proof,
				Destination: "terminal-no-allocation:" + proof.ID,
			}
		} else if recoveryTerminal(proof.Status) {
			a.Resolution = allocationResolution{
				State: "result_known",
				Proof: &proof,
			}
		}
		if key != "" && a.Kind == "vm_disk" {
			if r.Group.Identity == nil {
				r.Group.Identity = &allocationIdentity{
					VM:      key,
					Owner:   r.TenantID,
					Shape:   a.Shape,
					Source:  a.OperationID,
					Receipt: proof,
				}
			}
			if r.Group.Identity.VM != key {
				return Fail(
					ErrManualRecovery,
					"allocation returned another VM",
				)
			}
			if recoveryTerminal(proof.Status) {
				r.Group.Identity.Receipt = proof
			}
			if proof.Status == "succeeded" && proof.Request.Action == "clone" &&
				proof.ResultInstance != "" {
				r.Group.Identity.Instance = proof.ResultInstance
				r.Group.Identity.Generation = &proof
			}
		}
	}
	if proof.Request.Action == "start" && proof.Status == "succeeded" &&
		proof.ResultInstance != "" {
		identity := r.Group.Identity
		if identity == nil || identity.VM != proof.VMID ||
			proof.Request.VMID != identity.VM {
			return Fail(ErrManualRecovery, "start returned another VM")
		}
		identity.Instance = proof.ResultInstance
		identity.Generation = &proof
	}
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *store) validateAllocationGroups(ctx context.Context) error {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT body FROM resource_recovery WHERE json_extract(body,'$.kind') IN ('allocation_group','allocation_cleanup')`,
	)
	if err != nil {
		return err
	}
	var groups []resourceRecovery
	for rows.Next() {
		var raw []byte
		var r resourceRecovery
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &r)
		}
		if err != nil {
			_ = rows.Close()
			return err
		}
		groups = append(groups, r)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, r := range groups {
		if r.TenantID == "" || r.Source == "" {
			return Fail(ErrManualRecovery, "group has no owner or source")
		}
		if r.Kind == "allocation_cleanup" {
			if r.Deletion == nil || r.Deletion.Identity.Instance == "" ||
				r.ID != allocationCollectorID(
					r.TenantID,
					r.Deletion.Subject,
					r.Deletion.Identity.VM,
					r.Deletion.Identity.Instance,
				) {
				return Fail(
					ErrManualRecovery,
					"cleanup identity is invalid",
				)
			}
			parent, e := loadRecoveryTx(ctx, s.db, r.Deletion.Group)
			if e != nil {
				return e
			}
			if e = validateAllocationDeletion(r, parent); e != nil {
				return e
			}
			continue
		}
		g := r.Group
		if g == nil || r.ID != "allocation:"+r.Source || g.Subject.ID == "" ||
			(g.Subject.Kind != "build" && g.Subject.Kind != "warm" && g.Subject.Kind != "sandbox") ||
			g.Template.Shape.VCPUs < 1 ||
			g.Template.Shape.DiskMiB < 64 ||
			g.Template.Shape.MemoryMiB < 128 {
			return Fail(
				ErrManualRecovery,
				"allocation subject or shape is invalid",
			)
		}
		if g.Gate != nil &&
			(g.Gate.Reason != "generation_witness_missing" || g.Gate.Mode != "observe_absence_only" || r.Done) {
			return Fail(
				ErrManualRecovery,
				"invalid allocation observation gate",
			)
		}
		for _, step := range g.Steps {
			if step.Dispatched && !step.Bound {
				return Fail(
					ErrManualRecovery,
					"unbound allocation step was dispatched",
				)
			}
			if step.Proof != nil &&
				(!step.Dispatched || !step.Bound || step.Proof.ID == "" || step.Proof.Key != step.Request.Key || !bytes.Equal(encode(step.Proof.Request), encode(step.Request))) {
				return Fail(
					ErrManualRecovery,
					"step receipt differs from frozen request",
				)
			}
		}
		if err = validateGroupSource(ctx, s.db, r); err != nil {
			return err
		}
		if err = validateWarmSelection(ctx, s.db, r); err != nil {
			return err
		}
		if g.Identity != nil {
			if err = validateAllocationProvenance(r); err != nil {
				return err
			}
		}
		if g.Subject.Kind == "build" && len(r.Allocations) > 0 &&
			(!r.Done || hasUnresolvedAllocations(r)) {
			if err = requireQuotaResource(ctx, s.db, quotaResource{
				Tenant: r.TenantID, Kind: "build_slot", Key: r.Source,
				Slots: 1, Source: r.Source,
			}, quotaRef{Kind: "active_use", ID: groupHolder(r)}); err != nil {
				return err
			}
		}
		for _, a := range r.Allocations {
			if a.Subject != g.Subject || a.Shape != g.Template.Shape ||
				a.ChargeTenantID != r.TenantID ||
				a.OperationID != r.Source ||
				a.Request.Key != stepKey(a.operation(), a.Step) {
				return Fail(
					ErrManualRecovery,
					"allocation provenance conflicts",
				)
			}
			if !a.Resolution.delivered() &&
				a.Resolution.State != "unresolved" &&
				a.Resolution.State != "result_known" {
				return Fail(
					ErrManualRecovery,
					"invalid allocation resolution",
				)
			}
			if a.Resolution.Proof != nil {
				if err = a.validate(*a.Resolution.Proof); err != nil {
					return err
				}
			}
			if a.Kind == "vm_disk" && a.Request.SnapshotID != "" &&
				!a.Resolution.delivered() {
				var dependency int
				if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`, r.TenantID, a.Request.SnapshotID, "operation:"+r.Source).
					Scan(&dependency); err != nil {
					return err
				}
				if dependency != 1 {
					return Fail(
						ErrManualRecovery,
						"allocation lost its source snapshot dependency",
					)
				}
			}
			if a.Kind == "snapshot" {
				step := g.Steps[a.Step]
				if step.Proof != nil && step.Proof.SnapshotID != "" {
					var liveParent int
					if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM operations WHERE id=? AND status IN ('pending','running')`, r.Source).
						Scan(&liveParent); err != nil {
						return err
					}
					if !a.Resolution.delivered() ||
						(a.Resolution.State == "cleanup" && liveParent != 0) {
						var dependency int
						if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`, r.TenantID, step.Proof.SnapshotID, "operation:"+r.Source).
							Scan(&dependency); err != nil {
							return err
						}
						if dependency != 1 {
							return Fail(
								ErrManualRecovery,
								"capture lost its publication reader",
							)
						}
					}
				}
			}
			if !a.Resolution.delivered() {
				step, ok := g.Steps[a.Step]
				if !ok || step.Bound != a.RequestBound ||
					!bytes.Equal(encode(step.Request), encode(a.Request)) {
					return Fail(
						ErrManualRecovery,
						"allocation step binding conflicts",
					)
				}
				kind, key := a.Kind, ""
				proof := step.Proof
				if proof != nil {
					key = proof.VMID
					if kind == "snapshot" {
						key = proof.SnapshotID
					}
				}
				if key == "" {
					kind = "pending_storage"
					key = a.Request.Key
				}
				cost := a.Shape.DiskMiB
				if a.Kind == "snapshot" {
					cost += a.Shape.MemoryMiB
				}
				ref, e := groupAllocationRef(
					ctx,
					s.db,
					r,
					kind == "pending_storage",
				)
				if e != nil {
					return e
				}
				if err = requireQuotaResource(ctx, s.db, quotaResource{
					Tenant: r.TenantID, Kind: kind, Key: key,
					Storage: cost, Source: a.OperationID,
				}, ref); err != nil {
					return err
				}
			}
		}
	}
	if err = validateReadyWarmRows(ctx, s.db); err != nil {
		return err
	}
	var catalogBad int
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM templates t WHERE COALESCE(json_extract(t.body,'$.snapshot_id'),'')!='' AND (NOT EXISTS(SELECT 1 FROM snapshot_owners o WHERE o.snapshot_id=json_extract(t.body,'$.snapshot_id') AND o.tenant_id=t.tenant) OR NOT EXISTS(SELECT 1 FROM quota_resources q JOIN quota_refs f USING(tenant_id,kind,resource_key) WHERE q.tenant_id=t.tenant AND q.kind='snapshot' AND q.resource_key=json_extract(t.body,'$.snapshot_id') AND q.storage_mib=json_extract(t.body,'$.shape.disk_mib')+json_extract(t.body,'$.shape.memory_mib') AND f.holder_kind='active_use' AND f.holder_id='template:'||t.id))`).
		Scan(&catalogBad); err != nil {
		return err
	}
	if catalogBad != 0 {
		return Fail(
			ErrManualRecovery,
			"template catalog lost its snapshot cost, owner or holder",
		)
	}
	return nil
}

type allocationGroup struct {
	Subject        allocationSubject         `json:"subject"`
	SnapshotSource *templateSnapshotSource   `json:"snapshot_source,omitempty"`
	Template       Template                  `json:"template"`
	Steps          map[string]allocationStep `json:"steps"`
	Identity       *allocationIdentity       `json:"identity,omitempty"`
	Gate           *allocationRecoveryGate   `json:"gate,omitempty"`
	Selection      *warmSelection            `json:"selection,omitempty"`
}

type allocationStep struct {
	Request    api.Request    `json:"request"`
	Bound      bool           `json:"bound"`
	Dispatched bool           `json:"dispatched"`
	Proof      *api.Operation `json:"proof,omitempty"`
}

type allocationIdentity struct {
	VM         string         `json:"vm"`
	Owner      string         `json:"owner"`
	Shape      api.Shape      `json:"shape"`
	Source     string         `json:"source"`
	Receipt    api.Operation  `json:"receipt"`
	Instance   string         `json:"instance,omitempty"`
	Generation *api.Operation `json:"generation,omitempty"`
}

type allocationRecoveryGate struct {
	Reason string `json:"reason"`
	Mode   string `json:"mode"`
}

func groupStepsTerminal(r resourceRecovery) bool {
	if r.Group == nil {
		return true
	}
	for _, step := range r.Group.Steps {
		if step.Dispatched &&
			(step.Proof == nil || !recoveryTerminal(step.Proof.Status)) {
			return false
		}
	}
	return true
}

func hasUnresolvedAllocations(r resourceRecovery) bool {
	if !groupStepsTerminal(r) {
		return true
	}
	for _, a := range r.Allocations {
		if !a.Resolution.delivered() {
			return true
		}
	}
	return false
}

func (s *Service) recordAllocationResultTx(
	tx *sql.Tx,
	a recoveryAllocation,
	proof api.Operation,
) error {
	var id string
	var raw []byte
	if err := tx.QueryRowContext(s.ctx, `SELECT r.id,r.body FROM resource_recovery r,json_each(r.body,'$.allocations') a WHERE json_extract(a.value,'$.operation_id')=? AND json_extract(a.value,'$.step')=?`, a.OperationID, a.Step).
		Scan(&id, &raw); err != nil {
		return err
	}
	var r resourceRecovery
	if err := json.Unmarshal(raw, &r); err != nil {
		return err
	}
	for i := range r.Allocations {
		next := &r.Allocations[i]
		if next.OperationID != a.OperationID || next.Step != a.Step {
			continue
		}
		if err := next.validate(proof); err != nil {
			return err
		}
		if next.Resolution.delivered() {
			return nil
		}
		if next.Resolution.Proof != nil &&
			!reflect.DeepEqual(*next.Resolution.Proof, proof) {
			return Fail(ErrManualRecovery, "allocation receipt changed")
		}
		next.Resolution = allocationResolution{
			State: "result_known",
			Proof: &proof,
		}
		return writeRecovery(s.ctx, tx, r)
	}
	return Fail(ErrManualRecovery, "missing allocation responsibility")
}

func validateAllocationProvenance(r resourceRecovery) error {
	if r.Group == nil || r.Group.Identity == nil {
		return Fail(
			ErrManualRecovery,
			"missing known allocation provenance",
		)
	}
	i := r.Group.Identity
	if i.Owner != r.TenantID || i.Source != r.Source ||
		i.Shape != r.Group.Template.Shape ||
		i.VM == "" ||
		i.Receipt.ID == "" ||
		i.Receipt.VMID != i.VM {
		return Fail(
			ErrManualRecovery,
			"known allocation provenance changed",
		)
	}
	matched := false
	for _, step := range r.Group.Steps {
		if step.Proof != nil && reflect.DeepEqual(*step.Proof, i.Receipt) &&
			step.Bound &&
			step.Dispatched &&
			reflect.DeepEqual(step.Request, i.Receipt.Request) &&
			step.Request.Key == i.Receipt.Key {
			matched = true
		}
	}
	if i.Generation != nil {
		generation := false
		for _, step := range r.Group.Steps {
			if step.Proof != nil && step.Bound && step.Dispatched &&
				reflect.DeepEqual(*step.Proof, *i.Generation) &&
				reflect.DeepEqual(step.Request, i.Generation.Request) &&
				step.Request.Key == i.Generation.Key {
				generation = true
			}
		}
		if !generation || i.Generation.Status != "succeeded" ||
			i.Generation.ResultInstance == "" ||
			i.Generation.ResultInstance != i.Instance ||
			i.Generation.VMID != i.VM {
			return Fail(
				ErrManualRecovery,
				"generation has no original frozen successful receipt",
			)
		}
	}
	if !matched {
		return Fail(
			ErrManualRecovery,
			"known VM has no original frozen receipt",
		)
	}
	if i.Receipt.Request.Action == "create" &&
		(i.Receipt.Request.Config == nil || i.Receipt.Request.Config.Shape != i.Shape) {
		return Fail(
			ErrManualRecovery,
			"created VM lost full shape provenance",
		)
	}
	if i.Receipt.Request.Action == "clone" &&
		i.Receipt.Request.SnapshotID != r.Group.Template.SnapshotID {
		return Fail(ErrManualRecovery, "clone lost frozen snapshot source")
	}
	return nil
}

func validAllocationIdentity(r resourceRecovery, v api.VM) error {
	if err := validateAllocationProvenance(r); err != nil {
		return err
	}
	g := r.Group
	if g == nil || g.Identity == nil {
		return Fail(
			ErrManualRecovery,
			"allocation has no known VM identity",
		)
	}
	i := g.Identity
	if i.Owner != r.TenantID || i.Source != r.Source ||
		i.Shape != g.Template.Shape ||
		i.VM == "" ||
		i.Receipt.VMID != i.VM ||
		i.Receipt.Key != i.Receipt.Request.Key ||
		i.Receipt.Status != "succeeded" ||
		(i.Receipt.Request.Action != "create" && i.Receipt.Request.Action != "clone") {
		return Fail(
			ErrManualRecovery,
			"allocation VM provenance is incomplete",
		)
	}
	if i.Instance == "" || i.Generation == nil ||
		i.Generation.Status != "succeeded" ||
		i.Generation.ResultInstance != i.Instance ||
		i.Generation.VMID != i.VM ||
		(i.Generation.Request.Action != "start" && i.Generation.Request.Action != "clone") {
		return Fail(
			ErrManualRecovery,
			"allocation lacks immutable successful generation witness",
		)
	}
	if v.ID != i.VM || v.Instance != i.Instance || !v.Verified ||
		v.OperationID != "" ||
		v.Config.Shape != i.Shape {
		return Fail(
			ErrManualRecovery,
			"allocation VM changed or is owned elsewhere",
		)
	}
	if i.Receipt.Request.Action == "create" &&
		!reflect.DeepEqual(i.Receipt.Request.Config, &v.Config) {
		return Fail(ErrManualRecovery, "created VM config changed")
	}
	if i.Receipt.Request.Action == "clone" &&
		i.Receipt.Request.SnapshotID != g.Template.SnapshotID {
		return Fail(ErrManualRecovery, "clone source changed")
	}
	return nil
}
