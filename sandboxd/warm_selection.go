package sandboxd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/woxQAQ/sandbox-engine/api"
)

// Ready rows retain the version originally published by their refill. A
// rolling template replacement does not rewrite that frozen owner or shape.
func validateReadyWarmRows(ctx context.Context, e *sql.DB) error {
	rows, err := e.QueryContext(
		ctx,
		`SELECT vm_id,template_id,snapshot_id FROM warm`,
	)
	if err != nil {
		return err
	}
	type ready struct{ vm, template, snapshot string }
	var inventory []ready
	for rows.Next() {
		var item ready
		if err = rows.Scan(
			&item.vm,
			&item.template,
			&item.snapshot,
		); err != nil {
			_ = rows.Close()
			return err
		}
		inventory = append(inventory, item)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, item := range inventory {
		if err = validateReadyWarmRow(
			ctx,
			e,
			item.vm,
			item.template,
			item.snapshot,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateReadyWarmRow(
	ctx context.Context,
	e executor,
	vm, template, snapshot string,
) error {
	bad := func() error { return api.Fail(api.ManualRecovery, "ready warm lost its frozen cost, source or holder") }
	var count int
	var raw []byte
	if err := e.QueryRowContext(ctx, `SELECT count(*),body FROM resource_recovery WHERE json_extract(body,'$.kind')='allocation_group' AND json_extract(body,'$.allocation_group.subject.kind')='warm' AND json_extract(body,'$.allocation_group.identity.vm')=?`, vm).
		Scan(&count, &raw); err != nil {
		return err
	}
	var r resourceRecovery
	if count != 1 || json.Unmarshal(raw, &r) != nil || r.Group == nil ||
		r.Group.Identity == nil {
		return bad()
	}
	g, i := r.Group, r.Group.Identity
	if r.ID != "allocation:"+r.Source || r.TenantID == "" ||
		g.Subject != (allocationSubject{Kind: "warm", ID: r.Source}) || g.Gate != nil ||
		g.Selection != nil || g.Template.ID != template || g.Template.SnapshotID != snapshot ||
		g.Template.TenantID != r.TenantID || i.VM != vm || i.Owner != r.TenantID ||
		i.Shape != g.Template.Shape || i.Instance == "" || i.Generation == nil ||
		i.Receipt.Status != "succeeded" || i.Generation.Status != "succeeded" ||
		i.Generation.ResultInstance != i.Instance || len(r.Allocations) != 1 {
		return bad()
	}
	if err := validateGroupSource(ctx, e, r); err != nil {
		return errors.Join(bad(), err)
	}
	if err := validateAllocationProvenance(r); err != nil {
		return err
	}
	if err := e.QueryRowContext(ctx, `SELECT body FROM operations WHERE id=?`, r.Source).
		Scan(&raw); err != nil {
		return errors.Join(bad(), err)
	}
	var refill Operation
	if json.Unmarshal(raw, &refill) != nil || refill.ID != r.Source ||
		refill.Command.Action != "refill" ||
		refill.Command.TemplateID != template ||
		refill.ChargeTenantID != r.TenantID {
		return bad()
	}
	a := r.Allocations[0]
	step, ok := g.Steps[a.Step]
	if !ok || !step.Bound || !step.Dispatched || step.Proof == nil ||
		!reflect.DeepEqual(
			*step.Proof,
			i.Receipt,
		) || !reflect.DeepEqual(a.Request, i.Receipt.Request) ||
		a.OperationID != r.Source || a.TenantID != refill.TenantID || a.ChargeTenantID != r.TenantID || a.Shape != i.Shape ||
		a.Subject != g.Subject || a.Kind != "vm_disk" || !a.RequestBound ||
		a.Request.Key != stepKey(a.operation(), a.Step) ||
		a.Resolution.State != "active" || a.Resolution.Destination != "warm:"+vm ||
		a.Resolution.Proof == nil || !reflect.DeepEqual(*a.Resolution.Proof, i.Receipt) {
		return bad()
	}
	if snapshot != "" &&
		(!reflect.DeepEqual(a.Request, api.Request{Key: a.Request.Key, Action: "clone", SnapshotID: snapshot}) || !reflect.DeepEqual(i.Receipt, *i.Generation)) {
		return bad()
	}
	if err := e.QueryRowContext(ctx, `SELECT count(*) FROM quota_resources q JOIN quota_refs f USING(tenant_id,kind,resource_key) WHERE q.tenant_id=? AND q.kind='vm_disk' AND q.resource_key=? AND q.sandbox_slots=0 AND q.storage_mib=? AND q.source_operation_id=? AND f.holder_kind='active_use' AND f.holder_id=?`, r.TenantID, vm, i.Shape.DiskMiB, r.Source, "warm:"+vm).
		Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return bad()
	}
	if snapshot != "" {
		if err := e.QueryRowContext(ctx, `SELECT count(*) FROM quota_resources q JOIN quota_refs f USING(tenant_id,kind,resource_key) JOIN snapshot_owners o ON o.snapshot_id=q.resource_key AND o.tenant_id=q.tenant_id WHERE q.tenant_id=? AND q.kind='snapshot' AND q.resource_key=? AND q.storage_mib=? AND f.holder_kind='active_use' AND f.holder_id=?`, r.TenantID, snapshot, i.Shape.DiskMiB+i.Shape.MemoryMiB, "backing:"+vm).
			Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return bad()
		}
	}
	return nil
}

func (s *Service) selectWarmTx(
	ctx context.Context,
	tx *sql.Tx,
	t Template,
) (*resourceRecovery, error) {
	var vm string
	err := tx.QueryRowContext(ctx, `SELECT vm_id FROM warm WHERE template_id=? AND snapshot_id=? ORDER BY created_at,vm_id LIMIT 1`, t.ID, t.SnapshotID).
		Scan(&vm)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = validateReadyWarmRow(ctx, tx, vm, t.ID, t.SnapshotID); err != nil {
		return nil, err
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE json_extract(body,'$.kind')='allocation_group' AND json_extract(body,'$.allocation_group.subject.kind')='warm' AND json_extract(body,'$.allocation_group.identity.vm')=?`, vm).
		Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, api.Fail(
			api.ManualRecovery,
			"ready warm VM has no durable provenance",
		)
	}
	if err != nil {
		return nil, err
	}
	var r resourceRecovery
	if err = json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	g := r.Group
	if g == nil || g.Identity == nil || g.Gate != nil ||
		r.TenantID != t.TenantID ||
		g.Template.ID != t.ID ||
		g.Template.SnapshotID != t.SnapshotID ||
		g.Template.Shape != t.Shape ||
		g.Identity.Instance == "" ||
		g.Identity.Generation == nil ||
		g.Identity.Generation.Status != "succeeded" ||
		g.Identity.Generation.ResultInstance != g.Identity.Instance ||
		g.Identity.Generation.VMID != vm ||
		len(r.Allocations) != 1 ||
		r.Allocations[0].Resolution.State != "active" ||
		r.Allocations[0].Resolution.Destination != "warm:"+vm {
		return nil, api.Fail(
			api.ManualRecovery,
			"ready warm provenance does not match template",
		)
	}
	var cost int64
	if err = tx.QueryRowContext(ctx, `SELECT storage_mib FROM quota_resources WHERE tenant_id=? AND kind='vm_disk' AND resource_key=?`, t.TenantID, vm).
		Scan(&cost); err != nil {
		return nil, err
	}
	if cost != t.Shape.DiskMiB {
		return nil, api.Fail(
			api.ManualRecovery,
			"ready warm disk cost mismatch",
		)
	}
	var refs int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`, t.TenantID, vm, "warm:"+vm).
		Scan(&refs); err != nil {
		return nil, err
	}
	if refs != 1 {
		return nil, api.Fail(
			api.ManualRecovery,
			"ready warm disk holder is missing",
		)
	}
	if t.SnapshotID != "" {
		if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`, t.TenantID, t.SnapshotID, "backing:"+vm).
			Scan(&refs); err != nil {
			return nil, err
		}
		if refs != 1 {
			return nil, api.Fail(
				api.ManualRecovery,
				"ready warm backing holder is missing",
			)
		}
	}
	return &r, nil
}

func (s *Service) claimWarmTx(
	ctx context.Context,
	tx *sql.Tx,
	o Operation,
	b *Sandbox,
	t Template,
	refill resourceRecovery,
) error {
	i := *refill.Group.Identity
	result, err := tx.ExecContext(
		ctx,
		`DELETE FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=?`,
		i.VM,
		t.ID,
		t.SnapshotID,
	)
	if err = requireSQLWrite(result, err); err != nil {
		return err
	}
	if err = deleteQuotaRows(
		ctx,
		tx,
		`FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`,
		t.TenantID,
		i.VM,
		"warm:"+i.VM,
	); err != nil {
		return err
	}
	if err = putQuotaRef(
		ctx,
		tx,
		t.TenantID,
		"vm_disk",
		i.VM,
		quotaRef{Kind: "active_use", ID: "sandbox:" + b.ID},
	); err != nil {
		return err
	}
	refill.Allocations[0].Resolution.Destination = "sandbox:" + b.ID
	if err = writeRecovery(ctx, tx, refill); err != nil {
		return err
	}
	r := newAllocationGroup(o, allocationSubject{Kind: "sandbox", ID: b.ID}, t)
	r.SandboxID = b.ID
	r.Group.Selection = &warmSelection{
		Refill:   refill.ID,
		Identity: i,
		Stage:    "selected",
	}
	b.VMID = i.VM
	b.Instance = i.Instance
	b.ProvisionSource = "warm"
	return insertRecovery(ctx, tx, r)
}

func (s *Service) commitSelectionActiveTx(
	tx *sql.Tx,
	o Operation,
	b Sandbox,
	target *activationTarget,
) error {
	r, err := loadRecoveryTx(s.ctx, tx, "allocation:"+target.Origin)
	if err != nil {
		return err
	}
	selection := r.Group.Selection
	current, err := getSandbox(s.ctx, tx, b.ID)
	if err != nil {
		return err
	}
	if selection == nil || selection.Stage != "selected" ||
		current.State != "provisioning" ||
		current.OperationID != target.Origin ||
		current.VMID != target.VM ||
		current.Instance != target.Instance ||
		current.TenantID != r.TenantID ||
		selection.Identity.VM != target.VM ||
		selection.Identity.Instance != target.Instance {
		return api.Fail(api.Conflict, "warm selection publication changed")
	}
	var holders int
	identity := selection.Identity
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources q JOIN quota_refs f USING(tenant_id,kind,resource_key) WHERE q.tenant_id=? AND q.kind='vm_disk' AND q.resource_key=? AND q.storage_mib=? AND q.source_operation_id=? AND f.holder_kind='active_use' AND f.holder_id=?`, r.TenantID, identity.VM, identity.Shape.DiskMiB, identity.Source, "sandbox:"+b.ID).
		Scan(&holders); err != nil {
		return err
	}
	if holders != 1 {
		return api.Fail(
			api.ManualRecovery,
			"selection active destination lost its disk holder",
		)
	}
	if r.Group.Template.SnapshotID != "" {
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`, r.TenantID, r.Group.Template.SnapshotID, "backing:"+identity.VM).
			Scan(&holders); err != nil {
			return err
		}
		if holders != 1 {
			return api.Fail(
				api.ManualRecovery,
				"selection active destination lost its backing",
			)
		}
	}
	selection.Stage = "active"
	r.Done = true
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return err
	}
	return retireSelectionReaderTx(s.ctx, tx, r)
}

// The active selection is a historical transfer to the real sandbox. Later
// pause/release operations may change its VM, without reviving the old reader.
func retireSelectionReaderTx(
	ctx context.Context,
	e executor,
	r resourceRecovery,
) error {
	if err := validateWarmSelection(ctx, e, r); err != nil {
		return err
	}
	if r.Group == nil || r.Group.Selection == nil ||
		r.Group.Selection.Stage != "active" ||
		!r.Done ||
		!groupStepsTerminal(r) ||
		hasUnresolvedAllocations(r) {
		return api.Fail(
			api.ManualRecovery,
			"selection has no completed active destination",
		)
	}
	if r.Group.Template.SnapshotID == "" {
		return nil
	}
	return deleteQuotaRows(
		ctx,
		e,
		`FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`,
		r.TenantID,
		r.Group.Template.SnapshotID,
		"operation:"+r.Source,
	)
}

type warmSelection struct {
	Refill   string             `json:"refill"`
	Identity allocationIdentity `json:"identity"`
	Stage    string             `json:"stage"`
	Deletion allocationStep     `json:"deletion"`
	History  []cleanupAttempt   `json:"history,omitempty"`
	Attempt  uint64             `json:"attempt,omitempty"`
}

// validateWarmSelection follows the original refill, even after the claim has
// delivered a different replacement generation. Only frozen receipts authorize
// cleanup; current Engine state never repairs a persisted request.
func validateWarmSelection(
	ctx context.Context,
	e executor,
	r resourceRecovery,
) error {
	bad := func() error {
		return api.Fail(
			api.ManualRecovery,
			"warm selection differs from its frozen responsibility",
		)
	}
	if r.Kind != "allocation_group" {
		return nil
	}
	if r.Group == nil {
		return bad()
	}
	// The refill's transfer remains durable after replacement, pause and release.
	// Current sandbox VM/source fields cannot establish whether a selection existed.
	if r.Group.Subject.Kind == "sandbox" {
		if r.SandboxID != r.Group.Subject.ID {
			return bad()
		}
		if err := validateGroupSource(ctx, e, r); err != nil {
			return errors.Join(bad(), err)
		}
		var transfers int
		if err := e.QueryRowContext(ctx, `SELECT count(*) FROM resource_recovery r, json_each(r.body,'$.allocations') a WHERE json_extract(r.body,'$.kind')='allocation_group' AND json_extract(r.body,'$.allocation_group.subject.kind')='warm' AND json_extract(a.value,'$.resolution.state')='active' AND json_extract(a.value,'$.resolution.destination')=?`, "sandbox:"+r.Group.Subject.ID).
			Scan(&transfers); err != nil {
			return err
		}
		if (transfers != 0 && (transfers != 1 || r.Group.Selection == nil)) ||
			(transfers == 0 && r.Group.Selection != nil) ||
			(r.Group.Selection == nil && len(r.Allocations) != 1) {
			return bad()
		}
	}
	if r.Group.Selection == nil {
		return nil
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
		// Absence can precede dispatch; the durable destination is checked below.
	default:
		return bad()
	}
	if err = validateWarmSelectionDestination(ctx, e, r, b); err != nil {
		return err
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

// A stage records a transfer, not its authority. Validate the destination from
// the original claim and its frozen version, including after later lifecycles.
func validateWarmSelectionDestination(
	ctx context.Context,
	e executor,
	r resourceRecovery,
	b Sandbox,
) error {
	g, q := r.Group, r.Group.Selection
	if q.Stage != "fallback_reserved" && q.Stage != "cleanup_done" {
		return nil
	}
	bad := func() error {
		return api.Fail(
			api.ManualRecovery,
			"warm selection has no qualified durable destination",
		)
	}
	count := func(query string, args ...any) (int, error) {
		var n int
		err := e.QueryRowContext(ctx, query, args...).Scan(&n)
		return n, err
	}
	absent := func(vm string) error {
		n, err := count(`SELECT
 (SELECT count(*) FROM quota_resources q WHERE q.kind='vm_disk' AND (q.resource_key=? OR EXISTS(SELECT 1 FROM quota_refs f WHERE f.tenant_id=q.tenant_id AND f.kind=q.kind AND f.resource_key=q.resource_key AND f.holder_id=?)))+
 (SELECT count(*) FROM quota_refs WHERE kind='snapshot' AND holder_id=?)+
 (SELECT count(*) FROM warm WHERE vm_id=?)+
 (SELECT count(*) FROM sandboxes WHERE json_extract(body,'$.vm_id')=?)`, vm, "vm:"+vm, "backing:"+vm, vm, vm)
		if err != nil {
			return err
		}
		if n != 0 {
			return bad()
		}
		return nil
	}
	if err := absent(q.Identity.VM); err != nil {
		return err
	}
	if r.Done && (g.Gate != nil || hasUnresolvedAllocations(r)) {
		return bad()
	}
	if !groupStepsTerminal(r) || hasUnresolvedAllocations(r) {
		ref := quotaRef{Kind: "active_use", ID: "sandbox:" + b.ID}
		if b.State == "terminated" {
			ref.Kind = "cleanup"
		}
		if err := requireQuotaResource(ctx, e, quotaResource{
			Tenant: r.TenantID, Kind: "sandbox_slot", Key: b.ID,
			Slots: 1, Source: r.Source,
		}, ref); err != nil {
			return err
		}
		if g.Template.SnapshotID != "" {
			n, err := count(
				`SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`,
				r.TenantID,
				g.Template.SnapshotID,
				"operation:"+r.Source,
			)
			if err != nil {
				return err
			}
			if n != 1 {
				return bad()
			}
		}
	}
	if q.Stage == "cleanup_done" {
		if b.State != "terminated" || b.VMID != "" || b.Instance != "" ||
			len(r.Allocations) != 0 ||
			g.Identity != nil {
			return bad()
		}
		if r.Done {
			n, err := count(
				`SELECT count(*) FROM quota_refs WHERE tenant_id=? AND holder_kind='engine_dependency' AND holder_id=?`,
				r.TenantID,
				"operation:"+r.Source,
			)
			if err != nil {
				return err
			}
			if n != 0 {
				return bad()
			}
		}
		return nil
	}
	if len(r.Allocations) != 1 {
		return bad()
	}
	var raw []byte
	if err := e.QueryRowContext(ctx, `SELECT body FROM operations WHERE id=?`, r.Source).
		Scan(&raw); err != nil {
		return err
	}
	var claim Operation
	if err := json.Unmarshal(raw, &claim); err != nil {
		return err
	}
	a := r.Allocations[0]
	request := api.Request{
		Key:        stepKey(claim, "provision"),
		Action:     "clone",
		SnapshotID: g.Template.SnapshotID,
	}
	if g.Template.SnapshotID == "" {
		request = api.Request{
			Key:    request.Key,
			Action: "create",
			Config: &api.Config{
				Image: g.Template.Image,
				Shape: g.Template.Shape,
				Labels: map[string]string{
					"sandboxd":   "sandbox",
					"sandbox_id": b.ID,
				},
			},
		}
	}
	step, ok := g.Steps["provision"]
	if !ok || a.OperationID != r.Source || a.TenantID != claim.TenantID ||
		a.ChargeTenantID != r.TenantID ||
		a.SandboxID != b.ID ||
		a.Subject != g.Subject ||
		a.Shape != g.Template.Shape ||
		a.Kind != "vm_disk" ||
		a.Step != "provision" ||
		!a.RequestBound ||
		!step.Bound ||
		!reflect.DeepEqual(a.Request, request) ||
		!reflect.DeepEqual(step.Request, request) ||
		(step.Proof != nil && !step.Dispatched) {
		return bad()
	}
	if step.Proof != nil {
		p := step.Proof
		if p.ID == "" || p.Key != request.Key ||
			!reflect.DeepEqual(p.Request, request) ||
			(p.Status != "pending" && p.Status != "running" && p.Status != "failed" && p.Status != "succeeded") ||
			(p.Status == "failed") != (p.Error != nil) {
			return bad()
		}
	}
	proof := a.Resolution.Proof
	if proof != nil &&
		(step.Proof == nil || !reflect.DeepEqual(*proof, *step.Proof) || !recoveryTerminal(proof.Status)) {
		return bad()
	}
	if g.Identity != nil {
		if err := validateAllocationProvenance(r); err != nil {
			return err
		}
		if step.Proof == nil ||
			!reflect.DeepEqual(g.Identity.Receipt, *step.Proof) ||
			g.Identity.VM == q.Identity.VM {
			return bad()
		}
	}
	pending, err := count(
		`SELECT count(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
		r.TenantID,
		request.Key,
	)
	if err != nil {
		return err
	}
	charged := func(kind, key string, ref quotaRef) error {
		return requireQuotaResource(ctx, e, quotaResource{
			Tenant: r.TenantID, Kind: kind, Key: key,
			Storage: a.Shape.DiskMiB, Source: r.Source,
		}, ref)
	}
	known := func() error {
		if g.Identity == nil || pending != 0 {
			return bad()
		}
		return charged(
			"vm_disk",
			g.Identity.VM,
			quotaRef{Kind: "cleanup", ID: groupHolder(r)},
		)
	}
	switch a.Resolution.State {
	case "unresolved", "result_known":
		if (a.Resolution.State == "unresolved" && proof != nil) ||
			(b.VMID != "" && (g.Identity == nil || b.VMID != g.Identity.VM)) {
			return bad()
		}
		if r.Done || a.Resolution.Destination != "" {
			return bad()
		}
		if a.Resolution.State == "result_known" &&
			(proof == nil || g.Identity == nil) {
			return bad()
		}
		if g.Identity == nil {
			if a.Resolution.State != "unresolved" || proof != nil ||
				(step.Proof != nil && step.Proof.VMID != "") ||
				pending != 1 {
				return bad()
			}
			ref, err := groupAllocationRef(ctx, e, r, true)
			if err != nil {
				return err
			}
			return charged("pending_storage", request.Key, ref)
		}
		return known()
	case "active":
		if !r.Done || (b.OperationID == r.Source && b.State != "running") {
			return bad()
		}
		if a.Resolution.Destination != "sandbox:"+b.ID || proof == nil ||
			proof.Status != "succeeded" ||
			g.Identity == nil ||
			g.Identity.Generation == nil ||
			g.Identity.Instance == "" ||
			pending != 0 {
			return bad()
		}
		if b.VMID == g.Identity.VM {
			if b.Instance != g.Identity.Instance {
				return bad()
			}
			ref := quotaRef{Kind: "active_use", ID: groupHolder(r)}
			if b.State == "terminated" {
				ref.Kind = "cleanup"
			}
			if err := charged("vm_disk", b.VMID, ref); err != nil {
				return err
			}
			if request.SnapshotID != "" {
				n, err := count(
					`SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`,
					r.TenantID,
					request.SnapshotID,
					"backing:"+b.VMID,
				)
				if err != nil {
					return err
				}
				if n != 1 {
					return bad()
				}
			}
		} else if err := absent(g.Identity.VM); err != nil {
			return err
		}
		return nil
	case "cleanup":
		if proof == nil || g.Identity == nil {
			return bad()
		}
		if err := known(); err != nil {
			return err
		}
		if err := e.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE id=?`, a.Resolution.Destination).
			Scan(&raw); err != nil {
			return errors.Join(bad(), err)
		}
		var collector resourceRecovery
		if err := json.Unmarshal(raw, &collector); err != nil {
			return err
		}
		if collector.Kind != "allocation_cleanup" ||
			collector.Source != r.Source ||
			collector.TenantID != r.TenantID ||
			collector.Done {
			return bad()
		}
		if err := validateAllocationDeletion(collector, r); err != nil {
			return err
		}
		return charged(
			"vm_disk",
			g.Identity.VM,
			quotaRef{Kind: "cleanup", ID: "recovery:" + collector.ID},
		)
	case "reclaimed":
		if proof == nil || pending != 0 {
			return bad()
		}
		if proof.VMID == "" {
			if a.Resolution.Destination != "terminal-no-allocation:"+proof.ID ||
				g.Identity != nil {
				return bad()
			}
		} else {
			if g.Identity == nil ||
				a.Resolution.Destination != "vm-absent:"+proof.VMID {
				return bad()
			}
			if err := absent(proof.VMID); err != nil {
				return err
			}
		}
		return nil
	case "cancelled": //nolint:misspell // Preserve the durable Resolution token.
		if a.Resolution.Destination != "never-dispatched:"+request.Key ||
			step.Dispatched ||
			step.Proof != nil ||
			proof != nil ||
			g.Identity != nil ||
			pending != 0 {
			return bad()
		}
		return nil
	default:
		return bad()
	}
}
