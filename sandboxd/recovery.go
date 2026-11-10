package sandboxd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/woxQAQ/sandbox-engine/api"
)

// A failed client intent remains terminal. Compensating work has its own durable
// intent, and advances its attempt only after a known terminal engine failure.
type resourceRecovery struct {
	ID          string               `json:"id"`
	TenantID    string               `json:"tenant_id,omitempty"`
	MemoryMiB   int64                `json:"memory_mib,omitempty"`
	Source      string               `json:"source"`
	Step        string               `json:"step,omitempty"`
	Kind        string               `json:"kind"`
	SandboxID   string               `json:"sandbox_id,omitempty"`
	VMID        string               `json:"vm_id,omitempty"`
	Snapshot    string               `json:"snapshot_id,omitempty"`
	Instance    string               `json:"instance,omitempty"`
	Attempt     uint64               `json:"attempt"`
	Done        bool                 `json:"done,omitempty"`
	LastError   string               `json:"last_error,omitempty"`
	Allocation  *recoveryAllocation  `json:"allocation,omitempty"`
	Allocations []recoveryAllocation `json:"allocations,omitempty"`
	Release     *releaseIntent       `json:"release_intent,omitempty"`
	Group       *allocationGroup     `json:"allocation_group,omitempty"`
	Deletion    *allocationDeletion  `json:"allocation_deletion,omitempty"`
	Cleanup     *instanceCleanup     `json:"instance_cleanup,omitempty"`
}

func (r resourceRecovery) operation() Operation {
	return Operation{ID: fmt.Sprintf("recovery:%s:%d", r.ID, r.Attempt)}
}

// Take sandbox/checkpoint/pool locks before this lock. Publication/transfer of
// sandbox references shares this lock with GC's final ownership check + delete.
// Allocation and activation stay outside it; pending operation keys protect them.
func (s *Service) lockResourceOwnership() func() { return s.lock("resource-ownership") }

func (s *Service) saveSandboxReference(b Sandbox) error {
	unlock := s.lockResourceOwnership()
	defer unlock()
	return s.saveQuotaSandbox(b)
}

func (s *Service) saveOperationReference(o Operation) error {
	unlock := s.lockResourceOwnership()
	defer unlock()
	return saveOp(s.ctx, s.store.db, o)
}

func (s *Service) saveRecovery(r resourceRecovery) error {
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if r.Kind == "retired_snapshot" {
		if r.Done {
			_, err = tx.ExecContext(
				s.ctx,
				`DELETE FROM quota_refs WHERE holder_id=?`,
				"recovery:"+r.ID,
			)
		} else {
			err = referenceQuotaSnapshot(
				s.ctx,
				tx,
				r.TenantID,
				r.Snapshot,
				quotaRef{Kind: "cleanup", ID: "recovery:" + r.ID},
			)
		}
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(
		s.ctx,
		"INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
		r.ID,
		encode(r),
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) resourceRecoveries() ([]resourceRecovery, error) {
	rows, err := s.store.db.QueryContext(
		s.ctx,
		"SELECT body FROM resource_recovery ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []resourceRecovery
	for rows.Next() {
		var raw []byte
		var r resourceRecovery
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Service) resourceOperations() ([]Operation, error) {
	rows, err := s.store.db.QueryContext(
		s.ctx,
		"SELECT body,spec_hash FROM operations ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Operation
	for rows.Next() {
		var raw []byte
		var hash string
		var o Operation
		if err = rows.Scan(&raw, &hash); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &o); err != nil {
			return nil, err
		}
		o.hash = hash
		out = append(out, o)
	}
	return out, rows.Err()
}

// recoverResources runs after ordinary reconciliation, at startup and on every
// maintenance pass. No failed guest execution or template build is replayed.
func (s *Service) recoverResources() (failure error) {
	defer func() {
		if failure != nil {
			failure = internalError(failure)
		}
	}()
	unlock := s.lock("resource-recovery")
	defer unlock()
	if _, err := s.store.db.ExecContext(
		s.ctx,
		"CREATE TABLE IF NOT EXISTS resource_recovery(id TEXT PRIMARY KEY, body BLOB NOT NULL)",
	); err != nil {
		return err
	}
	groupIntents, err := s.resourceRecoveries()
	if err != nil {
		return err
	}
	// Qualify every retained selection before any group's Engine observation or
	// retirement; ordering by random IDs must not consume another responsibility
	// before a corrupt historical transfer is discovered.
	for _, group := range groupIntents {
		if err = validateWarmSelection(s.ctx, s.store.db, group); err != nil {
			return err
		}
	}
	var groupFailures error
	groupSources := map[string]bool{}
	for _, group := range groupIntents {
		if group.Kind == "allocation_group" {
			groupSources[group.Source] = true
			groupFailures = errors.Join(
				groupFailures,
				s.resolveAllocationGroup(group.ID),
			)
		} else if group.Kind == "allocation_cleanup" && !group.Done {
			groupFailures = errors.Join(
				groupFailures,
				s.consumeAllocationCleanup(group.ID),
			)
		}
	}
	vms, err := s.opts.Engine.ListVMs(s.ctx)
	if err != nil {
		return errors.Join(groupFailures, err)
	}
	snapshots, err := s.opts.Engine.ListSnapshots(s.ctx)
	if err != nil {
		return err
	}
	ops, err := s.resourceOperations()
	if err != nil {
		return err
	}
	byOp := map[string]Operation{}
	for _, o := range ops {
		byOp[o.ID] = o
		if recoveryTerminal(o.Status) && o.ChargeTenantID != "" {
			if err = s.finishQuotaOperation(o); err != nil {
				return err
			}
		}
	}
	all, err := s.store.sandboxes(s.ctx)
	if err != nil {
		return err
	}
	for _, b := range all {
		o, ok := byOp[b.OperationID]
		if !ok || o.Status != "failed" || groupSources[o.ID] {
			continue
		}
		switch b.State {
		case "provisioning", "pausing", "resuming", "terminated":
			r := resourceRecovery{
				ID:        "sandbox:" + b.ID + ":" + o.ID,
				Source:    o.ID,
				Kind:      "sandbox",
				SandboxID: b.ID,
			}
			if _, err = s.store.db.ExecContext(
				s.ctx,
				"INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO NOTHING",
				r.ID,
				encode(r),
			); err != nil {
				return err
			}
		}
	}
	intents, err := s.resourceRecoveries()
	if err != nil {
		return err
	}
	failures := groupFailures
	for _, r := range intents {
		if r.Kind == "sandbox" && (!r.Done || hasUnresolvedAllocations(r)) {
			failures = errors.Join(failures, s.recoverSandboxResource(&r))
		}
	}
	// Only deterministic operation results establish ownership. Clone config
	// labels are inherited and can name another sandbox or an old build VM.
	vmIDs, snapshotIDs := map[string]bool{}, map[string]bool{}
	for _, v := range vms {
		vmIDs[v.ID] = true
	}
	for _, snap := range snapshots {
		snapshotIDs[snap.ID] = true
	}
	for _, o := range ops {
		if groupSources[o.ID] {
			continue
		}
		if o.Status != "failed" && o.Status != "succeeded" {
			continue
		}
		for _, step := range recoveryAllocationSteps(o) {
			engineOp, e := s.findResourceStep(o, step.name, step.action)
			if e != nil {
				failures = errors.Join(failures, e)
				continue
			}
			if engineOp == nil || !recoveryTerminal(engineOp.Status) {
				continue
			}
			r := resourceRecovery{
				Source: o.ID,
				Step:   step.name,
				Kind:   "vm",
				VMID:   engineOp.VMID,
			}
			if step.action == "snapshot" {
				r.Kind, r.VMID, r.Snapshot = "snapshot", "", engineOp.SnapshotID
			}
			if (r.Kind == "vm" && !vmIDs[r.VMID]) ||
				(r.Kind == "snapshot" && !snapshotIDs[r.Snapshot]) {
				continue
			}
			r.ID = r.Kind + ":" + o.ID + ":" + step.name + ":" + r.VMID + r.Snapshot
			if _, e = s.store.db.ExecContext(
				s.ctx,
				"INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO NOTHING",
				r.ID,
				encode(r),
			); e != nil {
				return errors.Join(failures, e)
			}
		}
	}
	intents, err = s.resourceRecoveries()
	if err != nil {
		return errors.Join(failures, err)
	}
	for _, r := range intents {
		if !r.Done && r.Kind == "instance_cleanup" {
			failures = errors.Join(failures, s.recoverInstanceCleanup(&r))
		} else if !r.Done && r.Kind == "retired_snapshot" {
			failures = errors.Join(failures, s.recoverRetiredSnapshot(&r))
		} else if !r.Done && r.Kind != "sandbox" && r.Kind != "instance_cleanup" && r.Kind != "allocation_group" && r.Kind != "allocation_cleanup" {
			failures = errors.Join(failures, s.recoverOrphanResource(&r))
		}
	}
	return errors.Join(failures, s.reconcileQuota())
}

func releaseVMStep(id string) string { return "release-vm:" + id }

type resourceStep struct{ name, action string }

func recoveryAllocationSteps(o Operation) []resourceStep {
	switch o.Command.Action {
	case "template_build":
		return []resourceStep{
			{"build-create", "create"},
			{"build-snapshot", "snapshot"},
		}
	case "claim":
		// Provision can be a cold create or a golden clone.
		return []resourceStep{{"provision", ""}}
	case "refill":
		return []resourceStep{{"warm-clone", "clone"}}
	case "fork":
		out := []resourceStep{{"fork-snapshot", "snapshot"}}
		for _, id := range o.IDs {
			out = append(out, resourceStep{"fork-clone:" + id, "clone"})
		}
		return out
	case "pause":
		return []resourceStep{{"hibernate-snapshot", "snapshot"}}
	case "resume":
		return []resourceStep{{"resume-clone", "clone"}}
	case "rollback":
		return []resourceStep{{"rollback-clone", "clone"}}
	case "checkpoint":
		return []resourceStep{{"checkpoint-capture", "snapshot"}}
	}
	return nil
}

func recoveryTerminal(
	status string,
) bool {
	return status == "succeeded" || status == "failed"
}

// This is a fresh reference read under the ownership lock, never a stale copy of
// the inventory used to discover candidates. Any read error cancels deletion.
func (s *Service) resourceReferences(
	excludeSandbox, excludeOperation string,
) (map[string]bool, map[string]bool, error) {
	vms, snaps := map[string]bool{}, map[string]bool{}
	all, err := s.store.sandboxes(s.ctx)
	if err != nil {
		return nil, nil, err
	}
	bySandbox := map[string]Sandbox{}
	for _, b := range all {
		bySandbox[b.ID] = b
		if b.ID != excludeSandbox {
			vms[b.VMID], snaps[b.SnapshotID] = true, true
		}
	}
	templates, err := s.store.templates(s.ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, t := range templates {
		snaps[t.SnapshotID] = true
	}
	rows, err := s.store.db.QueryContext(
		s.ctx,
		"SELECT vm_id,snapshot_id FROM warm",
	)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var vm, snap string
		if err = rows.Scan(&vm, &snap); err != nil {
			return nil, nil, errors.Join(err, rows.Close())
		}
		vms[vm], snaps[snap] = true, true
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return nil, nil, err
	}
	rows, err = s.store.db.QueryContext(s.ctx, "SELECT body FROM checkpoints")
	if err != nil {
		return nil, nil, err
	}
	checkpoints := map[string]Checkpoint{}
	for rows.Next() {
		var raw []byte
		var cp Checkpoint
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &cp)
		}
		if err != nil {
			return nil, nil, errors.Join(err, rows.Close())
		}
		checkpoints[cp.ID] = cp
		snaps[cp.SnapshotID] = true
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return nil, nil, err
	}
	ops, err := s.resourceOperations()
	if err != nil {
		return nil, nil, err
	}
	for _, o := range ops {
		pending := !recoveryTerminal(o.Status)
		cleanupContinuation := false
		if pending && o.Command.Action == "release" &&
			o.Command.SandboxID == excludeSandbox &&
			excludeSandbox != "" {
			b := bySandbox[excludeSandbox]
			if b.State == "terminated" && b.TenantID == o.ChargeTenantID {
				binding, e := s.originalRelease(b)
				if e != nil {
					return nil, nil, e
				}
				cleanupContinuation = binding != nil
			}
		}
		if pending && o.ID != excludeOperation && !cleanupContinuation {
			snaps[o.SnapshotID] = true
			if o.Command.Template != nil {
				snaps[o.Command.Template.SnapshotID] = true
			}
			cp := checkpoints[o.Command.CheckpointID]
			snaps[cp.SnapshotID] = true
			ids := append([]string{o.Command.SandboxID}, o.IDs...)
			for _, id := range ids {
				b := bySandbox[id]
				vms[b.VMID], snaps[b.SnapshotID] = true, true
			}
		}
		// Even a terminal sandbox operation may have lost contact while an engine
		// step was still running. Protect both sides of every such dependency.
		for _, step := range recoveryDependencySteps(o) {
			op, e := s.findResourceStep(o, step, "*")
			if e != nil {
				return nil, nil, e
			}
			if op != nil &&
				((!recoveryTerminal(op.Status)) || (pending && o.ID != excludeOperation && !cleanupContinuation)) {
				vms[op.VMID], vms[op.Request.VMID] = true, true
				snaps[op.SnapshotID], snaps[op.Request.SnapshotID] = true, true
			}
		}
	}
	rows, err = s.store.db.QueryContext(
		s.ctx,
		`SELECT r.kind,CASE WHEN r.kind='vm_disk' THEN COALESCE((SELECT substr(v.holder_id,4) FROM quota_refs v WHERE v.tenant_id=r.tenant_id AND v.kind=r.kind AND v.resource_key=r.resource_key AND v.holder_id LIKE 'vm:%' LIMIT 1),r.resource_key) ELSE r.resource_key END FROM quota_refs r WHERE r.holder_kind IN ('active_use','engine_dependency') AND r.holder_id NOT LIKE 'backing:%' AND r.holder_id NOT IN (?,?)`,
		"sandbox:"+excludeSandbox,
		"operation:"+excludeOperation,
	)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var kind, key string
		if err = rows.Scan(&kind, &key); err != nil {
			return nil, nil, errors.Join(err, rows.Close())
		}
		switch kind {
		case "snapshot":
			snaps[key] = true
		case "vm_disk":
			vms[key] = true
		}
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, nil, err
	}
	return vms, snaps, nil
}

func recoveryDependencySteps(o Operation) []string {
	var steps []string
	for _, step := range recoveryAllocationSteps(o) {
		steps = append(steps, step.name)
	}
	switch o.Command.Action {
	case "template_build":
		steps = append(steps, "build-start", "build-delete")
	case "claim":
		steps = append(steps, "start")
	case "refill":
		steps = append(steps, "warm-reject", "warm-stale")
	case "pause":
		steps = append(steps, "pause-network", "hibernate-delete")
	case "resume":
		steps = append(steps, "resume-rollback")
	case "rollback":
		steps = append(steps, "rollback-stop", "rollback-restore")
	case "release":
		steps = append(steps, releaseVMStep(o.Command.SandboxID))
	case "fork":
		for _, id := range o.IDs {
			steps = append(steps, releaseVMStep(id))
		}
	}
	for _, id := range append([]string{o.Command.SandboxID}, o.IDs...) {
		if id != "" {
			steps = append(
				steps,
				"network-off:"+id,
				"network-on:"+id,
				"failed-delete:"+id,
			)
		}
	}
	return steps
}

func (s *Service) finishRecoveryAttempt(
	r *resourceRecovery,
	cause error,
	steps ...string,
) error {
	current, err := s.loadRecovery(r.ID)
	if err != nil {
		return err
	}
	*r = current
	if cause == nil {
		progress, err := s.reconcileRecoveryAllocations(r)
		if err != nil {
			return err
		}
		if !progress.AllowedParentDone {
			return nil
		}
		tx, err := s.store.db.BeginTx(s.ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		var raw []byte
		if err = tx.QueryRowContext(s.ctx, `SELECT body FROM resource_recovery WHERE id=?`, r.ID).
			Scan(&raw); err != nil {
			return err
		}
		var latest resourceRecovery
		if err = json.Unmarshal(raw, &latest); err != nil {
			return err
		}
		if hasUnresolvedAllocations(latest) {
			return Fail(
				ErrConflict,
				"parent still owns undelivered allocations",
			)
		}
		latest.Done, latest.LastError = true, ""
		if latest.Kind == "retired_snapshot" {
			if _, err = tx.ExecContext(
				s.ctx,
				`DELETE FROM quota_refs WHERE holder_id=?`,
				"recovery:"+latest.ID,
			); err != nil {
				return err
			}
		}
		if err = writeRecovery(s.ctx, tx, latest); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		*r = latest
		return nil
	} else {
		r.LastError = cause.Error()
		failed, unresolved := false, false
		for _, step := range steps {
			op, err := s.findResourceStep(r.operation(), step, "*")
			if err != nil || (op != nil && !recoveryTerminal(op.Status)) {
				unresolved = true
			}
			if op != nil && op.Status == "failed" {
				failed = true
			}
		}
		if failed && !unresolved {
			r.Attempt++
		}
	}
	return errors.Join(cause, s.saveRecovery(*r))
}

func (s *Service) recoverOrphanResource(r *resourceRecovery) error {
	unlock := s.lockResourceOwnership()
	defer unlock()
	if r.Kind == "snapshot" {
		// Retired cleanup has the durable charge and deletion attempt. A generic
		// collector must not start another key while that responsibility is live.
		var retired int
		if err := s.store.db.QueryRowContext(s.ctx, `SELECT count(*) FROM resource_recovery
            WHERE json_extract(body,'$.kind')='retired_snapshot'
            AND json_extract(body,'$.snapshot_id')=?
            AND COALESCE(json_extract(body,'$.done'),0)=0`, r.Snapshot).
			Scan(&retired); err != nil {
			return err
		}
		if retired > 0 {
			return nil
		}
	}
	o, err := s.store.operation(s.ctx, r.Source)
	if err != nil || !recoveryTerminal(o.Status) {
		return err
	}
	action := "*"
	if r.Kind == "snapshot" {
		action = "snapshot"
	}
	proofOperation := o
	if r.Allocation != nil {
		proofOperation = r.Allocation.operation()
	}
	proof, err := s.findResourceStep(proofOperation, r.Step, action)
	if err == nil && proof != nil && r.Allocation != nil {
		err = r.Allocation.validate(*proof)
	}
	if err != nil {
		return err
	}
	if proof == nil || !recoveryTerminal(proof.Status) ||
		(r.Kind == "vm" && proof.VMID != r.VMID) ||
		(r.Kind == "snapshot" && proof.SnapshotID != r.Snapshot) {
		return nil
	}
	vms, snaps, err := s.resourceReferences("", "")
	if err != nil {
		return err
	}
	req := api.Request{Action: "delete", VMID: r.VMID}
	if r.Kind == "vm" {
		if vms[r.VMID] {
			return nil
		}
		v, e := s.opts.Engine.GetVM(s.ctx, r.VMID)
		if isEngineCode(e, api.NotFound) {
			if e = s.reclaimQuota("vm_disk", r.VMID); e != nil {
				return e
			}
			return s.finishRecoveryAttempt(r, nil)
		}
		if e != nil {
			return e
		}
		if !v.Verified {
			return Fail(
				ErrManualRecovery,
				"orphan VM %s is not verified",
				v.ID,
			)
		}
		if r.Instance != "" && r.Instance != v.Instance {
			return Fail(
				ErrManualRecovery,
				"orphan VM %s changed instance",
				v.ID,
			)
		}
		r.Instance = v.Instance
		req.ExpectedInstance = r.Instance
	} else {
		if snaps[r.Snapshot] {
			return nil
		}
		_, e := s.opts.Engine.GetSnapshot(s.ctx, r.Snapshot)
		if isEngineCode(e, api.NotFound) {
			return s.finishRecoveryAttempt(r, nil)
		}
		if e != nil {
			return e
		}
		req = api.Request{Action: "delete_snapshot", SnapshotID: r.Snapshot}
	}
	if err = s.saveRecovery(*r); err != nil {
		return err
	}
	_, err = s.engine(r.operation(), "orphan-delete", req)
	return s.finishRecoveryAttempt(r, err, "orphan-delete")
}

func (s *Service) findResourceStep(
	o Operation,
	step, action string,
) (*api.Operation, error) {
	key := stepKey(o, step)
	v, err := s.opts.Engine.FindOperation(s.ctx, key)
	if isEngineCode(err, api.NotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if v.Key != key ||
		(action != "" && action != "*" && v.Request.Action != action) ||
		(action == "" && v.Request.Action != "create" && v.Request.Action != "clone") {
		return nil, Fail(
			ErrManualRecovery,
			"engine operation %s does not prove resource ownership",
			key,
		)
	}
	return &v, nil
}

type cleanupAttempt struct {
	Request api.Request   `json:"request"`
	Receipt api.Operation `json:"receipt"`
}

func writeRecovery(ctx context.Context, e executor, r resourceRecovery) error {
	if err := validateWarmSelection(ctx, e, r); err != nil {
		return err
	}
	result, err := e.ExecContext(
		ctx,
		`UPDATE resource_recovery SET body=? WHERE id=?`,
		encode(r),
		r.ID,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return Fail(
			ErrManualRecovery,
			"recovery responsibility was not persisted",
		)
	}
	return nil
}

func (s *Service) loadRecovery(id string) (resourceRecovery, error) {
	return loadRecoveryTx(s.ctx, s.store.db, id)
}

func loadRecoveryTx(
	ctx context.Context,
	e executor,
	id string,
) (resourceRecovery, error) {
	var raw []byte
	var r resourceRecovery
	err := e.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE id=?`, id).
		Scan(&raw)
	if err == nil {
		err = json.Unmarshal(raw, &r)
	}
	if err == nil {
		err = validateWarmSelection(ctx, e, r)
	}
	return r, err
}

func insertRecovery(ctx context.Context, e executor, r resourceRecovery) error {
	if _, err := e.ExecContext(
		ctx,
		`INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO NOTHING`,
		r.ID,
		encode(r),
	); err != nil {
		return err
	}
	var actual []byte
	if err := e.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE id=?`, r.ID).
		Scan(&actual); err != nil {
		return err
	}
	if !bytes.Equal(actual, encode(r)) {
		return Fail(
			ErrManualRecovery,
			"recovery identity conflicts with durable responsibility",
		)
	}
	return nil
}
