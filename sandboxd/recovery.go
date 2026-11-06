package sandboxd

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

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
	Cleanup     *instanceCleanup     `json:"instance_cleanup,omitempty"`
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
		o.Hash = hash
		out = append(out, o)
	}
	return out, rows.Err()
}

// recoverResources runs after ordinary reconciliation, at startup and on every
// maintenance pass. No failed guest execution or template build is replayed.
func (s *Service) recoverResources() error {
	unlock := s.lock("resource-recovery")
	defer unlock()
	if _, err := s.store.db.ExecContext(
		s.ctx,
		"CREATE TABLE IF NOT EXISTS resource_recovery(id TEXT PRIMARY KEY, body BLOB NOT NULL)",
	); err != nil {
		return err
	}
	vms, err := s.opts.Engine.ListVMs(s.ctx)
	if err != nil {
		return err
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
		if !ok || o.Status != "failed" {
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
	var failures error
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
		} else if !r.Done && r.Kind != "sandbox" && r.Kind != "instance_cleanup" {
			failures = errors.Join(failures, s.recoverOrphanResource(&r))
		}
	}
	return errors.Join(failures, s.reconcileQuota())
}

// commitRunning transfers the sleep reference and its storage charge together.
// The caller holds the sandbox lock; no db query is opened inside the transaction.
func (s *Service) commitRunning(
	o Operation,
	b *Sandbox,
	exact ...string,
) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	next := *b
	target := ""
	if len(exact) > 0 {
		target = exact[0]
	}
	if next.SnapshotID == "" && target == "" {
		return saveSandbox(s.ctx, s.store.db, next)
	}
	if next.SnapshotID != "" {
		if err := s.proveRetiredSnapshot(next); err != nil {
			return err
		}
	}
	r := resourceRecovery{
		ID: "retired:" + next.ID + ":" + next.SnapshotID, Source: o.ID,
		Kind: "retired_snapshot", SandboxID: next.ID, TenantID: next.TenantID,
		Snapshot: next.SnapshotID, MemoryMiB: next.Shape.MemoryMiB,
	}
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if target != "" {
		if err = s.commitAllocationActiveTx(tx, o, next, target); err != nil {
			return err
		}
	}
	if r.Snapshot != "" {
		if _, err = tx.ExecContext(
			s.ctx,
			"INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO NOTHING",
			r.ID,
			encode(r),
		); err != nil {
			return err
		}
		next.SnapshotID = ""
		next.PausedUntil = time.Time{}
		if _, err = tx.ExecContext(
			s.ctx,
			`UPDATE OR IGNORE quota_refs SET holder_kind='cleanup' WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_id=?`,
			next.TenantID,
			r.Snapshot,
			"sandbox:"+next.ID,
		); err != nil {
			return err
		}
		if _, err = tx.ExecContext(
			s.ctx,
			`DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_id=? AND holder_kind!='cleanup'`,
			next.TenantID,
			r.Snapshot,
			"sandbox:"+next.ID,
		); err != nil {
			return err
		}
		if err = putQuotaRef(
			s.ctx,
			tx,
			next.TenantID,
			"snapshot",
			r.Snapshot,
			quotaRef{Kind: "cleanup", ID: "recovery:" + r.ID},
		); err != nil {
			return err
		}
	}
	if err = saveSandbox(s.ctx, tx, next); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*b = next
	return nil
}

func (s *Service) proveRetiredSnapshot(b Sandbox) error {
	if err := requireSnapshotOwner(
		s.ctx,
		s.store.db,
		b.SnapshotID,
		b.TenantID,
	); err != nil {
		if isCode(err, api.NotFound) {
			return api.Fail(
				api.ManualRecovery,
				"sleep snapshot provenance cannot be proven",
			)
		}
		return err
	}
	return nil
}

func (s *Service) recoverRetiredSnapshot(r *resourceRecovery) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	_, err := s.opts.Engine.GetSnapshot(s.ctx, r.Snapshot)
	if isCode(err, api.NotFound) {
		if err = s.reclaimQuota("snapshot", r.Snapshot); err != nil {
			return err
		}
		return s.finishRecoveryAttempt(r, nil)
	}
	if err != nil {
		return err
	}
	// Duplicate handoffs share one deletion sequence, while all retain their
	// persisted charges until physical absence is confirmed.
	var collector string
	if err = s.store.db.QueryRowContext(s.ctx, `SELECT id FROM resource_recovery
        WHERE json_extract(body,'$.kind')='retired_snapshot'
        AND json_extract(body,'$.snapshot_id')=?
        AND COALESCE(json_extract(body,'$.done'),0)=0 ORDER BY id LIMIT 1`, r.Snapshot).Scan(&collector); err != nil {
		return err
	}
	if collector != r.ID {
		return nil
	}
	o := r.operation()
	if err = s.retireSnapshotOwnedExcept(
		o,
		r.Snapshot,
		"",
		"",
		r.Source,
	); err != nil {
		if isCode(err, api.NotFound) {
			return s.finishRecoveryAttempt(r, nil)
		}
		return s.finishRecoveryAttempt(r, err, "retire-snapshot:"+r.Snapshot)
	}
	// A shared live reference leaves both the intent and its charge intact.
	_, err = s.opts.Engine.GetSnapshot(s.ctx, r.Snapshot)
	if isCode(err, api.NotFound) {
		return s.finishRecoveryAttempt(r, nil)
	}
	return err
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
			return api.Fail(
				api.Conflict,
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
		if isCode(e, api.NotFound) {
			if e = s.reclaimQuota("vm_disk", r.VMID); e != nil {
				return e
			}
			return s.finishRecoveryAttempt(r, nil)
		}
		if e != nil {
			return e
		}
		if !v.Verified {
			return api.Fail(
				api.ManualRecovery,
				"orphan VM %s is not verified",
				v.ID,
			)
		}
		if r.Instance != "" && r.Instance != v.Instance {
			return api.Fail(
				api.ManualRecovery,
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
		if isCode(e, api.NotFound) {
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

func (s *Service) recoverSandboxResource(r *resourceRecovery) error {
	unlock := s.lock(r.SandboxID)
	defer unlock()
	progress, err := s.reconcileRecoveryAllocations(r)
	if err != nil {
		return err
	}
	if !progress.AllowedLifecycleWork && !progress.AllowedParentDone {
		return nil
	}
	b, err := getSandbox(s.ctx, s.store.db, r.SandboxID)
	if isCode(err, api.NotFound) {
		return s.finishRecoveryAttempt(r, nil)
	}
	if err != nil {
		return err
	}
	source, err := s.store.operation(s.ctx, r.Source)
	if err != nil || source.Status != "failed" {
		return err
	}
	owned := b.OperationID == source.ID ||
		strings.HasPrefix(b.OperationID, "recovery:"+r.ID+":")
	if r.Release != nil && b.State == "terminated" &&
		b.TenantID == r.Release.Tenant &&
		b.VMID == r.Release.VM &&
		b.Instance == r.Release.Instance {
		owned = true
	}
	if !owned {
		// Activation can commit running before retiring the old sleep snapshot.
		if b.State == "running" && r.Instance != "" &&
			b.Instance == r.Instance {
			o := r.operation()
			snapshot := b.SnapshotID
			err = s.finishResume(&o, &b)
			return s.finishRecoveryAttempt(r, err, "retire-snapshot:"+snapshot)
		}
		return s.finishRecoveryAttempt(r, nil)
	}
	if b.State == "provisioning" && b.VMID == "" {
		step := "provision"
		if source.Command.Action == "fork" {
			step = "fork-clone:" + b.ID
		}
		proof, e := s.findResourceStep(source, step, "")
		if e != nil {
			return e
		}
		if proof != nil {
			if !recoveryTerminal(proof.Status) {
				return nil
			}
			b.VMID = proof.VMID
			if err = saveSandbox(s.ctx, s.store.db, b); err != nil {
				return err
			}
		}
	}
	// A queued operation on this sandbox owns its dependencies until it finishes.
	ownership := s.lockResourceOwnership()
	vms, _, err := s.resourceReferences(b.ID, source.ID)
	ownership()
	if err != nil {
		return err
	}
	if b.VMID != "" && vms[b.VMID] {
		return nil
	}
	o := r.operation()
	o.TenantID, o.ChargeTenantID, o.Command = source.TenantID, source.ChargeTenantID, source.Command
	switch b.State {
	case "provisioning", "terminated":
		cleanup := &o
		if b.VMID != "" &&
			(source.Command.Action == "fork" || source.Command.Action == "release") {
			deletion, e := s.findResourceStep(
				source,
				releaseVMStep(b.ID),
				"delete",
			)
			if e != nil {
				return e
			}
			if deletion != nil {
				if deletion.Request.VMID != b.VMID ||
					deletion.Request.ExpectedInstance != b.Instance {
					return api.Fail(
						api.ManualRecovery,
						"original release does not match the sandbox instance",
					)
				}
				if !recoveryTerminal(deletion.Status) {
					return nil
				}
				if deletion.Status == "succeeded" {
					_, e = s.opts.Engine.GetVM(s.ctx, b.VMID)
					if e == nil {
						return api.Fail(
							api.ManualRecovery,
							"successful deletion still has a live VM",
						)
					}
					if !isCode(e, api.NotFound) {
						return e
					}
				}
			}
			// Unknown dispatch reuses the original key. Only a proven terminal
			// engine failure permits the compensation intent's distinct key.
			if deletion == nil || deletion.Status != "failed" {
				cleanup = &source
			}
		}
		// Release is compensating work; it never retries activation or guest exec.
		err = s.release(cleanup, &b)
		return s.finishRecoveryAttempt(
			r,
			err,
			releaseVMStep(b.ID),
			"retire-snapshot:"+b.SnapshotID,
		)
	case "pausing":
		if source.Command.Action != "pause" {
			return api.Fail(
				api.ManualRecovery,
				"pausing sandbox has no pause intent",
			)
		}
		if b.SnapshotID == "" {
			proof, e := s.findResourceStep(
				source,
				"hibernate-snapshot",
				"snapshot",
			)
			if e != nil {
				return e
			}
			if proof != nil && !recoveryTerminal(proof.Status) {
				return nil
			}
			if proof != nil && proof.Status == "succeeded" &&
				proof.Request.VMID == b.VMID &&
				proof.Request.ExpectedInstance == b.Instance {
				b.SnapshotID = proof.SnapshotID
				ownership := s.lockResourceOwnership()
				err = recordSnapshotOwner(
					s.ctx,
					s.store.db,
					b.SnapshotID,
					b.TenantID,
					source.ID,
					"captured",
				)
				ownership()
				if err != nil {
					return err
				}
				if err = saveSandbox(s.ctx, s.store.db, b); err != nil {
					return err
				}
			}
		}
		if b.SnapshotID != "" {
			if err = s.recoverySnapshotReady(b.SnapshotID); err != nil {
				return err
			}
		}
		v, e := s.opts.Engine.GetVM(s.ctx, b.VMID)
		if isCode(e, api.NotFound) && b.SnapshotID != "" {
			if err = s.reclaimQuota("vm_disk", b.VMID); err != nil {
				return err
			}
			b.State, b.VMID, b.Instance, b.OperationID = "paused", "", "", ""
			b.PausedUntil = time.Now().UTC().Add(s.opts.PausedTTL)
			return s.finishRecoveryAttempt(r, saveSandbox(s.ctx, s.store.db, b))
		}
		if e != nil {
			return e
		}
		if !v.Verified || (v.Phase != "running" && b.SnapshotID == "") ||
			b.Instance == "" ||
			v.Instance != b.Instance {
			return api.Fail(
				api.ManualRecovery,
				"cannot prove the original hibernating VM instance",
			)
		}
		if b.SnapshotID == "" {
			if err = s.admitQuotaRecovery(
				r,
				o,
				b,
				"hibernate-snapshot",
				"snapshot",
				api.Request{
					Action:           "snapshot",
					VMID:             b.VMID,
					ExpectedInstance: b.Instance,
				},
				b.Shape.DiskMiB+b.Shape.MemoryMiB,
			); err != nil {
				return err
			}
		}
		err = s.pause(&o, &b)
		return s.finishRecoveryAttempt(
			r,
			err,
			"pause-network",
			"hibernate-snapshot",
			"hibernate-delete",
		)
	case "resuming":
		if source.Command.Action == "rollback" {
			err = s.recoverRollback(r, source, &b)
			return s.finishRecoveryAttempt(
				r,
				err,
				"rollback-restore",
				"rollback-clone",
				"resume-cleanup",
				"network-off:"+b.ID,
				"network-on:"+b.ID,
				"retire-snapshot:"+b.SnapshotID,
			)
		}
		if source.Command.Action != "resume" {
			return api.Fail(
				api.ManualRecovery,
				"resuming sandbox has no resume or rollback intent",
			)
		}
		err = s.recoverFailedClone(r, source, &b, "resume-clone")
		return s.finishRecoveryAttempt(r, err, "resume-cleanup")
	default:
		return s.finishRecoveryAttempt(r, nil)
	}
}

func (s *Service) recoverySnapshotReady(id string) error {
	snap, err := s.opts.Engine.GetSnapshot(s.ctx, id)
	if err != nil {
		return err
	}
	if !snap.Published || snap.Deleting {
		return api.Fail(
			api.Conflict,
			"recovery snapshot %s is not published",
			id,
		)
	}
	return nil
}

func (s *Service) recoverFailedClone(
	r *resourceRecovery,
	source Operation,
	b *Sandbox,
	cloneStep string,
) error {
	if err := s.recoverySnapshotReady(b.SnapshotID); err != nil {
		return err
	}
	proof, err := s.findResourceStep(source, cloneStep, "clone")
	if err != nil {
		return err
	}
	if proof != nil && !recoveryTerminal(proof.Status) {
		return api.Fail(api.Conflict, "resume clone is still running")
	}
	if b.VMID != "" && (proof == nil || proof.VMID != b.VMID) {
		return api.Fail(
			api.ManualRecovery,
			"failed resume VM has no matching clone receipt",
		)
	}
	vmid := b.VMID
	if proof != nil {
		vmid = proof.VMID
	}
	s.closeConnections(b.ID)
	if err = s.revoke(b.ID); err != nil {
		return err
	}
	if vmid != "" {
		unlock := s.lockResourceOwnership()
		defer unlock()
		vms, _, e := s.resourceReferences(b.ID, source.ID)
		if e != nil {
			return e
		}
		if vms[vmid] {
			return api.Fail(api.Conflict, "failed resume VM has another owner")
		}
		v, e := s.opts.Engine.GetVM(s.ctx, vmid)
		if e != nil && !isCode(e, api.NotFound) {
			return e
		}
		if isCode(e, api.NotFound) {
			if reclaimErr := s.reclaimQuota(
				"vm_disk",
				vmid,
			); reclaimErr != nil {
				return reclaimErr
			}
		}
		if e == nil {
			if !v.Verified || (b.Instance != "" && b.Instance != v.Instance) ||
				(r.Instance != "" && r.Instance != v.Instance) {
				return api.Fail(
					api.ManualRecovery,
					"failed resume VM instance cannot be proven",
				)
			}
			r.VMID, r.Instance = vmid, v.Instance
			if err = s.saveRecovery(*r); err != nil {
				return err
			}
			if _, err = s.engine(
				r.operation(),
				"resume-cleanup",
				api.Request{
					Action:           "delete",
					VMID:             vmid,
					ExpectedInstance: v.Instance,
				},
			); err != nil {
				return err
			}
		}
	}
	b.State, b.VMID, b.Instance, b.OperationID = "paused", "", "", ""
	b.ResumeAttempt++
	b.LastError = "resume failed; restored the retained sleep snapshot"
	return saveSandbox(s.ctx, s.store.db, *b)
}

// Rollback can have reused a VM ID. A resume-clone receipt never authorizes
// deleting that target. Only retry restore on a proven, already stopped source;
// an unproven running instance is left for explicit operator recovery.
func (s *Service) recoverRollback(
	r *resourceRecovery,
	source Operation,
	b *Sandbox,
) error {
	if source.Step == "rollback_clone" {
		// Rollback began asleep: retain its original sleep capture and discard
		// only the clone made for the failed checkpoint activation.
		return s.recoverFailedClone(r, source, b, "rollback-clone")
	}
	checkpointUnlock := s.lock("checkpoint:" + source.Command.CheckpointID)
	defer checkpointUnlock()
	cp, err := s.store.checkpoint(s.ctx, source.Command.CheckpointID)
	if err != nil {
		return err
	}
	if cp.SandboxID != b.ID || cp.TenantID != b.TenantID {
		return api.Fail(
			api.ManualRecovery,
			"rollback checkpoint ownership does not match",
		)
	}
	if err = requireSnapshotOwner(
		s.ctx,
		s.store.db,
		cp.SnapshotID,
		cp.TenantID,
	); err != nil {
		return err
	}
	if err = s.recoverySnapshotReady(cp.SnapshotID); err != nil {
		return err
	}
	if s.CredentialsMissing(*b) {
		return api.Fail(
			api.Conflict,
			"host credentials must be supplied before rollback recovery",
		)
	}
	o := r.operation()
	o.Command, o.TenantID, o.ChargeTenantID = source.Command, source.TenantID, source.ChargeTenantID
	if b.VMID == "" {
		return api.Fail(
			api.ManualRecovery,
			"rollback target is missing; refusing an unproven replacement",
		)
	}
	v, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
	if err != nil {
		return err
	}
	if !v.Verified {
		return api.Fail(api.ManualRecovery, "rollback VM is not verified")
	}
	target := ""
	for _, a := range r.Allocations {
		if a.Step == "rollback-restore" &&
			a.Resolution.State == "result_known" &&
			a.Resolution.Proof != nil {
			target = a.Resolution.Proof.ResultInstance
			o.ID = a.OperationID
		}
	}
	if target != "" && v.Instance != target {
		return api.Fail(api.ManualRecovery, "restore result instance changed")
	}
	if target == "" && v.Phase == "stopped" && source.SourceInstance != "" &&
		v.Instance == source.SourceInstance {
		stop, e := s.findResourceStep(source, "rollback-stop", "stop")
		if e != nil {
			return e
		}
		if stop == nil || stop.Status != "succeeded" || stop.VMID != v.ID ||
			stop.Request.ExpectedInstance != v.Instance {
			return api.Fail(
				api.ManualRecovery,
				"rollback stop receipt does not prove the stopped source",
			)
		}
		if err = s.admitQuotaRecovery(
			r,
			o,
			*b,
			"rollback-restore",
			"vm_disk",
			api.Request{
				Action:           "restore",
				VMID:             v.ID,
				SnapshotID:       cp.SnapshotID,
				ExpectedInstance: v.Instance,
			},
			b.Shape.DiskMiB,
		); err != nil {
			return err
		}
		_, err = s.engine(
			o,
			"rollback-restore",
			api.Request{
				Action:           "restore",
				VMID:             v.ID,
				SnapshotID:       cp.SnapshotID,
				ExpectedInstance: v.Instance,
			},
		)
		if err != nil {
			return err
		}
		if _, err = s.reconcileRecoveryAllocations(r); err != nil {
			return err
		}
		for _, a := range r.Allocations {
			if a.OperationID == o.ID && a.Step == "rollback-restore" &&
				a.Resolution.Proof != nil {
				target = a.Resolution.Proof.ResultInstance
			}
		}
		v, err = s.opts.Engine.GetVM(s.ctx, b.VMID)
		if err != nil {
			return err
		}
	}
	if target == "" {
		// Preserve ordinary public activation recovery's independent network proof;
		// it does not mint a compensation allocation or a new generation.
		activation, e := s.findResourceStep(
			source,
			"network-off:"+b.ID,
			"network",
		)
		if e != nil {
			return e
		}
		if activation == nil || activation.Status != "succeeded" ||
			activation.VMID != v.ID ||
			activation.Request.ExpectedInstance == "" ||
			activation.Request.ExpectedInstance != v.Instance ||
			!v.Verified ||
			!v.AgentReady ||
			v.Phase != "running" {
			return api.Fail(
				api.ManualRecovery,
				"rollback running instance cannot be proven",
			)
		}
		r.Instance = activation.Request.ExpectedInstance
		if err = s.saveRecovery(*r); err != nil {
			return err
		}
		if err = s.activate(o, b, false); err != nil {
			return err
		}
		return s.finishResume(&o, b)
	}
	if v.Instance != target || v.OperationID != "" {
		return api.Fail(
			api.ManualRecovery,
			"rollback result has another instance or owner",
		)
	}
	r.Instance = target
	if v.Phase != "running" || !v.AgentReady || !v.Verified {
		return api.Fail(
			api.ManualRecovery,
			"rollback restore is not verified running",
		)
	}
	if err = s.saveRecovery(*r); err != nil {
		return err
	}
	if err = s.activateTarget(o, b, false, target); err != nil {
		return err
	}
	return s.finishResume(&o, b)
}

func (s *Service) findResourceStep(
	o Operation,
	step, action string,
) (*api.Operation, error) {
	key := stepKey(o, step)
	v, err := s.opts.Engine.FindOperation(s.ctx, key)
	if isCode(err, api.NotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if v.Key != key ||
		(action != "" && action != "*" && v.Request.Action != action) ||
		(action == "" && v.Request.Action != "create" && v.Request.Action != "clone") {
		return nil, api.Fail(
			api.ManualRecovery,
			"engine operation %s does not prove resource ownership",
			key,
		)
	}
	return &v, nil
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
type releaseIntent struct {
	Tenant     string      `json:"tenant"`
	Sandbox    string      `json:"sandbox"`
	VM         string      `json:"vm"`
	Instance   string      `json:"instance"`
	Source     Operation   `json:"source"`
	Request    api.Request `json:"request"`
	Dispatched bool        `json:"dispatched"`
}
type cleanupAttempt struct {
	Request api.Request   `json:"request"`
	Receipt api.Operation `json:"receipt"`
}
type instanceCleanup struct {
	Original   releaseIntent      `json:"original"`
	Allocation recoveryAllocation `json:"allocation"`
	Request    api.Request        `json:"request"`
	Dispatched bool               `json:"dispatched"`
	History    []cleanupAttempt   `json:"history,omitempty"`
	DoneProof  *api.Operation     `json:"done_proof,omitempty"`
	Absent     bool               `json:"absent,omitempty"`
}

func hasUnresolvedAllocations(r resourceRecovery) bool {
	for _, a := range r.Allocations {
		if !a.Resolution.delivered() {
			return true
		}
	}
	return false
}

func writeRecovery(ctx context.Context, e executor, r resourceRecovery) error {
	_, err := e.ExecContext(
		ctx,
		`UPDATE resource_recovery SET body=? WHERE id=?`,
		encode(r),
		r.ID,
	)
	return err
}

func (s *Service) loadRecovery(id string) (resourceRecovery, error) {
	var raw []byte
	var r resourceRecovery
	err := s.store.db.QueryRowContext(s.ctx, `SELECT body FROM resource_recovery WHERE id=?`, id).
		Scan(&raw)
	if err == nil {
		err = json.Unmarshal(raw, &r)
	}
	return r, err
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
			return api.Fail(api.ManualRecovery, "allocation receipt changed")
		}
		next.Resolution = allocationResolution{
			State: "result_known",
			Proof: &proof,
		}
		return writeRecovery(s.ctx, tx, r)
	}
	return api.Fail(api.ManualRecovery, "missing allocation responsibility")
}

func instanceCollectorID(tenant, sandbox, vm, instance string) string {
	h := sha256.New()
	for _, part := range []string{"plan012-instance-cleanup-v1", tenant, sandbox, vm, instance} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	return "instance-cleanup:" + hex.EncodeToString(h.Sum(nil))
}

func (s *Service) originalRelease(b Sandbox) (*resourceRecovery, error) {
	all, err := s.resourceRecoveries()
	if err != nil {
		return nil, err
	}
	var found *resourceRecovery
	for _, r := range all {
		if r.Release == nil || r.Release.Sandbox != b.ID ||
			r.Release.Tenant != b.TenantID ||
			r.Release.VM != b.VMID {
			continue
		}
		if found != nil && !reflect.DeepEqual(found.Release, r.Release) {
			return nil, api.Fail(
				api.ManualRecovery,
				"conflicting release origins",
			)
		}
		next := r
		found = &next
	}
	return found, nil
}

func (s *Service) boundCollector(b Sandbox) (*resourceRecovery, error) {
	all, err := s.resourceRecoveries()
	if err != nil {
		return nil, err
	}
	var found *resourceRecovery
	for _, r := range all {
		if r.Kind != "instance_cleanup" || r.SandboxID != b.ID ||
			r.TenantID != b.TenantID ||
			(b.VMID != "" && r.VMID != b.VMID) {
			continue
		}
		if found != nil && found.ID != r.ID {
			return nil, api.Fail(
				api.ManualRecovery,
				"conflicting instance collectors",
			)
		}
		next := r
		found = &next
	}
	return found, nil
}

// Freeze A before dispatch with the access revocation and cleanup references.
func (s *Service) freezeRelease(
	o Operation,
	b *Sandbox,
) (*resourceRecovery, error) {
	origin, err := s.originalRelease(*b)
	if err != nil {
		return nil, err
	}
	next := *b
	next.State = "terminated"
	next.OperationID = o.ID
	if origin == nil && b.VMID != "" {
		if strings.HasPrefix(o.ID, "recovery:") {
			all, e := s.resourceRecoveries()
			if e != nil {
				return nil, e
			}
			for _, r := range all {
				if r.operation().ID == o.ID && r.SandboxID == b.ID {
					next := r
					origin = &next
					break
				}
			}
		}
		req := api.Request{
			Action:           "delete",
			VMID:             b.VMID,
			ExpectedInstance: b.Instance,
			Key:              stepKey(o, releaseVMStep(b.ID)),
		}
		if origin == nil {
			origin = &resourceRecovery{
				ID:        "sandbox:" + b.ID + ":" + o.ID,
				Kind:      "sandbox",
				Source:    o.ID,
				SandboxID: b.ID,
				TenantID:  b.TenantID,
				VMID:      b.VMID,
				Instance:  b.Instance,
			}
		}
		origin.Release = &releaseIntent{
			Tenant:   b.TenantID,
			Sandbox:  b.ID,
			VM:       b.VMID,
			Instance: b.Instance,
			Source:   o,
			Request:  req,
		}
	}
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err = quotaTerminate(s.ctx, tx, next); err != nil {
		return nil, err
	}
	if err = saveSandbox(s.ctx, tx, next); err != nil {
		return nil, err
	}
	if origin != nil {
		_, err = tx.ExecContext(
			s.ctx,
			`INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO NOTHING`,
			origin.ID,
			encode(origin),
		)
		if err == nil {
			err = writeRecovery(s.ctx, tx, *origin)
		}
		if err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	*b = next
	return origin, nil
}

func (s *Service) handoffInstanceCleanup(
	parent *resourceRecovery,
	index int,
	b Sandbox,
	v api.VM,
) error {
	a := parent.Allocations[index]
	proof := a.Resolution.Proof
	if proof == nil || proof.Status != "succeeded" ||
		proof.ResultInstance == "" ||
		b.State != "terminated" ||
		b.VMID != v.ID ||
		v.Instance != proof.ResultInstance ||
		!v.Verified ||
		v.OperationID != "" ||
		b.TenantID != a.ChargeTenantID {
		return api.Fail(api.ManualRecovery, "invalid exact instance handoff")
	}
	origin, err := s.originalRelease(b)
	if err != nil {
		return err
	}
	if origin == nil || origin.Release == nil {
		return api.Fail(api.ManualRecovery, "missing frozen release intent")
	}
	original := *origin.Release
	source, err := s.store.operation(s.ctx, origin.Source)
	if err != nil {
		return err
	}
	if source.Status != "failed" || source.Command.Action != "release" ||
		source.Command.SandboxID != b.ID ||
		source.ChargeTenantID != b.TenantID ||
		original.Instance != a.Request.ExpectedInstance ||
		original.Request.VMID != v.ID ||
		original.Request.ExpectedInstance != original.Instance ||
		original.Request.Key != stepKey(original.Source, releaseVMStep(b.ID)) {
		return api.Fail(
			api.ManualRecovery,
			"release origin does not match restore proof",
		)
	}
	deletion, err := s.opts.Engine.FindOperation(s.ctx, original.Request.Key)
	if isCode(err, api.NotFound) {
		// This is a generation fence proof, never a fabricated failed receipt or a
		// claim that NotFound means no dispatch. Replaying A cannot admit against B.
		_, err = api.Wait(s.ctx, s.opts.Engine, original.Request)
		if !isCode(err, api.Conflict) {
			if err == nil {
				err = api.Fail(
					api.ManualRecovery,
					"unproven A deletion outcome",
				)
			}
			return err
		}
	} else if err != nil {
		return err
	} else if !reflect.DeepEqual(deletion.Request, original.Request) || deletion.Key != original.Request.Key || deletion.Status != "failed" {
		return api.Fail(
			api.ManualRecovery,
			"A deletion is unresolved or contradicts generation proof",
		)
	}
	id := instanceCollectorID(b.TenantID, b.ID, v.ID, v.Instance)
	collector := resourceRecovery{
		ID:        id,
		Kind:      "instance_cleanup",
		Source:    origin.Source,
		TenantID:  b.TenantID,
		SandboxID: b.ID,
		VMID:      v.ID,
		Instance:  v.Instance,
	}
	req := api.Request{
		Action:           "delete",
		VMID:             v.ID,
		ExpectedInstance: v.Instance,
		Key:              stepKey(collector.operation(), releaseVMStep(b.ID)),
	}
	collector.Cleanup = &instanceCleanup{
		Original:   original,
		Allocation: a,
		Request:    req,
	}
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	latest, err := getSandbox(s.ctx, tx, b.ID)
	if err != nil {
		return err
	}
	if latest.State != "terminated" || latest.TenantID != b.TenantID ||
		latest.VMID != v.ID ||
		(latest.Instance != original.Instance && latest.Instance != v.Instance) {
		return api.Fail(api.ManualRecovery, "sandbox changed before handoff")
	}
	var raw []byte
	err = tx.QueryRowContext(s.ctx, `SELECT body FROM resource_recovery WHERE id=?`, id).
		Scan(&raw)
	if err == nil {
		var old resourceRecovery
		if err = json.Unmarshal(raw, &old); err != nil {
			return err
		}
		if old.Cleanup == nil || old.Source != collector.Source ||
			old.TenantID != collector.TenantID ||
			old.SandboxID != collector.SandboxID ||
			old.VMID != collector.VMID ||
			old.Instance != collector.Instance ||
			!reflect.DeepEqual(old.Cleanup.Original, original) ||
			!reflect.DeepEqual(old.Cleanup.Allocation.Request, a.Request) ||
			!reflect.DeepEqual(old.Cleanup.Allocation.Resolution.Proof, proof) {
			return api.Fail(api.ManualRecovery, "collector binding conflict")
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(
			s.ctx,
			`INSERT INTO resource_recovery(id,body) VALUES(?,?)`,
			id,
			encode(collector),
		)
		if err != nil {
			return err
		}
	} else {
		return err
	}
	_, err = tx.ExecContext(
		s.ctx,
		`INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) SELECT tenant_id,kind,resource_key,'cleanup',? FROM quota_resources q WHERE tenant_id=? AND ((kind='sandbox_slot' AND resource_key=?) OR (kind='vm_disk' AND EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=q.tenant_id AND r.kind=q.kind AND r.resource_key=q.resource_key AND r.holder_id=?))) ON CONFLICT DO NOTHING`,
		"recovery:"+id,
		b.TenantID,
		b.ID,
		"vm:"+v.ID,
	)
	if err != nil {
		return err
	}
	next := *parent
	next.Allocations = append([]recoveryAllocation(nil), parent.Allocations...)
	next.Allocations[index].Resolution.State = "cleanup"
	next.Allocations[index].Resolution.Destination = id
	if err = writeRecovery(s.ctx, tx, next); err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`DELETE FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?`,
		"operation:"+a.OperationID,
	); err != nil {
		return err
	}
	latest.Instance = v.Instance
	if err = saveSandbox(s.ctx, tx, latest); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*parent = next
	return nil
}

func (s *Service) recoverInstanceCleanup(r *resourceRecovery) error {
	unlock := s.lock(r.SandboxID)
	defer unlock()
	return s.consumeInstanceCleanup(r)
}

func (s *Service) consumeInstanceCleanup(r *resourceRecovery) error {
	current, err := s.loadRecovery(r.ID)
	if err != nil {
		return err
	}
	*r = current
	c := r.Cleanup
	if c == nil ||
		r.ID != instanceCollectorID(
			r.TenantID,
			r.SandboxID,
			r.VMID,
			r.Instance,
		) ||
		c.Request.Key != stepKey(r.operation(), releaseVMStep(r.SandboxID)) ||
		c.Request.Action != "delete" ||
		c.Request.VMID != r.VMID ||
		c.Request.ExpectedInstance != r.Instance ||
		c.Allocation.Resolution.Proof == nil ||
		c.Allocation.Resolution.Proof.ResultInstance != r.Instance {
		return api.Fail(api.ManualRecovery, "invalid collector binding")
	}
	if r.Done {
		return nil
	}
	if err = c.Allocation.validate(*c.Allocation.Resolution.Proof); err != nil {
		return err
	}
	if c.Original.Tenant != r.TenantID || c.Original.Sandbox != r.SandboxID ||
		c.Original.VM != r.VMID ||
		c.Allocation.ChargeTenantID != r.TenantID ||
		c.Allocation.SandboxID != r.SandboxID ||
		c.Original.Instance != c.Allocation.Request.ExpectedInstance {
		return api.Fail(api.ManualRecovery, "cleanup provenance changed")
	}
	b, err := getSandbox(s.ctx, s.store.db, r.SandboxID)
	if err != nil {
		return err
	}
	if b.TenantID != r.TenantID || b.State != "terminated" ||
		b.VMID != r.VMID ||
		(b.Instance != r.Instance && b.Instance != c.Original.Instance) {
		return api.Fail(
			api.ManualRecovery,
			"collector no longer owns terminated target",
		)
	}
	// Reconcile the existing attempt before considering references for a dispatch.
	receipt, findErr := s.opts.Engine.FindOperation(s.ctx, c.Request.Key)
	if findErr != nil && !isCode(findErr, api.NotFound) {
		return findErr
	}
	if findErr == nil &&
		(receipt.Key != c.Request.Key || !reflect.DeepEqual(receipt.Request, c.Request) || receipt.VMID != r.VMID) {
		return api.Fail(api.ManualRecovery, "cleanup receipt mismatch")
	}
	if findErr == nil && !recoveryTerminal(receipt.Status) {
		return api.Fail(api.Unknown, "cleanup attempt remains nonterminal")
	}
	v, vmErr := s.opts.Engine.GetVM(s.ctx, r.VMID)
	if vmErr != nil && !isCode(vmErr, api.NotFound) {
		return vmErr
	}
	if vmErr == nil &&
		(!v.Verified || v.Instance != r.Instance || v.OperationID != "") {
		return api.Fail(
			api.ManualRecovery,
			"cleanup target changed or has another owner",
		)
	}
	if findErr == nil && receipt.Status == "failed" {
		next := *r
		copyC := *c
		copyC.History = append(
			append([]cleanupAttempt(nil), c.History...),
			cleanupAttempt{Request: c.Request, Receipt: receipt},
		)
		next.Attempt++
		copyC.Request.Key = stepKey(
			next.operation(),
			releaseVMStep(r.SandboxID),
		)
		copyC.Dispatched = false
		next.Cleanup = &copyC
		if err = s.saveRecovery(next); err != nil {
			return err
		}
		*r = next
		return api.Fail(
			api.Unknown,
			"terminal failed cleanup advanced to a frozen retry",
		)
	}
	if isCode(vmErr, api.NotFound) {
		ownership := s.lockResourceOwnership()
		defer ownership()
		return s.completeInstanceCleanup(r, &receipt, findErr == nil)
	}
	if findErr == nil && receipt.Status == "succeeded" {
		return api.Fail(
			api.ManualRecovery,
			"successful B deletion still has a VM",
		)
	}
	ownership := s.lockResourceOwnership()
	defer ownership()
	vms, _, err := s.resourceReferences(b.ID, "")
	if err != nil {
		return err
	}
	if vms[r.VMID] {
		return api.Fail(api.Conflict, "instance cleanup has live dependencies")
	}
	var live int
	if err = s.store.db.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs r WHERE r.tenant_id=? AND r.holder_kind IN ('active_use','engine_dependency') AND ((r.kind='vm_disk' AND (r.resource_key=? OR EXISTS(SELECT 1 FROM quota_refs v WHERE v.tenant_id=r.tenant_id AND v.kind=r.kind AND v.resource_key=r.resource_key AND v.holder_id=?))) OR (r.kind='pending_storage' AND r.holder_id=?))`, r.TenantID, r.VMID, "vm:"+r.VMID, "sandbox:"+r.SandboxID).
		Scan(&live); err != nil {
		return err
	}
	if live > 0 {
		return api.Fail(api.Conflict, "instance cleanup has live ledger users")
	}
	next := *r
	copyC := *c
	copyC.Dispatched = true
	next.Cleanup = &copyC
	if err = s.saveRecovery(next); err != nil {
		return err
	}
	*r = next
	receipt, err = api.Wait(s.ctx, s.opts.Engine, copyC.Request)
	if err != nil {
		return err
	}
	if receipt.Key != copyC.Request.Key || receipt.Status != "succeeded" ||
		!reflect.DeepEqual(receipt.Request, copyC.Request) ||
		receipt.VMID != r.VMID {
		return api.Fail(api.ManualRecovery, "cleanup completion mismatch")
	}
	_, err = s.opts.Engine.GetVM(s.ctx, r.VMID)
	if !isCode(err, api.NotFound) {
		if err == nil {
			err = api.Fail(api.ManualRecovery, "cleanup VM still present")
		}
		return err
	}
	return s.completeInstanceCleanup(r, &receipt, true)
}

func (s *Service) completeInstanceCleanup(
	r *resourceRecovery,
	proof *api.Operation,
	hasReceipt bool,
) error {
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	b, err := getSandbox(s.ctx, tx, r.SandboxID)
	if err != nil {
		return err
	}
	if b.State != "terminated" || b.TenantID != r.TenantID ||
		b.VMID != r.VMID ||
		(b.Instance != r.Instance && b.Instance != r.Cleanup.Original.Instance) {
		return api.Fail(api.ManualRecovery, "reclaim metadata changed")
	}
	if err = s.reclaimQuotaTx(tx, "vm_disk", r.VMID); err != nil {
		return err
	}
	var remaining int
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND holder_id=?`, r.TenantID, "vm:"+r.VMID).
		Scan(&remaining); err != nil {
		return err
	}
	if remaining != 0 {
		return api.Fail(
			api.Conflict,
			"VM responsibility still has an unresolved dependency",
		)
	}
	next := *r
	copyC := *r.Cleanup
	next.Cleanup = &copyC
	next.Done = true
	if hasReceipt {
		copyC.DoneProof = proof
	} else {
		copyC.Absent = true
	}
	if err = writeRecovery(s.ctx, tx, next); err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`UPDATE resource_recovery SET body=json_set(body,'$.allocations',json((SELECT json_group_array(json(CASE WHEN json_extract(a.value,'$.resolution.destination')=? THEN json_set(a.value,'$.resolution.state','reclaimed') ELSE a.value END)) FROM json_each(resource_recovery.body,'$.allocations') a))) WHERE EXISTS(SELECT 1 FROM json_each(body,'$.allocations') a WHERE json_extract(a.value,'$.resolution.destination')=?)`,
		r.ID,
		r.ID,
	); err != nil {
		return err
	}
	b.VMID = ""
	b.Instance = ""
	if err = saveSandbox(s.ctx, tx, b); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*r = next
	return nil
}

func (s *Service) checkActivationTarget(
	o Operation,
	b Sandbox,
	target string,
) error {
	current, err := getSandbox(s.ctx, s.store.db, b.ID)
	if err != nil {
		return err
	}
	if current.TenantID != b.TenantID || current.State != "resuming" ||
		current.VMID != b.VMID ||
		(current.OperationID != o.ID && !strings.HasPrefix(o.ID, "recovery:sandbox:"+b.ID+":"+current.OperationID+":")) {
		return api.Fail(api.Conflict, "exact activation lifecycle changed")
	}
	v, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
	if err != nil {
		return err
	}
	if v.ID != b.VMID || v.Instance != target || v.OperationID != "" ||
		!v.Verified ||
		!v.AgentReady ||
		v.Phase != "running" {
		return api.Fail(api.ManualRecovery, "exact activation instance changed")
	}
	return nil
}

func (s *Service) commitAllocationActiveTx(
	tx *sql.Tx,
	o Operation,
	b Sandbox,
	target string,
) error {
	var id string
	var raw []byte
	if err := tx.QueryRowContext(s.ctx, `SELECT r.id,r.body FROM resource_recovery r,json_each(r.body,'$.allocations') a WHERE json_extract(a.value,'$.operation_id')=? AND json_extract(a.value,'$.resolution.state')='result_known' AND json_extract(a.value,'$.resolution.proof.result_instance')=?`, o.ID, target).
		Scan(&id, &raw); err != nil {
		return err
	}
	var r resourceRecovery
	if err := json.Unmarshal(raw, &r); err != nil {
		return err
	}
	current, err := getSandbox(s.ctx, tx, b.ID)
	if err != nil {
		return err
	}
	if current.State != "resuming" || current.VMID != b.VMID ||
		current.TenantID != b.TenantID ||
		(current.OperationID != r.Source && !strings.HasPrefix(current.OperationID, "recovery:"+r.ID+":")) ||
		b.Instance != target {
		return api.Fail(api.Conflict, "publication lifecycle changed")
	}
	for i := range r.Allocations {
		a := &r.Allocations[i]
		if a.OperationID != o.ID || a.Resolution.State != "result_known" {
			continue
		}
		proof := a.Resolution.Proof
		if proof == nil || proof.Status != "succeeded" ||
			proof.VMID != b.VMID ||
			proof.ResultInstance != target ||
			a.ChargeTenantID != b.TenantID {
			return api.Fail(
				api.ManualRecovery,
				"active destination proof mismatch",
			)
		}
		a.Resolution.State = "active"
		a.Resolution.Destination = string(
			encode(
				[]string{b.TenantID, b.ID, b.VMID, target, "sandbox:" + b.ID},
			),
		)
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) SELECT tenant_id,kind,resource_key,'active_use',? FROM quota_resources q WHERE q.tenant_id=? AND q.kind='vm_disk' AND EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=q.tenant_id AND r.kind=q.kind AND r.resource_key=q.resource_key AND r.holder_id=?) ON CONFLICT DO NOTHING`,
		"sandbox:"+b.ID,
		b.TenantID,
		"vm:"+b.VMID,
	); err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`DELETE FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?`,
		"operation:"+o.ID,
	); err != nil {
		return err
	}
	return writeRecovery(s.ctx, tx, r)
}
