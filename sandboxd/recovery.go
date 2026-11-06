package sandboxd

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// A failed client intent remains terminal. Compensating work has its own durable
// intent, and advances its attempt only after a known terminal engine failure.
type resourceRecovery struct {
	ID        string `json:"id"`
	TenantID  string `json:"tenant_id,omitempty"`
	MemoryMiB int64  `json:"memory_mib,omitempty"`
	Source    string `json:"source"`
	Step      string `json:"step,omitempty"`
	Kind      string `json:"kind"`
	SandboxID string `json:"sandbox_id,omitempty"`
	VMID      string `json:"vm_id,omitempty"`
	Snapshot  string `json:"snapshot_id,omitempty"`
	Instance  string `json:"instance,omitempty"`
	Attempt   uint64 `json:"attempt"`
	Done      bool   `json:"done,omitempty"`
	LastError string `json:"last_error,omitempty"`
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
	return saveSandbox(s.ctx, s.store.db, b)
}

func (s *Service) saveOperationReference(o Operation) error {
	unlock := s.lockResourceOwnership()
	defer unlock()
	return saveOp(s.ctx, s.store.db, o)
}

func (s *Service) saveRecovery(r resourceRecovery) error {
	_, err := s.store.db.ExecContext(
		s.ctx,
		"INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
		r.ID,
		encode(r),
	)
	return err
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
		if !r.Done && r.Kind == "sandbox" {
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
		if !r.Done && r.Kind == "retired_snapshot" {
			failures = errors.Join(failures, s.recoverRetiredSnapshot(&r))
		} else if !r.Done && r.Kind != "sandbox" {
			failures = errors.Join(failures, s.recoverOrphanResource(&r))
		}
	}
	return failures
}

// commitRunning transfers the sleep reference and its storage charge together.
// The caller holds the sandbox lock; no db query is opened inside the transaction.
func (s *Service) commitRunning(o Operation, b *Sandbox) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	next := *b
	if next.SnapshotID == "" {
		return saveSandbox(s.ctx, s.store.db, next)
	}
	if err := s.proveRetiredSnapshot(next); err != nil {
		return err
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
		if pending && o.ID != excludeOperation {
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
				((!recoveryTerminal(op.Status)) || (pending && o.ID != excludeOperation)) {
				vms[op.VMID], vms[op.Request.VMID] = true, true
				snaps[op.SnapshotID], snaps[op.Request.SnapshotID] = true, true
			}
		}
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
	if cause == nil {
		r.Done, r.LastError = true, ""
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
	proof, err := s.findResourceStep(o, r.Step, action)
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
	o.TenantID, o.Command = source.TenantID, source.Command
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
	o.Command, o.TenantID = source.Command, source.TenantID
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
	if v.Phase == "stopped" && source.SourceInstance != "" &&
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
		v, err = s.opts.Engine.GetVM(s.ctx, v.ID)
		if err != nil {
			return err
		}
		r.Instance = v.Instance
	} else if r.Instance != v.Instance ||
		r.Instance == "" {
		// An activation receipt binds a successful restore to its exact instance.
		activation, e := s.findResourceStep(
			source,
			"network-off:"+b.ID,
			"network",
		)
		if e != nil {
			return e
		}
		restored, e := s.findResourceStep(o, "rollback-restore", "restore")
		if e != nil {
			return e
		}
		cloned, e := s.findResourceStep(o, "rollback-clone", "clone")
		if e != nil {
			return e
		}
		ownResult := (restored != nil && restored.Status == "succeeded" && restored.VMID == v.ID) ||
			(cloned != nil && cloned.Status == "succeeded" && cloned.VMID == v.ID)
		if !ownResult &&
			(activation == nil || activation.Status != "succeeded" || activation.VMID != v.ID || activation.Request.ExpectedInstance != v.Instance) {
			return api.Fail(
				api.ManualRecovery,
				"rollback running instance cannot be proven; refusing stop or restart",
			)
		}
		r.Instance = v.Instance
	}
	if v.Phase != "running" || !v.AgentReady || !v.Verified {
		return api.Fail(
			api.ManualRecovery,
			"rollback restore is not verified running",
		)
	}
	if err = s.saveRecovery(*r); err != nil {
		return err
	}
	if err = s.activate(o, b, false); err != nil {
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
