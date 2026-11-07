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
	Group       *allocationGroup     `json:"allocation_group,omitempty"`
	Deletion    *allocationDeletion  `json:"allocation_deletion,omitempty"`
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
	Subject        allocationSubject    `json:"subject,omitempty"`
	RequestBound   bool                 `json:"request_bound,omitempty"`
	Resolution     allocationResolution `json:"resolution"`
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

type warmSelection struct {
	Refill   string             `json:"refill"`
	Identity allocationIdentity `json:"identity"`
	Stage    string             `json:"stage"`
	Deletion allocationStep     `json:"deletion"`
	History  []cleanupAttempt   `json:"history,omitempty"`
	Attempt  uint64             `json:"attempt,omitempty"`
}

type allocationDeletion struct {
	Group    string             `json:"group"`
	Subject  allocationSubject  `json:"subject"`
	Identity allocationIdentity `json:"identity"`
	Step     allocationStep     `json:"step"`
	History  []cleanupAttempt   `json:"history,omitempty"`
	Absent   bool               `json:"absent,omitempty"`
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
	groupIntents, err := s.resourceRecoveries()
	if err != nil {
		return err
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

// commitRunning transfers the sleep reference and its storage charge together.
// The caller holds the sandbox lock; no db query is opened inside the transaction.
func (s *Service) commitRunning(
	o Operation,
	b *Sandbox,
	exact ...*activationTarget,
) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	next := *b
	var target *activationTarget
	if len(exact) > 0 {
		target = exact[0]
	}
	if next.SnapshotID == "" && target == nil {
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
	if target != nil {
		switch target.Kind {
		case "selection":
			err = s.commitSelectionActiveTx(tx, o, next, target)
		case "allocation":
			err = s.commitGroupActiveTx(tx, next, target)
		default:
			err = s.commitAllocationActiveTx(tx, o, next, target.Instance)
		}
		if err != nil {
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
	// New allocation parents retain publication readers after their VM retires.
	// These readers and published templates cannot make accounting progress here.
	group, err := s.findAllocationGroup(Operation{ID: r.Source})
	if err != nil {
		return err
	}
	if group != nil {
		var readers int
		if err = s.store.db.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND ((holder_kind='active_use' AND holder_id LIKE 'template:%') OR (holder_kind='engine_dependency' AND holder_id=? AND EXISTS(SELECT 1 FROM operations WHERE id=? AND status IN ('pending','running'))))`, r.TenantID, r.Snapshot, "operation:"+r.Source, r.Source).
			Scan(&readers); err != nil {
			return err
		}
		if readers != 0 {
			return nil
		}
	}
	_, err = s.opts.Engine.GetSnapshot(s.ctx, r.Snapshot)
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
	if err = s.activateTarget(
		o,
		b,
		false,
		&activationTarget{
			Origin:   o.ID,
			Kind:     "recovery",
			VM:       b.VMID,
			Instance: target,
		},
	); err != nil {
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

func writeRecovery(ctx context.Context, e executor, r resourceRecovery) error {
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
		return api.Fail(
			api.ManualRecovery,
			"recovery responsibility was not persisted",
		)
	}
	return nil
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

func validateAllocationProvenance(r resourceRecovery) error {
	if r.Group == nil || r.Group.Identity == nil {
		return api.Fail(
			api.ManualRecovery,
			"missing known allocation provenance",
		)
	}
	i := r.Group.Identity
	if i.Owner != r.TenantID || i.Source != r.Source ||
		i.Shape != r.Group.Template.Shape ||
		i.VM == "" ||
		i.Receipt.ID == "" ||
		i.Receipt.VMID != i.VM {
		return api.Fail(
			api.ManualRecovery,
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
			return api.Fail(
				api.ManualRecovery,
				"generation has no original frozen successful receipt",
			)
		}
	}
	if !matched {
		return api.Fail(
			api.ManualRecovery,
			"known VM has no original frozen receipt",
		)
	}
	if i.Receipt.Request.Action == "create" &&
		(i.Receipt.Request.Config == nil || i.Receipt.Request.Config.Shape != i.Shape) {
		return api.Fail(
			api.ManualRecovery,
			"created VM lost full shape provenance",
		)
	}
	if i.Receipt.Request.Action == "clone" &&
		i.Receipt.Request.SnapshotID != r.Group.Template.SnapshotID {
		return api.Fail(api.ManualRecovery, "clone lost frozen snapshot source")
	}
	return nil
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
			return api.Fail(
				api.ManualRecovery,
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
		return api.Fail(api.Conflict, "allocation VM has a sandbox user")
	}
	if err := tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs q WHERE q.kind='vm_disk' AND (q.resource_key=? OR EXISTS(SELECT 1 FROM quota_refs v WHERE v.tenant_id=q.tenant_id AND v.kind=q.kind AND v.resource_key=q.resource_key AND v.holder_id=?)) AND (q.tenant_id!=? OR q.holder_kind='engine_dependency' OR (q.holder_kind='active_use' AND q.holder_id NOT IN (?,?)))`, i.VM, "vm:"+i.VM, r.TenantID, "warm:"+i.VM, "sandbox:"+excluded).
		Scan(&users); err != nil {
		return err
	}
	if users != 0 {
		return api.Fail(
			api.Conflict,
			"allocation VM has another owner or dependency",
		)
	}
	return nil
}

func validAllocationIdentity(r resourceRecovery, v api.VM) error {
	if err := validateAllocationProvenance(r); err != nil {
		return err
	}
	g := r.Group
	if g == nil || g.Identity == nil {
		return api.Fail(
			api.ManualRecovery,
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
		return api.Fail(
			api.ManualRecovery,
			"allocation VM provenance is incomplete",
		)
	}
	if i.Instance == "" || i.Generation == nil ||
		i.Generation.Status != "succeeded" ||
		i.Generation.ResultInstance != i.Instance ||
		i.Generation.VMID != i.VM ||
		(i.Generation.Request.Action != "start" && i.Generation.Request.Action != "clone") {
		return api.Fail(
			api.ManualRecovery,
			"allocation lacks immutable successful generation witness",
		)
	}
	if v.ID != i.VM || v.Instance != i.Instance || !v.Verified ||
		v.OperationID != "" ||
		v.Config.Shape != i.Shape {
		return api.Fail(
			api.ManualRecovery,
			"allocation VM changed or is owned elsewhere",
		)
	}
	if i.Receipt.Request.Action == "create" &&
		!reflect.DeepEqual(i.Receipt.Request.Config, &v.Config) {
		return api.Fail(api.ManualRecovery, "created VM config changed")
	}
	if i.Receipt.Request.Action == "clone" &&
		i.Receipt.Request.SnapshotID != g.Template.SnapshotID {
		return api.Fail(api.ManualRecovery, "clone source changed")
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
			return "", api.Fail(
				api.Conflict,
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
			return "", api.Fail(api.ManualRecovery, "cleanup tuple collision")
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
		return api.Fail(api.ManualRecovery, "absent VM has no durable identity")
	}
	i := r.Group.Identity
	if err = validateAllocationProvenance(r); err != nil {
		return err
	}
	if !recoveryTerminal(i.Receipt.Status) {
		return api.Fail(
			api.Conflict,
			"nonterminal allocation cannot retire its known disk",
		)
	}
	if i.Owner != r.TenantID || i.Source != r.Source ||
		i.Shape != r.Group.Template.Shape ||
		i.Receipt.VMID != i.VM ||
		i.Receipt.ID == "" {
		return api.Fail(api.ManualRecovery, "absence provenance mismatch")
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
		return api.Fail(
			api.ManualRecovery,
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
		return api.Fail(
			api.Conflict,
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
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return err
	}
	if r.Group.Subject.Kind == "sandbox" {
		b, e := getSandbox(s.ctx, tx, r.Group.Subject.ID)
		if e != nil {
			return e
		}
		if b.TenantID != r.TenantID || (b.VMID != "" && b.VMID != i.VM) {
			return api.Fail(api.ManualRecovery, "absent sandbox target changed")
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
			return api.Fail(
				api.ManualRecovery,
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
		return api.Fail(
			api.ManualRecovery,
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
		return api.Fail(
			api.ManualRecovery,
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
		return api.Fail(
			api.ManualRecovery,
			"cleanup request differs from exact generation destination",
		)
	}
	if uint64(len(d.History)) != c.Attempt {
		return api.Fail(
			api.ManualRecovery,
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
			return api.Fail(
				api.ManualRecovery,
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
		return api.Fail(api.ManualRecovery, "cleanup has no successful witness")
	}
	r, err := s.loadRecovery(c.Deletion.Group)
	if err != nil {
		return err
	}
	if err = validateAllocationDeletion(c, r); err != nil {
		return err
	}
	v, err := s.opts.Engine.GetVM(s.ctx, c.VMID)
	if isCode(err, api.NotFound) {
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
		if e != nil && !isCode(e, api.NotFound) {
			return e
		}
		if e == nil {
			if !reflect.DeepEqual(proof.Request, c.Deletion.Step.Request) {
				return api.Fail(api.ManualRecovery, "cleanup receipt mismatch")
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
	if !isCode(err, api.NotFound) {
		if err != nil {
			return err
		}
		return api.Fail(api.ManualRecovery, "delete did not prove VM absence")
	}
	return s.finishAllocationCleanup(c)
}

func (s *Service) finishAllocationCleanup(c resourceRecovery) error {
	return s.retireAllocationAbsent(c.Deletion.Group, c.ID)
}

func snapshotCollector(
	ctx context.Context,
	e executor,
	tenant, sid, source string,
) (resourceRecovery, error) {
	var raw []byte
	err := e.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE json_extract(body,'$.kind')='retired_snapshot' AND json_extract(body,'$.snapshot_id')=? AND COALESCE(json_extract(body,'$.done'),0)=0 ORDER BY id LIMIT 1`, sid).
		Scan(&raw)
	var r resourceRecovery
	if err == nil {
		err = json.Unmarshal(raw, &r)
		if err == nil && r.TenantID != tenant {
			err = api.Fail(
				api.ManualRecovery,
				"snapshot collector owner mismatch",
			)
		}
		return r, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return r, err
	}
	h := sha256.Sum256(
		encode([]string{"plan013-retired-snapshot-v1", tenant, sid}),
	)
	r = resourceRecovery{
		ID:       "template-retired:" + hex.EncodeToString(h[:]),
		Source:   source,
		TenantID: tenant,
		Kind:     "retired_snapshot",
		Snapshot: sid,
	}
	if err = insertRecovery(ctx, e, r); err != nil {
		return r, err
	}
	err = putQuotaRef(
		ctx,
		e,
		tenant,
		"snapshot",
		sid,
		quotaRef{Kind: "cleanup", ID: "recovery:" + r.ID},
	)
	return r, err
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
	target *activationTarget,
) error {
	current, err := getSandbox(s.ctx, s.store.db, b.ID)
	if err != nil {
		return err
	}
	if target.Kind == "allocation" {
		r, e := s.loadRecovery("allocation:" + target.Origin)
		if e != nil {
			return e
		}
		if r.Group == nil || r.Group.Identity == nil ||
			current.State != "provisioning" ||
			current.OperationID != target.Origin ||
			current.VMID != target.VM ||
			r.Group.Identity.VM != target.VM ||
			r.Group.Identity.Instance != target.Instance {
			return api.Fail(
				api.Conflict,
				"allocation activation lifecycle changed",
			)
		}
	} else if target.Kind == "selection" {
		r, e := s.loadRecovery("allocation:" + target.Origin)
		if e != nil {
			return e
		}
		if r.Group == nil || r.Group.Selection == nil ||
			r.Group.Selection.Stage != "selected" ||
			current.State != "provisioning" ||
			current.OperationID != target.Origin ||
			current.TenantID != r.TenantID ||
			current.VMID != target.VM ||
			current.Instance != target.Instance ||
			r.Group.Selection.Identity.VM != target.VM ||
			r.Group.Selection.Identity.Instance != target.Instance {
			return api.Fail(
				api.Conflict,
				"exact warm activation lifecycle changed",
			)
		}
	} else if current.TenantID != b.TenantID || current.State != "resuming" ||
		current.VMID != b.VMID ||
		(current.OperationID != o.ID && !strings.HasPrefix(o.ID, "recovery:sandbox:"+b.ID+":"+current.OperationID+":")) {
		return api.Fail(api.Conflict, "exact activation lifecycle changed")
	}
	v, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
	if err != nil {
		return err
	}
	if v.ID != target.VM || v.Instance != target.Instance ||
		v.OperationID != "" ||
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
