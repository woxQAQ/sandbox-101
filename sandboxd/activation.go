package sandboxd

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

type activationTarget struct {
	Origin   string
	Kind     string
	VM       string
	Instance string
}

func (s *Service) activate(o Operation, b *Sandbox, reset bool) error {
	if b.State == "provisioning" {
		group, err := s.findAllocationGroup(o)
		if err != nil {
			return err
		}
		if group != nil && group.Group.Identity != nil {
			i := group.Group.Identity
			if i.Instance == "" {
				return Fail(
					ErrManualRecovery,
					"allocation activation lacks successful witness",
				)
			}
			return s.activateTarget(
				o,
				b,
				reset,
				&activationTarget{
					Origin:   group.Source,
					Kind:     "allocation",
					VM:       i.VM,
					Instance: i.Instance,
				},
			)
		}
	}
	return s.activateTarget(o, b, reset, nil)
}

func (s *Service) activateTarget(
	o Operation,
	b *Sandbox,
	reset bool,
	target *activationTarget,
) (err error) {
	if err = s.activationFence(o, b.ID); err != nil {
		return err
	}
	if s.credentialsMissing(*b) {
		return Fail(
			ErrConflict,
			"host credentials must be supplied again after daemon restart",
		)
	}
	v, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
	if err != nil {
		return engineFailure(err)
	}
	if target != nil && (v.ID != target.VM || v.Instance != target.Instance) {
		return Fail(
			ErrManualRecovery,
			"activation read returned another instance",
		)
	}
	if target != nil {
		if err = s.checkActivationTarget(o, *b, target); err != nil {
			return err
		}
	}
	if v.Phase != "running" || !v.AgentReady || !v.Verified {
		return Fail(ErrConflict, "engine VM is not verified ready")
	}
	// Network is disabled even for a recovered activation. Sandbox policy always precedes enable.
	if _, err = s.engine(
		o,
		"network-off:"+b.ID,
		api.Request{
			Action:           "network",
			VMID:             v.ID,
			ExpectedInstance: v.Instance,
			NetworkEnabled:   false,
		},
	); err != nil {
		return err
	}
	if target != nil {
		if err = s.checkActivationTarget(o, *b, target); err != nil {
			return err
		}
	}
	if err = s.opts.Agent.Info(s.ctx, v); err != nil {
		return err
	}
	// Fence replacement and Bind are serialized, so an old worker cannot
	// publish after a newer intent was durably admitted.
	publication := s.lock("configuration:" + b.ID)
	if err = s.activationFence(o, b.ID); err == nil {
		if s.opts.Egress != nil {
			err = s.bind(s.ctx, *b, v.VSockPath)
		} else if b.Policy.Mode != "none" {
			err = Fail(
				ErrIncompatible,
				"egress manager required for network mode",
			)
		}
	}
	publication()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			s.closeConnections(b.ID)
			err = errors.Join(err, s.revoke(b.ID))
		}
	}()
	if target != nil {
		if err = s.checkActivationTarget(o, *b, target); err != nil {
			return err
		}
	}
	if err = s.opts.Agent.Activate(
		s.ctx,
		v,
		Activation{
			SandboxID:     b.ID,
			Hostname:      strings.ReplaceAll(b.ID, "_", "-"),
			ResetIdentity: reset,
			Env:           b.Env,
			Network:       b.Policy.Mode,
			CAPEM:         s.opts.CAPEM,
		},
	); err != nil {
		return err
	}
	publication = s.lock("configuration:" + b.ID)
	defer publication()
	if err = s.activationFence(o, b.ID); err != nil {
		return err
	}
	if b.Policy.Mode == "direct" {
		if _, err = s.engine(
			o,
			"network-on:"+b.ID,
			api.Request{
				Action:           "network",
				VMID:             v.ID,
				ExpectedInstance: v.Instance,
				NetworkEnabled:   true,
			},
		); err != nil {
			return err
		}
	}
	if target != nil {
		if err = s.checkActivationTarget(o, *b, target); err != nil {
			return err
		}
	}
	next := *b
	next.Instance = v.Instance
	if b.State != "running" {
		next.Deadline = time.Now().
			UTC().
			Add(time.Duration(b.TTLSeconds) * time.Second)
	}
	next.State, next.OperationID, next.LastError = "running", "", ""
	if err = s.commitRunning(o, &next, target); err != nil {
		return err
	}
	*b = next
	return nil
}

func (s *Service) commitGroupActiveTx(
	tx *sql.Tx,
	b Sandbox,
	target *activationTarget,
) error {
	r, err := loadRecoveryTx(s.ctx, tx, "allocation:"+target.Origin)
	if err != nil {
		return err
	}
	current, err := getSandbox(s.ctx, tx, b.ID)
	if err != nil {
		return err
	}
	if r.Group == nil || r.Group.Identity == nil ||
		current.State != "provisioning" ||
		current.OperationID != target.Origin ||
		current.TenantID != r.TenantID ||
		current.VMID != target.VM ||
		r.Group.Identity.VM != target.VM ||
		r.Group.Identity.Instance != target.Instance ||
		b.Instance != target.Instance {
		return Fail(ErrConflict, "allocation publication changed")
	}
	for index := range r.Allocations {
		a := &r.Allocations[index]
		if a.Kind == "vm_disk" && a.Resolution.Proof != nil &&
			a.Resolution.Proof.VMID == target.VM {
			a.Resolution.State = "active"
			a.Resolution.Destination = "sandbox:" + b.ID
		}
	}
	r.Done = true
	if err = putQuotaRef(
		s.ctx,
		tx,
		r.TenantID,
		"vm_disk",
		target.VM,
		quotaRef{Kind: "active_use", ID: "sandbox:" + b.ID},
	); err != nil {
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
	if err = saveSandbox(s.ctx, tx, b); err != nil {
		return err
	}
	return writeRecovery(s.ctx, tx, r)
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
			return Fail(
				ErrConflict,
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
			return Fail(
				ErrConflict,
				"exact warm activation lifecycle changed",
			)
		}
	} else if current.TenantID != b.TenantID || current.State != "resuming" ||
		current.VMID != b.VMID ||
		(current.OperationID != o.ID && !strings.HasPrefix(o.ID, "recovery:sandbox:"+b.ID+":"+current.OperationID+":")) {
		return Fail(ErrConflict, "exact activation lifecycle changed")
	}
	v, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
	if err != nil {
		return engineFailure(err)
	}
	if v.ID != target.VM || v.Instance != target.Instance ||
		v.OperationID != "" ||
		!v.Verified ||
		!v.AgentReady ||
		v.Phase != "running" {
		return Fail(ErrManualRecovery, "exact activation instance changed")
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
		return Fail(ErrConflict, "publication lifecycle changed")
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
			return Fail(
				ErrManualRecovery,
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

// Activation supplies the guest identity, visible environment, and network
// settings applied after an exact engine instance has been verified.
type Activation struct {
	SandboxID     string
	Hostname      string
	ResetIdentity bool
	Env           map[string]string
	Network       string
	CAPEM         string
}
