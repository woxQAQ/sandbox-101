package sandboxd

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
)

type releaseIntent struct {
	Tenant     string      `json:"tenant"`
	Sandbox    string      `json:"sandbox"`
	VM         string      `json:"vm"`
	Instance   string      `json:"instance"`
	Source     Operation   `json:"source"`
	Request    api.Request `json:"request"`
	Dispatched bool        `json:"dispatched"`
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
