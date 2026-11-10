package sandboxd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func (s *Service) Execute(
	ctx context.Context,
	p Principal,
	key string,
	c Command,
) (Result, error) {
	o, err := s.Submit(ctx, p, key, c)
	if err != nil {
		return Result{}, err
	}
	return s.Wait(ctx, p, o.ID)
}

func (s *Service) start(o Operation) {
	if o.Status == "succeeded" || o.Status == "failed" {
		return
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	if _, ok := s.active[o.ID]; ok {
		s.mu.Unlock()
		return
	}
	done := make(chan struct{})
	s.active[o.ID] = done
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		defer func() { s.mu.Lock(); close(done); delete(s.active, o.ID); s.mu.Unlock() }()
		s.run(o)
	}()
}

func (s *Service) Wait(
	ctx context.Context,
	p Principal,
	id string,
) (Result, error) {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		o, err := s.Operation(ctx, p, id)
		if err != nil {
			return Result{}, err
		}
		if o.Status == "succeeded" {
			return s.resultTokens(p, o.Result)
		}
		if o.Status == "failed" {
			if o.Error != nil {
				return o.Result, o.Error
			}
			return o.Result, Fail(ErrInternal, "operation failed")
		}
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-t.C:
		}
	}
}

func (s *Service) Operation(
	ctx context.Context,
	p Principal,
	id string,
) (Operation, error) {
	o, err := s.store.operation(ctx, id)
	if err != nil {
		return o, err
	}
	if !visible(p, o.TenantID, o.Command.SandboxID) ||
		(p.SandboxID != "" && !p.Root && o.Command.Action != "renew" && o.Command.Action != "release") {
		return Operation{}, Fail(ErrNotFound, "not found")
	}
	if _, err := s.resultTokens(p, o.Result); err != nil {
		return Operation{}, err
	}
	o.Result.Tokens = nil
	return publicOperation(o), nil
}

func (s *Service) resultTokens(p Principal, r Result) (Result, error) {
	r.Tokens = nil
	targets := append([]Sandbox(nil), r.Sandboxes...)
	if r.Sandbox != nil {
		targets = append(targets, *r.Sandbox)
	}
	for _, b := range targets {
		if !visible(p, b.TenantID, b.ID) {
			return Result{}, Fail(ErrNotFound, "not found")
		}
	}
	if r.Checkpoint != nil &&
		!visible(p, r.Checkpoint.TenantID, r.Checkpoint.SandboxID) {
		return Result{}, Fail(ErrNotFound, "not found")
	}
	if r.Template != nil && !visible(p, r.Template.TenantID, "") {
		return Result{}, Fail(ErrNotFound, "not found")
	}
	for _, b := range targets {
		if b.State != "terminated" {
			if r.Tokens == nil {
				r.Tokens = map[string]string{}
			}
			r.Tokens[b.ID] = s.issuedToken(b)
		}
	}
	return publicResult(r), nil
}

func (s *Service) setStep(o *Operation, step string) error {
	o.Step = step
	return saveOp(s.ctx, s.store.db, *o)
}

func (s *Service) engine(
	o Operation,
	step string,
	r api.Request,
) (api.Operation, error) {
	r.Key = stepKey(o, step)
	if group, err := s.findAllocationGroup(o); err != nil {
		return api.Operation{}, err
	} else if group != nil {
		return s.allocationEngine(o, step, r)
	}
	if a, err := s.quotaRecoveryAllocation(o, step); err != nil {
		return api.Operation{}, err
	} else if a != nil && !bytes.Equal(encode(a.Request), encode(r)) {
		return api.Operation{}, Fail(
			ErrManualRecovery,
			"recovery dispatch does not match frozen allocation request",
		)
	}
	if err := s.requireQuotaReservation(o, step); err != nil {
		return api.Operation{}, err
	}
	captureTenant := o.TenantID
	if o.ChargeTenantID != "" {
		captureTenant = o.ChargeTenantID
	}
	if r.Action == "snapshot" && o.Command.SandboxID != "" {
		source, err := getSandbox(s.ctx, s.store.db, o.Command.SandboxID)
		if err != nil {
			return api.Operation{}, err
		}
		if source.VMID != r.VMID || source.Instance != r.ExpectedInstance {
			return api.Operation{}, Fail(
				ErrManualRecovery,
				"capture does not match the source sandbox instance",
			)
		}
		// Root's selected tenant scopes the request, not the existing data.
		captureTenant = source.TenantID
	}
	if r.Action == "restore" && o.ChargeTenantID != "" {
		source, err := getSandbox(s.ctx, s.store.db, o.Command.SandboxID)
		if err != nil {
			return api.Operation{}, err
		}
		if source.TenantID != o.ChargeTenantID {
			return api.Operation{}, Fail(
				ErrManualRecovery,
				"restore charge owner mismatch",
			)
		}
		if err = putQuotaRef(
			s.ctx,
			s.store.db,
			o.ChargeTenantID,
			"snapshot",
			r.SnapshotID,
			quotaRef{Kind: "active_use", ID: "backing:" + r.VMID},
		); err != nil {
			return api.Operation{}, err
		}
	}
	if r.Action == "clone" && o.ChargeTenantID != "" {
		if err := referenceQuotaSnapshot(
			s.ctx,
			s.store.db,
			o.ChargeTenantID,
			r.SnapshotID,
			quotaRef{Kind: "engine_dependency", ID: "operation:" + o.ID},
		); err != nil {
			return api.Operation{}, err
		}
	}
	if err := s.beginQuotaDispatch(o, step); err != nil {
		return api.Operation{}, err
	}
	result, err := api.Wait(s.ctx, s.opts.Engine, r)
	err = engineFailure(err)
	if r.Action == "clone" && result.VMID != "" && o.ChargeTenantID != "" {
		if e := referenceQuotaSnapshot(
			s.ctx,
			s.store.db,
			o.ChargeTenantID,
			r.SnapshotID,
			quotaRef{Kind: "active_use", ID: "backing:" + result.VMID},
		); e != nil {
			return result, errors.Join(err, e)
		}
	}
	if ledgerErr := s.acceptQuotaReceipt(o, step, r, result); ledgerErr != nil {
		return result, errors.Join(err, ledgerErr)
	}
	if err == nil && (r.Action == "delete" || r.Action == "delete_snapshot") {
		kind, key := "vm_disk", r.VMID
		if r.Action == "delete_snapshot" {
			kind, key = "snapshot", r.SnapshotID
		}
		if ledgerErr := s.reclaimQuota(kind, key); ledgerErr != nil {
			return result, ledgerErr
		}
	}
	if err == nil && r.Action == "snapshot" {
		ownership := s.lockResourceOwnership()
		err = recordSnapshotOwner(
			s.ctx,
			s.store.db,
			result.SnapshotID,
			captureTenant,
			o.ID,
			"captured",
		)
		ownership()
	}
	return result, err
}

func (s *Service) run(o Operation) {
	// A retry may carry a stale pending copy after atomic configure delivery.
	if o.Command.Action == "configure" {
		current, err := s.store.operation(s.ctx, o.ID)
		if err != nil || current.Status == "succeeded" ||
			current.Status == "failed" {
			return
		}
		o = current
	}
	current, currentErr := s.store.operation(s.ctx, o.ID)
	if currentErr != nil || recoveryTerminal(current.Status) {
		return
	}
	o = current
	o.Status = "running"
	if err := saveOp(s.ctx, s.store.db, o); err != nil {
		return
	}
	var err error
	switch o.Command.Action {
	case "claim":
		err = s.claim(&o)
	case "fork":
		err = s.fork(&o)
	case "pause",
		"resume",
		"release",
		"renew",
		"checkpoint",
		"configure",
		"rollback":
		err = s.change(&o)
	case "delete_checkpoint":
		err = s.deleteCheckpoint(&o)
	case "template_put", "template_build":
		err = s.template(&o)
	case "refill":
		err = s.refill(&o)
	}
	// Configure commits its terminal result with delivery and fence retirement.
	// A later write must never turn that durable success into a failed intent.
	if o.Status != "succeeded" {
		if s.ctx.Err() != nil {
			return
		} // Preserve resumable intent on daemon shutdown.
		current, e := s.store.operation(s.ctx, o.ID)
		if e != nil || recoveryTerminal(current.Status) {
			return
		}
		if err != nil {
			o.Status = "failed"
			o.Error = operationError(o, err)
		} else {
			o.Status = "succeeded"
		}
		o.FinishedAt = time.Now().UTC()
		// Resource receipts are recorded before publishing a terminal result.
		if e := s.finishQuotaOperation(o); e != nil {
			o.Status = "failed"
			o.Error = operationError(o, errors.Join(err, e))
		}
		group, groupErr := s.findAllocationGroup(o)
		if groupErr != nil {
			o.Status = "failed"
			o.Error = operationError(o, errors.Join(err, groupErr))
		} else if group == nil {
			_ = s.reconcileQuota()
		}
		if e := saveOp(s.ctx, s.store.db, o); e != nil {
			return
		}
	}
	_ = s.store.audit(
		s.ctx,
		map[string]any{
			"action":       o.Command.Action,
			"tenant_id":    o.TenantID,
			"operation_id": o.ID,
			"status":       o.Status,
			"step":         o.Step,
			"diagnostic":   privateDiagnostic(o.Error),
		},
	)
}

func operationError(o Operation, err error) *Error {
	e := publicError(err)
	return Wrap(
		e.Code,
		fmt.Sprintf("%s (%s): %s", o.Command.Action, o.Step, e.Message),
		err,
	)
}

func saveOp(ctx context.Context, e executor, o Operation) error {
	result, err := e.ExecContext(
		ctx,
		"INSERT INTO operations(id,tenant,idem_key,spec_hash,status,body) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,body=excluded.body",
		o.ID,
		o.TenantID,
		o.Key,
		o.hash,
		o.Status,
		encode(o),
	)
	return requireSQLWrite(result, err)
}

func (s *store) operation(ctx context.Context, id string) (Operation, error) {
	var b []byte
	var hash string
	err := s.db.QueryRowContext(ctx, "SELECT body,spec_hash FROM operations WHERE id=?", id).
		Scan(&b, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, Fail(ErrNotFound, "not found")
	}
	var o Operation
	if err == nil {
		err = json.Unmarshal(b, &o)
		o.hash = hash
	}
	return o, err
}

func (s *store) byKey(
	ctx context.Context,
	tenant, key string,
) (Operation, error) {
	var id string
	err := s.db.QueryRowContext(ctx, "SELECT id FROM operations WHERE tenant=? AND idem_key=?", tenant, key).
		Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, Fail(ErrNotFound, "not found")
	}
	if err != nil {
		return Operation{}, err
	}
	return s.operation(ctx, id)
}

// Command describes a requested sandbox or template action. Admission validates
// its scope and freezes the accepted intent before dispatching engine effects.
type Command struct {
	Action        string            `json:"action"`
	SandboxID     string            `json:"sandbox_id,omitempty"`
	CheckpointID  string            `json:"checkpoint_id,omitempty"`
	TemplateID    string            `json:"template_id,omitempty"`
	Template      *Template         `json:"template,omitempty"`
	TTLSeconds    int64             `json:"ttl_seconds,omitempty"`
	OnExpire      string            `json:"on_expire,omitempty"`
	AutoRenew     bool              `json:"auto_renew,omitempty"`
	LeaseDeadline time.Time         `json:"lease_deadline"`
	Env           map[string]EnvVar `json:"env,omitempty"`
	Policy        egress.Policy     `json:"policy"`
	Count         int               `json:"count,omitempty"`
}

// Result contains an operation's published resources and authorized access tokens.
// Tokens are regenerated for the caller rather than retained in durable results.
type Result struct {
	OperationID string            `json:"operation_id"`
	Sandbox     *Sandbox          `json:"sandbox,omitempty"`
	Sandboxes   []Sandbox         `json:"sandboxes,omitempty"`
	Tokens      map[string]string `json:"tokens,omitempty"`
	Checkpoint  *Checkpoint       `json:"checkpoint,omitempty"`
	Template    *Template         `json:"template,omitempty"`
}

// Operation records an admitted command, its progress, and its terminal result.
// Key identifies idempotent submission within the tenant scope.
type Operation struct {
	ID             string    `json:"id"`
	Key            string    `json:"key"`
	TenantID       string    `json:"tenant_id"`
	ChargeTenantID string    `json:"charge_tenant_id,omitempty"`
	origin         string    `json:"-"`
	hash           string    `json:"-"`
	Command        Command   `json:"command"`
	IDs            []string  `json:"resource_ids,omitempty"`
	Status         string    `json:"status"`
	Step           string    `json:"step"`
	SnapshotID     string    `json:"snapshot_id,omitempty"`
	OwnedSnapshot  bool      `json:"owned_snapshot,omitempty"`
	ReservedMiB    int64     `json:"reserved_mib,omitempty"`
	SourceInstance string    `json:"source_instance,omitempty"`
	Result         Result    `json:"result"`
	Error          *Error    `json:"error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	FinishedAt     time.Time `json:"finished_at"`
}

func stepKey(
	op Operation,
	step string,
) string {
	return "sbx-op:" + op.ID + ":step:" + step
}
