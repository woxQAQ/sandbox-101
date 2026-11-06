package sandboxd

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

var randRead = rand.Read

type Service struct {
	opts        Options
	store       *store
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.Mutex
	locks       map[string]*sync.Mutex
	active      map[string]chan struct{}
	secrets     map[string]map[string]egress.Secret
	bindings    map[string]string
	connections map[string]map[net.Conn]struct{}
	draining    bool
	closing     bool
	closeOnce   sync.Once
	closeErr    error
	opSecrets   map[string]map[string]egress.Secret
	epoch       string
}

func Open(opts Options) (*Service, error) {
	if opts.Engine == nil || opts.Agent == nil || opts.RootToken == "" {
		return nil, api.Fail(
			api.Invalid,
			"engine, agent and root token are required",
		)
	}
	if opts.MaxTTL <= 0 {
		opts.MaxTTL = time.Hour
	}
	if opts.PausedTTL <= 0 {
		opts.PausedTTL = 24 * time.Hour
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Second
	}
	if opts.MaxWarm <= 0 {
		opts.MaxWarm = 32
	}
	st, err := openStore(opts.StateDir, opts.Initialize)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		opts:        opts,
		store:       st,
		ctx:         ctx,
		cancel:      cancel,
		epoch:       newID("daemon_"),
		locks:       map[string]*sync.Mutex{},
		active:      map[string]chan struct{}{},
		secrets:     map[string]map[string]egress.Secret{},
		opSecrets:   map[string]map[string]egress.Secret{},
		bindings:    map[string]string{},
		connections: map[string]map[net.Conn]struct{}{},
	}
	if err = s.recoverOperations(); err != nil {
		cancel()
		return nil, errors.Join(err, st.close())
	}
	s.wg.Go(func() { ; s.maintain() })
	return s, nil
}

func (s *Service) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		s.cancel()
		var connections []net.Conn
		for _, cs := range s.connections {
			for c := range cs {
				connections = append(connections, c)
			}
		}
		s.mu.Unlock()
		for _, c := range connections {
			_ = c.Close()
		}
		s.wg.Wait()
		if s.opts.Egress != nil {
			s.closeErr = s.opts.Egress.Close()
		}
		s.closeErr = errors.Join(s.closeErr, s.store.close())
	})
	return s.closeErr
}

func (s *Service) lock(id string) func() {
	s.mu.Lock()
	m := s.locks[id]
	if m == nil {
		m = &sync.Mutex{}
		s.locks[id] = m
	}
	s.mu.Unlock()
	m.Lock()
	return m.Unlock
}

func (s *Service) PutTenant(
	ctx context.Context,
	p Principal,
	t Tenant,
) (Tenant, error) {
	if !p.Root {
		return t, api.Fail(api.Conflict, "root access required")
	}
	if t.ID == "" || t.MaxSandboxes < 1 || t.MaxStorageMiB < 1 {
		return t, api.Fail(
			api.Invalid,
			"tenant id and positive quotas required",
		)
	}
	if t.Token == "" {
		t.Token = newID("tenant_")
	}
	disk := t
	disk.Token = ""
	_, err := s.store.db.ExecContext(
		ctx,
		"INSERT INTO tenants(id,token_hash,body) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET token_hash=excluded.token_hash,body=excluded.body",
		t.ID,
		hashToken(t.Token),
		encode(disk),
	)
	return t, err
}

func (s *Service) Authenticate(
	ctx context.Context,
	token string,
) (Principal, error) {
	return s.authenticateToken(ctx, token, false)
}

// Terminated credentials remain usable only for authorized operation polling.
func (s *Service) authenticateToken(
	ctx context.Context,
	token string,
	allowTerminated bool,
) (Principal, error) {
	if token == "" {
		return Principal{}, api.Fail(api.NotFound, "not found")
	}
	if hmac.Equal(
		[]byte(hashToken(token)),
		[]byte(hashToken(s.opts.RootToken)),
	) {
		return Principal{Root: true, TenantID: "root"}, nil
	}
	var id string
	err := s.store.db.QueryRowContext(ctx, "SELECT id FROM tenants WHERE token_hash=?", hashToken(token)).
		Scan(&id)
	if err == nil {
		return Principal{TenantID: id}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Principal{}, err
	}
	// A token carries an opaque id prefix solely to avoid scanning all sandboxes.
	if parts := strings.SplitN(token, ".", 2); len(parts) == 2 {
		b, err := getSandbox(ctx, s.store.db, parts[0])
		if err == nil && (b.State != "terminated" || allowTerminated) &&
			hmac.Equal([]byte(parts[1]), []byte(s.store.token(b))) {
			return Principal{TenantID: b.TenantID, SandboxID: b.ID}, nil
		}
		if err != nil && !isCode(err, api.NotFound) {
			return Principal{}, err
		}
	}
	return Principal{}, api.Fail(api.NotFound, "not found")
}

func (s *Service) issuedToken(
	b Sandbox,
) string {
	return b.ID + "." + s.store.token(b)
}

func visible(p Principal, tenant, id string) bool {
	return p.Root ||
		(p.TenantID == tenant && (p.SandboxID == "" || p.SandboxID == id))
}

func (s *Service) Get(
	ctx context.Context,
	p Principal,
	id string,
) (Sandbox, error) {
	b, err := getSandbox(ctx, s.store.db, id)
	if err != nil {
		return b, err
	}
	if !visible(p, b.TenantID, id) {
		return Sandbox{}, api.Fail(api.NotFound, "not found")
	}
	return b, nil
}

func (s *Service) List(ctx context.Context, p Principal) ([]Sandbox, error) {
	if p.SandboxID != "" {
		return nil, api.Fail(api.Conflict, "tenant access required")
	}
	all, err := s.store.sandboxes(ctx)
	if err != nil {
		return nil, err
	}
	out := []Sandbox{}
	for _, b := range all {
		if visible(p, b.TenantID, b.ID) {
			out = append(out, b)
		}
	}
	return out, nil
}

func (s *Service) Templates(
	ctx context.Context,
	p Principal,
) ([]Template, error) {
	if p.SandboxID != "" {
		return nil, api.Fail(api.Conflict, "tenant access required")
	}
	all, err := s.store.templates(ctx)
	if err != nil {
		return nil, err
	}
	out := []Template{}
	for _, t := range all {
		if p.Root || t.TenantID == p.TenantID {
			out = append(out, t)
		}
	}
	return out, nil
}

func isCode(
	err error,
	c api.Code,
) bool {
	var e *api.Error
	return errors.As(err, &e) && e.Code == c
}

func (s *Service) ttl(n int64) (int64, error) {
	if n == 0 {
		n = 300
	}
	if n < 1 || time.Duration(n) > s.opts.MaxTTL/time.Second {
		return 0, api.Fail(
			api.Invalid,
			"ttl_seconds must be between 1 and %d",
			int64(s.opts.MaxTTL/time.Second),
		)
	}
	return n, nil
}

func splitEnv(
	env map[string]EnvVar,
) (map[string]string, map[string]egress.Secret, error) {
	visibleEnv := map[string]string{}
	secrets := map[string]egress.Secret{}
	for k, v := range env {
		if k == "" || strings.ContainsAny(k, "=\x00\n") || len(k) > 256 ||
			strings.ContainsRune(v.Value, 0) {
			return nil, nil, api.Fail(
				api.Invalid,
				"invalid environment variable",
			)
		}
		if v.IsSecret() {
			if v.Inject == nil || len(v.Inject.Hosts) == 0 ||
				v.Inject.Header == "" ||
				v.Value == "" {
				return nil, nil, api.Fail(
					api.Invalid,
					"host secret requires value, authorized hosts and header",
				)
			}
			secrets[k] = egress.Secret{
				Value:       v.Value,
				Hosts:       v.Inject.Hosts,
				Header:      v.Inject.Header,
				Placeholder: v.Inject.Placeholder,
			}
			if v.Inject.Placeholder != "" {
				visibleEnv[k] = v.Inject.Placeholder
			}
		} else {
			visibleEnv[k] = v.Value
		}
	}
	return visibleEnv, secrets, nil
}

func sanitized(c Command) Command {
	c.Env = cloneEnv(c.Env)
	for k, v := range c.Env {
		if v.IsSecret() {
			v.Value = ""
			c.Env[k] = v
		}
	}
	return c
}

func cloneEnv(e map[string]EnvVar) map[string]EnvVar {
	out := map[string]EnvVar{}
	maps.Copy(out, e)
	return out
}

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

func (s *Service) Submit(
	ctx context.Context,
	p Principal,
	key string,
	c Command,
) (Operation, error) {
	if len(key) < 1 || len(key) > 256 {
		return Operation{}, api.Fail(
			api.Invalid,
			"Idempotency-Key required (max 256 bytes)",
		)
	}
	if p.SandboxID != "" &&
		(c.SandboxID != p.SandboxID || (c.Action != "renew" && c.Action != "release")) {
		return Operation{}, api.Fail(api.NotFound, "not found")
	}
	// HMAC permits comparing retries without persisting credential material or a guessable secret hash.
	hash := s.store.sign("request-v1", p.SandboxID, string(encode(c)))
	unlock := s.lock("request:" + p.TenantID + ":" + key)
	defer unlock()
	old, err := s.store.byKey(ctx, p.TenantID, key)
	if err == nil {
		if old.Hash != hash {
			return old, api.Fail(
				api.Conflict,
				"idempotency key has different parameters",
			)
		}
		if !visible(p, old.TenantID, old.Command.SandboxID) && !p.Root {
			return Operation{}, api.Fail(api.NotFound, "not found")
		}
		s.start(old)
		return old, nil
	}
	if !isCode(err, api.NotFound) {
		return Operation{}, err
	}
	env, secrets, err := splitEnv(c.Env)
	if err != nil {
		return Operation{}, err
	}
	o := Operation{
		ID:        newID("op_"),
		Key:       key,
		TenantID:  p.TenantID,
		Hash:      hash,
		Command:   sanitized(c),
		Status:    "pending",
		CreatedAt: time.Now().UTC(),
		Result:    Result{},
	}
	o.Result.OperationID = o.ID
	// Configuration admission and binding publication share this short guard.
	// Agent calls never hold it: a newer intent can fence a blocked worker.
	if c.Action == "configure" {
		configuration := s.lock("configuration:" + c.SandboxID)
		defer configuration()
	}
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return o, err
	}
	defer func() { _ = tx.Rollback() }()
	var source Sandbox
	switch c.Action {
	case "claim", "fork":
		s.mu.Lock()
		draining := s.draining
		s.mu.Unlock()
		if draining {
			return o, api.Fail(api.Exhausted, "node is draining")
		}
		var t Template
		count := 1
		if c.Action == "claim" {
			t, err = getTemplateTx(ctx, tx, c.TemplateID)
			if err != nil {
				return o, err
			}
			if !p.Root && t.TenantID != p.TenantID {
				return o, api.Fail(api.NotFound, "not found")
			}
			if t.State != "ready" {
				return o, api.Fail(api.Conflict, "template is not ready")
			}
			if err = requireSnapshotOwner(
				ctx,
				tx,
				t.SnapshotID,
				p.TenantID,
			); err != nil {
				return o, err
			}
			o.Command.Template = &t
			o.SnapshotID = t.SnapshotID
		}
		if c.Action == "fork" {
			count = c.Count
			if count < 1 || count > 32 {
				return o, api.Fail(api.Invalid, "fork count must be 1..32")
			}
			if c.CheckpointID != "" {
				cp, e := getCheckpointTx(ctx, tx, c.CheckpointID)
				if e != nil {
					return o, e
				}
				if !p.Root && cp.TenantID != p.TenantID {
					return o, api.Fail(api.NotFound, "not found")
				}
				source = Sandbox{
					TenantID:   cp.TenantID,
					SnapshotID: cp.SnapshotID,
					Shape:      cp.Shape,
					State:      "paused",
				}
				o.Step = "checkpoint_source"
			} else {
				source, err = getSandbox(ctx, tx, c.SandboxID)
				if err != nil {
					return o, err
				}
				if !visible(p, source.TenantID, source.ID) {
					return o, api.Fail(api.NotFound, "not found")
				}
				if source.State != "running" && source.State != "paused" {
					return o, api.Fail(
						api.Conflict,
						"sandbox cannot fork in %s",
						source.State,
					)
				}
			}
			if source.TenantID != p.TenantID {
				return o, api.Fail(api.NotFound, "not found")
			}
			if err = requireSnapshotOwner(
				ctx,
				tx,
				source.SnapshotID,
				p.TenantID,
			); err != nil {
				return o, err
			}
			o.SnapshotID = source.SnapshotID
			t = Template{ID: source.TemplateID, Shape: source.Shape}
			if c.Env == nil {
				env = map[string]string{}
			}
			if c.Policy.Mode == "" {
				c.Policy = source.Policy
				o.Command.Policy = c.Policy
			}
		}
		if c.Action == "fork" && source.State == "running" {
			o.ReservedMiB = source.Shape.MemoryMiB + source.Shape.DiskMiB
		}
		if err = s.quota(
			ctx,
			tx,
			p,
			count,
			t.Shape.DiskMiB*int64(count)+o.ReservedMiB,
		); err != nil {
			return o, err
		}
		ttl, e := s.ttl(c.TTLSeconds)
		if e != nil {
			return o, e
		}
		expiry := c.OnExpire
		if expiry == "" {
			expiry = "kill"
		}
		if expiry != "kill" && expiry != "pause" {
			return o, api.Fail(api.Invalid, "on_expire must be kill or pause")
		}
		policy := c.Policy
		if policy.Mode == "" {
			policy.Mode = "relay"
		}
		if err = checkEgressPolicy(policy, len(secrets) > 0); err != nil {
			return o, err
		}
		for i := 0; i < count; i++ {
			b := Sandbox{
				ID:              newID("sbx_"),
				TenantID:        p.TenantID,
				State:           "provisioning",
				TemplateID:      t.ID,
				Shape:           t.Shape,
				TTLSeconds:      ttl,
				OnExpire:        expiry,
				AutoRenew:       c.AutoRenew,
				Env:             env,
				Policy:          policy,
				RequiresSecrets: len(secrets) > 0,
				Epoch:           newID("epoch_"),
				OperationID:     o.ID,
				CreatedAt:       time.Now().UTC(),
			}
			o.IDs = append(o.IDs, b.ID)
			if err = saveSandbox(ctx, tx, b); err != nil {
				return o, err
			}
		}
	case "pause",
		"resume",
		"release",
		"renew",
		"checkpoint",
		"configure",
		"rollback":
		source, err = getSandbox(ctx, tx, c.SandboxID)
		if err != nil {
			return o, err
		}
		if !visible(p, source.TenantID, source.ID) {
			return o, api.Fail(api.NotFound, "not found")
		}
		if source.State == "terminated" && c.Action != "release" {
			return o, api.Fail(api.Conflict, "sandbox terminated")
		}
		o.SnapshotID = source.SnapshotID
		if c.Action == "rollback" {
			cp, e := getCheckpointTx(ctx, tx, c.CheckpointID)
			if e != nil {
				return o, e
			}
			if cp.TenantID != source.TenantID || cp.SandboxID != source.ID {
				return o, api.Fail(api.NotFound, "not found")
			}
			o.SnapshotID = cp.SnapshotID
		}
		if c.Action == "renew" {
			if _, err = s.ttl(c.TTLSeconds); err != nil {
				return o, err
			}
		}
		if c.Action == "checkpoint" {
			if source.State != "running" && source.State != "paused" {
				return o, api.Fail(
					api.Conflict,
					"sandbox cannot checkpoint in %s",
					source.State,
				)
			}
			o.ReservedMiB = source.Shape.MemoryMiB + source.Shape.DiskMiB
			if err = s.quota(ctx, tx, p, 0, o.ReservedMiB); err != nil {
				return o, err
			}
			o.IDs = []string{newID("cp_")}
		}
		if c.Action == "pause" && source.State == "running" {
			o.ReservedMiB = source.Shape.MemoryMiB
			if err = s.quota(ctx, tx, p, 0, o.ReservedMiB); err != nil {
				return o, err
			}
		}
		if c.Action == "configure" {
			if c.Env == nil {
				return o, api.Fail(
					api.Invalid,
					"env is required; an empty object clears environment",
				)
			}
			policy := c.Policy
			if policy.Mode == "" {
				policy = source.Policy
			}
			if err = checkEgressPolicy(policy, len(secrets) > 0); err != nil {
				return o, err
			}
			// The configuration fence is admitted atomically with the
			// operation. It blocks automatic resume/rebind until this
			// configure delivers, is replaced by a newer explicit configure,
			// or the sandbox is released.
			if err = recordConfigFence(
				ctx,
				tx,
				c.SandboxID,
				o.ID,
			); err != nil {
				return o, err
			}
		}
	case "delete_checkpoint":
		cp, e := getCheckpointTx(ctx, tx, c.CheckpointID)
		if e != nil {
			return o, e
		}
		if !p.Root && cp.TenantID != p.TenantID {
			return o, api.Fail(api.NotFound, "not found")
		}
	case "template_put", "template_build":
		if p.SandboxID != "" || c.Template == nil {
			return o, api.Fail(api.Invalid, "template required")
		}
		t := *c.Template
		if t.ID == "" || t.Shape.VCPUs < 1 || t.Shape.VCPUs > 255 ||
			t.Shape.MemoryMiB < 128 ||
			t.Shape.MemoryMiB > 1<<24 ||
			t.Shape.DiskMiB < 64 ||
			t.Shape.DiskMiB > 1<<30 ||
			t.TargetWarm < 0 ||
			t.TargetWarm > s.opts.MaxWarm {
			return o, api.Fail(
				api.Invalid,
				"invalid template shape or pool target",
			)
		}
		existing, e := getTemplateTx(ctx, tx, t.ID)
		if e != nil && !isCode(e, api.NotFound) {
			return o, e
		}
		owner, current, e := templateIntent(ctx, tx, t.ID)
		if e != nil {
			return o, e
		}
		if owner == "" && existing.ID != "" {
			// A published row without a placeholder still proves ownership.
			owner = existing.TenantID
		}
		// Foreign and missing names share one response; root must name the
		// owning tenant explicitly and can never transfer the reservation.
		if owner != "" && owner != p.TenantID {
			return o, api.Fail(api.NotFound, "not found")
		}
		if current != "" {
			var status string
			e := tx.QueryRowContext(
				ctx,
				"SELECT status FROM operations WHERE id=?",
				current,
			).Scan(&status)
			if errors.Is(e, sql.ErrNoRows) {
				return o, api.Fail(
					api.ManualRecovery,
					"template intent references a missing operation",
				)
			}
			if e != nil {
				return o, e
			}
			if status == "pending" || status == "running" {
				return o, api.Fail(
					api.Conflict,
					"template publication is already in flight",
				)
			}
			// A terminal pointer is recycled without touching its result.
		}
		// The first admission reserves the name permanently for this tenant
		// and commits the placeholder atomically with the operation.
		if owner == "" {
			owner = p.TenantID
		}
		if e = reserveTemplateIntent(ctx, tx, t.ID, owner, o.ID); e != nil {
			return o, e
		}
		t.TenantID = p.TenantID
		t.State = "building"
		if c.Action == "template_put" {
			t.State = "ready"
			if t.SnapshotID != "" {
				if p.TenantID == "" {
					return o, api.Fail(api.Invalid, "target tenant required")
				}
				if e := requireSnapshotOwner(
					ctx,
					tx,
					t.SnapshotID,
					p.TenantID,
				); e != nil {
					if !p.Root || !isCode(e, api.NotFound) {
						return o, e
					}
					if e = recordSnapshotOwner(
						ctx,
						tx,
						t.SnapshotID,
						p.TenantID,
						o.ID,
						"external_import",
					); e != nil {
						return o, e
					}
				}
			}
			if t.SnapshotID == "" && t.Image == "" {
				return o, api.Fail(
					api.Invalid,
					"template requires image or snapshot",
				)
			}
		}
		o.Command.Template = &t
	default:
		return o, api.Fail(api.Invalid, "unsupported action %q", c.Action)
	}
	if err = saveOp(ctx, tx, o); err != nil {
		return o, err
	}
	if err = tx.Commit(); err != nil {
		return o, err
	}
	if c.Action == "configure" {
		// Revoke any binding published by a worker before this admission.
		// The durable fence prevents that worker from publishing again.
		s.closeConnections(c.SandboxID)
		_ = s.revoke(c.SandboxID)
	}
	s.mu.Lock()
	for _, id := range o.IDs {
		if strings.HasPrefix(id, "sbx_") {
			s.secrets[id] = secrets
		}
	}
	if c.Action == "configure" {
		s.opSecrets[o.ID] = secrets
	}
	s.mu.Unlock()
	s.start(o)
	return o, nil
}

func getTemplateTx(
	ctx context.Context,
	e executor,
	id string,
) (Template, error) {
	var b []byte
	err := e.QueryRowContext(ctx, "SELECT body FROM templates WHERE id=?", id).
		Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return Template{}, api.Fail(api.NotFound, "not found")
	}
	var t Template
	if err == nil {
		err = json.Unmarshal(b, &t)
	}
	return t, err
}

func getCheckpointTx(
	ctx context.Context,
	e executor,
	id string,
) (Checkpoint, error) {
	var b []byte
	err := e.QueryRowContext(ctx, "SELECT body FROM checkpoints WHERE id=?", id).
		Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, api.Fail(api.NotFound, "not found")
	}
	var cp Checkpoint
	if err == nil {
		err = json.Unmarshal(b, &cp)
	}
	return cp, err
}

func (s *Service) quota(
	ctx context.Context,
	tx *sql.Tx,
	p Principal,
	count int,
	storage int64,
) error {
	if p.Root {
		return nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT body FROM tenants WHERE id=?", p.TenantID).
		Scan(&raw); err != nil {
		return api.Fail(api.NotFound, "not found")
	}
	var t Tenant
	if err := json.Unmarshal(raw, &t); err != nil {
		return err
	}
	rows, err := tx.QueryContext(
		ctx,
		"SELECT body FROM sandboxes WHERE tenant=? AND state!='terminated'",
		p.TenantID,
	)
	if err != nil {
		return err
	}
	n := 0
	used := int64(0)
	coveredSnapshots := map[string]bool{}
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return errors.Join(err, rows.Close())
		}
		sb, e := decodeSandbox(b)
		if e != nil {
			return errors.Join(e, rows.Close())
		}
		n++
		used += sb.Shape.DiskMiB
		if sb.SnapshotID != "" {
			coveredSnapshots[sb.SnapshotID] = true
			used += sb.Shape.MemoryMiB
		}
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	rows, err = tx.QueryContext(
		ctx,
		"SELECT body FROM checkpoints WHERE tenant=?",
		p.TenantID,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b []byte
		var cp Checkpoint
		if err = rows.Scan(&b); err != nil {
			return errors.Join(err, rows.Close())
		}
		if err = json.Unmarshal(b, &cp); err != nil {
			return errors.Join(err, rows.Close())
		}
		used += cp.Shape.DiskMiB + cp.Shape.MemoryMiB
		coveredSnapshots[cp.SnapshotID] = true
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	rows, err = tx.QueryContext(ctx, `SELECT body FROM resource_recovery
        WHERE json_extract(body,'$.kind')='retired_snapshot'
        AND json_extract(body,'$.tenant_id')=?
        AND COALESCE(json_extract(body,'$.done'),0)=0`, p.TenantID)
	if err != nil {
		return err
	}
	charges := map[string]int64{}
	for rows.Next() {
		var raw []byte
		var r resourceRecovery
		if err = rows.Scan(&raw); err != nil {
			return errors.Join(err, rows.Close())
		}
		if err = json.Unmarshal(raw, &r); err != nil {
			return errors.Join(err, rows.Close())
		}
		if !coveredSnapshots[r.Snapshot] && r.MemoryMiB > charges[r.Snapshot] {
			charges[r.Snapshot] = r.MemoryMiB
		}
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, charge := range charges {
		used += charge
	}
	var reserved int64
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(CAST(json_extract(body,'$.reserved_mib') AS INTEGER)),0) FROM operations WHERE tenant=? AND status IN ('pending','running')", p.TenantID).
		Scan(&reserved); err != nil {
		return err
	}
	if n+count > t.MaxSandboxes || used+reserved+storage > t.MaxStorageMiB {
		return api.Fail(api.Exhausted, "tenant quota exceeded")
	}
	return nil
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
			return o.Result, api.Fail(api.Internal, "operation failed")
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
		return Operation{}, api.Fail(api.NotFound, "not found")
	}
	if _, err := s.resultTokens(p, o.Result); err != nil {
		return Operation{}, err
	}
	o.Result.Tokens = nil
	return o, nil
}

func (s *Service) resultTokens(p Principal, r Result) (Result, error) {
	r.Tokens = nil
	targets := append([]Sandbox(nil), r.Sandboxes...)
	if r.Sandbox != nil {
		targets = append(targets, *r.Sandbox)
	}
	for _, b := range targets {
		if !visible(p, b.TenantID, b.ID) {
			return Result{}, api.Fail(api.NotFound, "not found")
		}
	}
	if r.Checkpoint != nil &&
		!visible(p, r.Checkpoint.TenantID, r.Checkpoint.SandboxID) {
		return Result{}, api.Fail(api.NotFound, "not found")
	}
	if r.Template != nil && !visible(p, r.Template.TenantID, "") {
		return Result{}, api.Fail(api.NotFound, "not found")
	}
	for _, b := range targets {
		if b.State != "terminated" {
			if r.Tokens == nil {
				r.Tokens = map[string]string{}
			}
			r.Tokens[b.ID] = s.issuedToken(b)
		}
	}
	return r, nil
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
	captureTenant := o.TenantID
	if r.Action == "snapshot" && o.Command.SandboxID != "" {
		source, err := getSandbox(s.ctx, s.store.db, o.Command.SandboxID)
		if err != nil {
			return api.Operation{}, err
		}
		if source.VMID != r.VMID || source.Instance != r.ExpectedInstance {
			return api.Operation{}, api.Fail(
				api.ManualRecovery,
				"capture does not match the source sandbox instance",
			)
		}
		// Root's selected tenant scopes the request, not the existing data.
		captureTenant = source.TenantID
	}
	result, err := api.Wait(s.ctx, s.opts.Engine, r)
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
		if err != nil {
			o.Status = "failed"
			o.Error = internalError(err)
		} else {
			o.Status = "succeeded"
		}
		o.FinishedAt = time.Now().UTC()
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
		},
	)
}

func (s *Service) VM(ctx context.Context, b Sandbox) (api.VM, error) {
	v, err := s.opts.Engine.GetVM(ctx, b.VMID)
	if err != nil {
		return v, err
	}
	if v.Instance != b.Instance || v.Phase != "running" || !v.AgentReady ||
		!v.Verified {
		return v, api.Fail(api.Conflict, "engine VM is not verified ready")
	}
	return v, nil
}

func (s *Service) closeConnections(id string) {
	s.mu.Lock()
	var connections []net.Conn
	for c := range s.connections[id] {
		connections = append(connections, c)
	}
	delete(s.connections, id)
	s.mu.Unlock()
	for _, c := range connections {
		_ = c.Close()
	}
}

func (s *Service) track(id string, c net.Conn) func() {
	s.mu.Lock()
	if s.connections[id] == nil {
		s.connections[id] = map[net.Conn]struct{}{}
	}
	s.connections[id][c] = struct{}{}
	s.mu.Unlock()
	return func() { s.mu.Lock(); delete(s.connections[id], c); s.mu.Unlock() }
}

func (s *Service) CredentialsMissing(b Sandbox) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return b.RequiresSecrets && len(s.secrets[b.ID]) == 0
}

func (s *Service) Drain() { s.mu.Lock(); s.draining = true; s.mu.Unlock() }

func (s *Service) String() string { return fmt.Sprintf("sandboxd(%s)", s.opts.StateDir) }
