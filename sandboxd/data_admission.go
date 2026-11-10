package sandboxd

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

type trackedConn struct {
	net.Conn

	remove func()
	done   chan struct{}
	once   chan struct{}
}

func (c *trackedConn) Close() error {
	select {
	case c.once <- struct{}{}:
		close(c.done)
		c.remove()
	default:
	}
	return c.Conn.Close()
}

func (c *trackedConn) CloseWrite() error {
	if v, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return v.CloseWrite()
	}
	return nil
}

func (s *Service) trackedConnection(id string, c net.Conn) net.Conn {
	t := &trackedConn{
		Conn:   c,
		remove: s.track(id, c),
		done:   make(chan struct{}),
		once:   make(chan struct{}, 1),
	}
	go s.enforceLease(id, t.done, c)
	return t
}

func (s *Service) DataReady(
	ctx context.Context,
	p Principal,
	id string,
) (Sandbox, error) {
	// Serialize admission before choosing a durable resume attempt. Explicit resume
	// operations still converge under the sandbox lock in change.
	admit := s.lock("auto-resume:" + id)
	defer admit()
	b, err := s.Get(ctx, p, id)
	if err != nil {
		return b, err
	}
	// A configuration fence blocks admission before any automatic resume.
	// A failed fence query is never treated as the absence of a fence.
	if err = s.dataFence(ctx, b.ID); err != nil {
		return b, err
	}
	if b.State == "paused" || b.State == "resuming" {
		key := "auto-resume:" + b.ID + ":" + b.SnapshotID + ":" + fmt.Sprint(
			b.ResumeAttempt,
		)
		_, err = s.Execute(
			ctx,
			Principal{TenantID: b.TenantID},
			key,
			Command{Action: "resume", SandboxID: id},
		)
		if err != nil {
			return b, err
		}
	}
	unlock := s.lock(id)
	defer unlock()
	b, err = s.Get(ctx, p, id)
	if err != nil {
		return b, err
	}
	// Recheck under the sandbox lock: an admitted configure may have fenced
	// the sandbox between the first read and this acquisition.
	if err = s.dataFence(ctx, b.ID); err != nil {
		return b, err
	}
	if b.State != "running" || !time.Now().Before(b.Deadline) {
		return b, Fail(ErrConflict, "sandbox lease is not active")
	}
	if b.SnapshotID != "" {
		if err = s.commitRunning(
			Operation{ID: "handoff:" + b.ID + ":" + b.SnapshotID},
			&b,
		); err != nil {
			return b, err
		}
	}
	if s.credentialsMissing(b) {
		return b, Fail(
			ErrConflict,
			"host credentials must be supplied again after daemon restart",
		)
	}
	if b.Policy.Mode == "none" {
		// Confirm residual teardown before admission; LastError is diagnostic
		// and its persistence may have failed during maintenance.
		if err = s.bind(ctx, b, ""); err != nil {
			return b, Fail(
				ErrConflict,
				"egress policy could not be revoked",
			)
		}
	}
	if s.opts.Egress != nil && b.Policy.Mode != "none" {
		s.mu.Lock()
		bound := s.bindings[b.ID] != ""
		s.mu.Unlock()
		if !bound {
			o := Operation{
				ID: "rebind:" + s.epoch + ":" + b.ID + ":" + b.Instance,
			}
			if err = s.activate(o, &b, false); err != nil {
				return b, err
			}
		}
	}
	// Teardown or activation may have waited while a newer configure intent
	// committed. Refresh the fence and serialize the final decision/renewal
	// with admission, without holding this guard across those external calls.
	b, publication, err := s.lockDataAdmission(ctx, p, id)
	if err != nil {
		return b, err
	}
	defer publication()
	if b.AutoRenew {
		b.Deadline = time.Now().
			UTC().
			Add(time.Duration(b.TTLSeconds) * time.Second)
		if err = saveSandbox(ctx, s.store.db, b); err != nil {
			return b, err
		}
	}
	return b, nil
}

func (s *Service) verifiedVM(ctx context.Context, b Sandbox) (api.VM, error) {
	v, err := s.opts.Engine.GetVM(ctx, b.VMID)
	if err != nil {
		return v, engineFailure(err)
	}
	if v.Instance != b.Instance || v.Phase != "running" || !v.AgentReady ||
		!v.Verified {
		return v, Fail(ErrConflict, "engine VM is not verified ready")
	}
	return v, nil
}

// dataFence fails closed on both a pending intent and an unreadable fence.
func (s *Service) dataFence(ctx context.Context, id string) error {
	fence, err := configFence(ctx, s.store.db, id)
	if err != nil {
		return err
	}
	if fence != "" {
		return Fail(ErrConflict, "sandbox configuration change is pending")
	}
	return nil
}

// lockDataAdmission requires the sandbox lock. The returned guard serializes
// the final local decision and connection registration with configure Submit.
// VM/agent calls and teardown must finish before acquiring it; delivery and
// stream IO start after releasing it.
func (s *Service) lockDataAdmission(
	ctx context.Context, p Principal, id string,
) (Sandbox, func(), error) {
	// Reject an already committed intent without waiting for its teardown to
	// release the configuration guard.
	if err := s.dataFence(ctx, id); err != nil {
		return Sandbox{}, nil, err
	}
	unlock := s.lock("configuration:" + id)
	b, err := s.Get(ctx, p, id)
	if err == nil {
		err = s.dataFence(ctx, id)
	}
	if err == nil && (b.State != "running" || !time.Now().Before(b.Deadline)) {
		err = Fail(ErrConflict, "sandbox lease is not active")
	}
	if err == nil && s.credentialsMissing(b) {
		err = Fail(
			ErrConflict,
			"host credentials must be supplied again after daemon restart",
		)
	}
	if err == nil && b.LastError != "" {
		err = Fail(ErrConflict, "sandbox is blocked; operator review required")
	}
	if err != nil {
		unlock()
		return b, nil, err
	}
	return b, unlock, nil
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

func (s *Service) credentialsMissing(b Sandbox) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return b.RequiresSecrets && len(s.secrets[b.ID]) == 0
}
