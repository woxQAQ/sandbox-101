// Package egress owns per-sandbox, host-side Cloud Hypervisor egress portals.
package egress

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrDenied  = errors.New("egress: destination denied")
	ErrRevoked = errors.New("egress: binding revoked")
	ErrClosed  = errors.New("egress: manager closed")
	ErrAudit   = errors.New("egress: audit persistence failed")
)

type Policy struct {
	Mode           string   `json:"mode"`
	AllowedHosts   []string `json:"allowed_hosts,omitempty"`
	AllowedCIDRs   []string `json:"allowed_cidrs,omitempty"`
	MaxConnections int      `json:"max_connections,omitempty"`
}

// Secret values are never serialized. Hosts are exact names, without wildcards.
// Value is the complete replacement, e.g. "Bearer sk-real".
type Secret struct {
	Value       string   `json:"-"`
	Hosts       []string `json:"hosts"`
	Header      string   `json:"header"`
	Placeholder string   `json:"placeholder,omitempty"`
}

func (Secret) String() string   { return "egress.Secret{redacted}" }
func (Secret) GoString() string { return "egress.Secret{redacted}" }

// Binding contains public activation metadata only, never secret material.
type Binding struct {
	SandboxID string `json:"sandbox_id"`
	TenantID  string `json:"tenant_id"`
	Mode      string `json:"mode"`
	VSockPath string `json:"vsock_path"`
	HTTPPath  string `json:"http_path,omitempty"`
	SOCKSPath string `json:"socks_path,omitempty"`
}

type AuditEvent struct {
	Time      time.Time `json:"time"`
	TenantID  string    `json:"tenant_id"`
	SandboxID string    `json:"sandbox_id"`
	Host      string    `json:"host,omitempty"`
	Method    string    `json:"method,omitempty"`
	Action    string    `json:"action"`
	Reason    string    `json:"reason,omitempty"`
}

type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type Options struct {
	// AuditRecord must synchronously persist the event before returning nil.
	// Errors deny credential injection before modifying headers or dialing.
	// Events contain no URLs, request headers, secret names, values or errors.
	// Callbacks must be concurrency-safe, prompt, and must not reenter Manager.
	AuditRecord func(AuditEvent) error
	// Audit is the legacy sink, used only when AuditRecord is nil. Returning
	// normally confirms persistence; use AuditRecord if writes can fail.
	Audit    func(AuditEvent)
	Resolver Resolver
	// DialContext is a trusted host integration hook. It only receives numeric
	// addresses that have passed the guard, and must honor cancellation.
	DialContext func(context.Context, string, string) (net.Conn, error)
	RootCAs     *x509.CertPool
	// AllowPrivateCIDRs permits explicit AllowedCIDRs exemptions for internal
	// unicast ranges. Transition, metadata and non-unicast ranges stay blocked.
	AllowPrivateCIDRs bool
	// InstallDirect must install all host rules synchronously, before returning.
	// It returns a teardown even on partial failure; no NIC is enabled here.
	InstallDirect func(context.Context, Binding, Policy) (func() error, error)
	// RecoverStaleSocket must prove exclusive ownership of the VM runtime
	// directory and that the previous binding is dead. Only refused, unchanged
	// Unix sockets can be recovered; active sockets and regular files cannot.
	// Leave nil unless restart reconciliation can establish that proof.
	RecoverStaleSocket func(Binding, string, os.FileInfo) bool
	// PrepareSocket grants the verified VMM identity access to a new 0600 socket.
	// It must preserve the supplied inode and never follow replacement symlinks.
	// Nil keeps ownership with the process creating the manager.
	PrepareSocket func(Binding, string, os.FileInfo) error
	DialTimeout   time.Duration
	IdleTimeout   time.Duration
}

type Manager struct {
	mu       sync.Mutex
	opts     Options
	ca       tls.Certificate
	caPEM    []byte
	bindings map[string]*binding
	closed   bool
}

type binding struct {
	info      Binding
	manager   *Manager
	policy    compiledPolicy
	secrets   []credential
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	conns     map[*trackedConn]struct{}
	inbound   int
	listeners []net.Listener
	server    *http.Server
	transport *http.Transport
	cleanup   func() error
	workers   sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func New(opts Options) (*Manager, error) {
	if opts.Resolver == nil {
		opts.Resolver = net.DefaultResolver
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 10 * time.Second
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = 90 * time.Second
	}
	if opts.DialTimeout < 0 || opts.IdleTimeout < 0 {
		return nil, errors.New("egress: invalid timeout")
	}
	if opts.DialContext == nil {
		d := &net.Dialer{Timeout: opts.DialTimeout, KeepAlive: 30 * time.Second}
		opts.DialContext = d.DialContext
	}
	if opts.RootCAs != nil {
		opts.RootCAs = opts.RootCAs.Clone()
	}
	ca, pem, err := newCA()
	if err != nil {
		return nil, err
	}
	return &Manager{opts: opts, ca: ca, caPEM: pem, bindings: make(map[string]*binding)}, nil
}

// CACertPEM returns only the public CA certificate. Install it in the guest
// trust store before exposing the proxy. Its private key never leaves Manager.
func (m *Manager) CACertPEM() []byte { return append([]byte(nil), m.caPEM...) }

// Bind is synchronous: success means both relay sockets, or direct host rules,
// are installed. ctx covers installation, not the lifetime of the binding.
// To change a policy, Revoke first. Existing bindings are never silently reused.
func (m *Manager) Bind(ctx context.Context, sandboxID, tenantID, vsockPath string, policy Policy, secrets map[string]Secret) error {
	if sandboxID == "" || tenantID == "" {
		return errors.New("egress: missing identity")
	}
	p, err := compilePolicy(policy, m.opts.AllowPrivateCIDRs)
	if err != nil {
		return err
	}
	creds, err := compileSecrets(secrets)
	if err != nil {
		return err
	}
	if p.mode != "relay" && len(creds) != 0 {
		return errors.New("egress: credentials require relay mode")
	}
	if len(creds) != 0 && m.opts.Audit == nil && m.opts.AuditRecord == nil {
		return errors.New("egress: credentials require a host audit sink")
	}
	if p.mode == "direct" && m.opts.InstallDirect == nil {
		return errors.New("egress: direct host policy installer required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if _, exists := m.bindings[sandboxID]; exists {
		return errors.New("egress: sandbox already bound")
	}
	bctx, cancel := context.WithCancel(context.Background())
	b := &binding{info: Binding{SandboxID: sandboxID, TenantID: tenantID, Mode: p.mode, VSockPath: vsockPath}, manager: m, policy: p, secrets: creds, ctx: bctx, cancel: cancel, conns: make(map[*trackedConn]struct{})}
	if p.mode == "relay" {
		if !filepath.IsAbs(vsockPath) || filepath.Clean(vsockPath) != vsockPath {
			cancel()
			return errors.New("egress: absolute clean vsock path required")
		}
		b.info.HTTPPath, b.info.SOCKSPath = vsockPath+"_2049", vsockPath+"_2050"
		for _, path := range []string{b.info.HTTPPath, b.info.SOCKSPath} {
			ln, err := b.listenUnix(path)
			if err != nil {
				_ = b.close()
				return err
			}
			b.listeners = append(b.listeners, ln)
		}
		b.startHTTP()
		b.startSOCKS()
	} else if p.mode == "direct" {
		b.cleanup, err = m.opts.InstallDirect(ctx, b.info, copyPolicy(policy))
		if err != nil {
			_ = b.close()
			return errors.New("egress: direct host policy installation failed")
		}
		if b.cleanup == nil {
			_ = b.close()
			return errors.New("egress: direct installer must return teardown")
		}
	}
	if err := ctx.Err(); err != nil {
		_ = b.close()
		return err
	}
	m.bindings[sandboxID] = b
	return nil
}

func copyPolicy(p Policy) Policy {
	p.AllowedHosts = append([]string(nil), p.AllowedHosts...)
	p.AllowedCIDRs = append([]string(nil), p.AllowedCIDRs...)
	return p
}

func (m *Manager) Binding(sandboxID string) (Binding, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.bindings[sandboxID]
	if !ok {
		return Binding{}, false
	}
	return b.info, true
}

// Revoke is idempotent and closes accepted, hijacked and upstream connections.
func (m *Manager) Revoke(sandboxID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.bindings[sandboxID]
	if b == nil {
		return nil
	}
	err := b.close()
	if err == nil {
		delete(m.bindings, sandboxID)
	}
	return err
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	var errs []error
	for id, b := range m.bindings {
		errs = append(errs, b.close())
		delete(m.bindings, id)
	}
	return errors.Join(errs...)
}

func (b *binding) close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.cancel()
		conns := make([]*trackedConn, 0, len(b.conns))
		for c := range b.conns {
			conns = append(conns, c)
		}
		b.mu.Unlock()
		for _, ln := range b.listeners {
			if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				b.closeErr = errors.Join(b.closeErr, errors.New("egress: proxy socket teardown failed"))
			}
		}
		if b.server != nil {
			_ = b.server.Close()
		}
		for _, c := range conns {
			_ = c.Close()
		}
		if b.transport != nil {
			b.transport.CloseIdleConnections()
		}
		b.workers.Wait()
	})
	// Manager serializes teardown calls. Keep a failed direct cleanup available
	// for another Revoke; closing streams is once-only, removing host rules is retryable.
	if b.cleanup != nil {
		if err := b.cleanup(); err != nil {
			return errors.Join(b.closeErr, errors.New("egress: direct host policy teardown failed"))
		}
		b.cleanup = nil
	}
	return b.closeErr
}

func (b *binding) audit(host, method, action, reason string) error {
	opts := b.manager.opts
	if opts.AuditRecord == nil && opts.Audit == nil {
		return nil
	}
	scrub := func(s string) string {
		for _, c := range b.secrets {
			s = strings.ReplaceAll(s, c.value, "[redacted]")
		}
		return s
	}
	event := AuditEvent{Time: time.Now().UTC(), SandboxID: scrub(b.info.SandboxID), TenantID: scrub(b.info.TenantID), Host: scrub(host), Method: scrub(method), Action: action, Reason: reason}
	if opts.AuditRecord != nil {
		if err := opts.AuditRecord(event); err != nil {
			return ErrAudit
		}
	} else {
		opts.Audit(event)
	}
	return nil
}

// Every accepted and upstream socket remains tracked after HTTP hijacking.
type trackedConn struct {
	net.Conn
	b       *binding
	inbound bool
	once    sync.Once
}

func (c *trackedConn) CloseWrite() error {
	if half, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite()
	}
	return c.Close()
}
func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		c.b.mu.Lock()
		delete(c.b.conns, c)
		if c.inbound {
			c.b.inbound--
		}
		c.b.mu.Unlock()
	})
	return err
}
func (b *binding) track(c net.Conn, inbound bool) (*trackedConn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		_ = c.Close()
		return nil, ErrRevoked
	}
	if inbound && b.inbound >= b.policy.max {
		_ = c.Close()
		return nil, errors.New("egress: connection limit")
	}
	t := &trackedConn{Conn: c, b: b, inbound: inbound}
	b.conns[t] = struct{}{}
	if inbound {
		b.inbound++
	}
	return t, nil
}

// Deadlines are refreshed per I/O, including hijacked MITM and SOCKS streams.
type idleConn struct {
	net.Conn
	timeout       time.Duration
	mu            sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func earlierDeadline(explicit time.Time, timeout time.Duration) time.Time {
	idle := time.Now().Add(timeout)
	if !explicit.IsZero() && explicit.Before(idle) {
		return explicit
	}
	return idle
}
func (c *idleConn) CloseWrite() error {
	if half, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite()
	}
	return c.Conn.Close()
}
func (c *idleConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	_ = c.Conn.SetReadDeadline(earlierDeadline(c.readDeadline, c.timeout))
	c.mu.Unlock()
	return c.Conn.Read(p)
}
func (c *idleConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	_ = c.Conn.SetWriteDeadline(earlierDeadline(c.writeDeadline, c.timeout))
	c.mu.Unlock()
	return c.Conn.Write(p)
}

// Preserve explicit deadlines used by net/http to interrupt background reads
// during Hijack; an idle refresh must never undo that interruption.
func (c *idleConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = t
	return c.Conn.SetReadDeadline(earlierDeadline(t, c.timeout))
}
func (c *idleConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = t
	return c.Conn.SetWriteDeadline(earlierDeadline(t, c.timeout))
}
func (c *idleConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline, c.writeDeadline = t, t
	return c.Conn.SetDeadline(earlierDeadline(t, c.timeout))
}

type guardedListener struct {
	net.Listener
	b *binding
}

func (l guardedListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		t, err := l.b.track(&idleConn{Conn: c, timeout: l.b.manager.opts.IdleTimeout}, true)
		if err == nil {
			return t, nil
		}
		if errors.Is(err, ErrRevoked) {
			return nil, net.ErrClosed
		}
	}
}

var discardLogger = log.New(io.Discard, "", 0)
