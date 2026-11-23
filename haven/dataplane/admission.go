package dataplane

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/types"
)

func (p *Plane) beginCall() error {
	if p.store.Reviewed || !p.lifetime.Begin() {
		return fmt.Errorf("%w: sandbox data admission is closed", errs.ErrConflict)
	}
	return nil
}

// Ready verifies actual resource collaboration outside the short publication
// guard, then repeats lease/fence/generation checks before local admission.
func (p *Plane) Ready(ctx context.Context, principal authorization.Principal, id string) (types.Sandbox, error) {
	return p.admit(ctx, principal, id, nil)
}

// streamAdmission pins the resource and configuration checked before dialing.
type streamAdmission struct {
	principal authorization.Principal
	identity  ephemeral.ConnectionIdentity
	resource  loom.Resource
}

func (p *Plane) admit(ctx context.Context, principal authorization.Principal, id string, stream *streamAdmission) (types.Sandbox, error) {
	if err := p.beginCall(); err != nil {
		return types.Sandbox{}, err
	}
	defer p.lifetime.End()
	if err := p.ensureResume(ctx, principal, id); err != nil {
		return types.Sandbox{}, err
	}
	releaseLifecycle := p.sandboxLifecycleLocks.Lock(id)
	defer releaseLifecycle()
	view, err := p.catalog.Get(ctx, principal, id)
	if err != nil {
		return view, err
	}
	if err = p.catalog.Unfenced(ctx, id); err != nil {
		return view, err
	}
	if view.State != "running" || !time.Now().Before(view.Deadline) {
		return view, fmt.Errorf("%w: sandbox lease is not active", errs.ErrConflict)
	}

	settings, err := sandbox.LoadSettings(ctx, p.store.Database, id)
	if err != nil {
		return view, err
	}
	if p.admissionPolicy.CredentialsMissing(id, settings) {
		return view, fmt.Errorf("%w: host credentials must be supplied again after restart", errs.ErrConflict)
	}
	if settings.ActiveEpoch != p.epoch {
		if len(settings.Secrets) > 0 {
			return view, fmt.Errorf("%w: host credentials require explicit configuration after restart", errs.ErrConflict)
		}
		if _, err = p.waitRestartBinding(ctx, view, settings); err != nil {
			return view, err
		}
		view, err = p.catalog.Get(ctx, principal, id)
		if err != nil {
			return view, err
		}
		settings, err = sandbox.LoadSettings(ctx, p.store.Database, id)
		if err != nil {
			return view, err
		}
	}
	source, err := p.catalog.ResourceForData(ctx, view)
	if err != nil {
		return view, p.rejectNetwork(view, settings, err)
	}
	observation, err := p.opts.Backend.Inspect(ctx, source.Input.Resource)
	if err != nil {
		return view, p.rejectNetwork(view, settings, err)
	}
	if !observation.Alive || observation.Paused || !observation.Booted {
		return view, p.rejectNetwork(view, settings, fmt.Errorf("%w: sandbox runtime readiness unconfirmed", errs.ErrUnknown))
	}
	switch {
	case settings.Policy.Mode != "direct" && observation.Network:
		return view, p.rejectNetwork(view, settings, fmt.Errorf("%w: sandbox NIC enabled outside direct mode", errs.ErrUnknown))
	case settings.Policy.Mode == "direct":
		if !observation.Network {
			return view, p.rejectNetwork(view, settings, fmt.Errorf("%w: direct sandbox NIC is not enabled", errs.ErrUnknown))
		}
		if err = p.verifyDirectNetwork(ctx, view, settings, source); err != nil {
			return view, p.rejectNetwork(view, settings, err)
		}
	}
	if view.Policy.Mode == "none" {
		if err = p.activations.RevokeSandbox(id); err != nil {
			return view, err
		}
	}
	unlock := p.sandboxConfigLocks.Lock(id)
	defer unlock()
	identity := ephemeral.ConnectionIdentity{
		SandboxID:  id,
		Instance:   view.Instance,
		Generation: settings.Generation,
	}
	view, settings, err = p.finalAdmission(ctx, principal, identity)
	if err != nil {
		return view, err
	}
	view.Env = settings.Env
	view, err = p.renewLease(ctx, id, view, settings)
	if err == nil && stream != nil {
		*stream = streamAdmission{
			principal: principal,
			identity:  identity,
			resource:  source.Input.Resource,
		}
	}
	return view, err
}

// finalAdmission verifies the original connection identity under the
// configuration guard. Verification never renews a lease during socket handoff
// or periodic connection enforcement.
func (p *Plane) finalAdmission(ctx context.Context, principal authorization.Principal, identity ephemeral.ConnectionIdentity) (types.Sandbox, sandbox.Settings, error) {
	id, instance, generation := identity.SandboxID, identity.Instance, identity.Generation
	closed := p.lifetime.Closing() || p.store.Reviewed
	if closed {
		return types.Sandbox{}, sandbox.Settings{}, fmt.Errorf("%w: sandbox data admission is closed", errs.ErrConflict)
	}
	if err := p.catalog.Unfenced(ctx, id); err != nil {
		return types.Sandbox{}, sandbox.Settings{}, err
	}
	view, err := p.catalog.Get(ctx, principal, id)
	if err != nil {
		return view, sandbox.Settings{}, err
	}

	settings, err := sandbox.LoadSettings(ctx, p.store.Database, id)
	if err != nil {
		return view, sandbox.Settings{}, err
	}
	p.registrationMu.Lock()
	closing := p.lifetime.Closing() || p.store.Reviewed
	bound := p.registry.Binding(id)
	p.registrationMu.Unlock()
	if closing || view.State != "running" || view.Instance != instance || settings.Generation != generation || settings.ActiveEpoch != p.epoch || !time.Now().Before(settings.Deadline) || p.admissionPolicy.CredentialsMissing(id, settings) {
		return view, sandbox.Settings{}, fmt.Errorf("%w: sandbox data admission changed", errs.ErrConflict)
	}
	source, err := p.catalog.ResourceForData(ctx, view)
	if err != nil {
		return view, sandbox.Settings{}, err
	}
	activation, err := sandbox.LoadActivation(ctx, p.store.Database, settings.Generation)
	if err != nil {
		return view, sandbox.Settings{}, err
	}
	if activation.Status != "succeeded" || activation.Phase != "complete" || activation.Input.ResourceOperation != source.OperationID || activation.Input.ResourceHash != source.Hash || activation.Input.ExpectedInstance != view.Instance || !reflect.DeepEqual(activation.Input.Settings.Env, settings.Env) || !reflect.DeepEqual(activation.Input.Settings.Policy, settings.Policy) || !reflect.DeepEqual(activation.Input.Settings.Secrets, settings.Secrets) {
		return view, sandbox.Settings{}, fmt.Errorf("%w: data configuration differs from original activation witness", errs.ErrManualRecovery)
	}
	expectedBinding := p.sign("binding-v1", settings.Generation, guest.Socket(source.Input.Resource))
	if settings.Policy.Mode != "none" && bound != expectedBinding {
		return view, sandbox.Settings{}, fmt.Errorf("%w: sandbox proxy binding is not ready", errs.ErrConflict)
	}
	return view, settings, nil
}

// renewLease runs only for a fresh readiness admission while the same
// configuration guard still protects the verified generation and lease.
func (p *Plane) renewLease(ctx context.Context, id string, view types.Sandbox, settings sandbox.Settings) (types.Sandbox, error) {
	if settings.AutoRenew {
		deadline, err := sandbox.RenewActivityLease(ctx, p.store.Database, id, settings)
		if err != nil {
			return view, err
		}
		if err = p.store.Checkpoint(); err != nil {
			return view, err
		}
		view.Deadline = deadline
	}
	return view, nil
}

// AdmissionError identifies a failure to hand off a newly dialed socket.
// The preview proxy preserves its HTTP status even after dialing has started.
type AdmissionError struct {
	error
}

func (e AdmissionError) Unwrap() error {
	return e.error
}

func (p *Plane) open(ctx context.Context, admission streamAdmission, port int) (net.Conn, error) {
	if err := p.beginCall(); err != nil {
		return nil, AdmissionError{err}
	}
	defer p.lifetime.End()
	if p.opts.Guest == nil {
		return nil, AdmissionError{fmt.Errorf("%w: guest workload effects unavailable", errs.ErrIncompatible)}
	}
	identity, principal := admission.identity, admission.principal
	var conn net.Conn
	var err error
	if port == 0 {
		conn, err = p.opts.Guest.Connect(ctx, admission.resource)
	} else {
		conn, err = p.opts.Guest.Forward(ctx, admission.resource, port)
	}
	if err != nil {
		return nil, err
	}
	unlock := p.sandboxConfigLocks.Lock(identity.SandboxID)
	if _, _, err = p.finalAdmission(ctx, principal, identity); err != nil {
		unlock()
		_ = conn.Close()
		return nil, AdmissionError{err}
	}
	p.registrationMu.Lock()
	if p.store.Reviewed || !p.lifetime.Begin() {
		p.registrationMu.Unlock()
		unlock()
		_ = conn.Close()
		return nil, AdmissionError{fmt.Errorf("%w: haven is closing", errs.ErrConflict)}
	}
	tracked := p.registry.Track(conn, identity)
	p.registrationMu.Unlock()
	unlock()
	go func() {
		defer p.lifetime.End()
		p.enforceLease(tracked, principal)
	}()
	return tracked, nil
}

// HandoffRequest binds the route's sandbox identity to its admitted upstream
// and downstream accept operation. Accept runs only under the final guard.
type HandoffRequest struct {
	SandboxID string
	Upstream  net.Conn
	Accept    func() (net.Conn, error)
}

// Attach holds the final configuration guard while the HTTP adapter takes
// ownership of its downstream socket. A fence admitted during dial rejects the
// handoff; both endpoints enter the same revocation and lease registry.
func (p *Plane) Attach(ctx context.Context, principal authorization.Principal, request HandoffRequest) (net.Conn, error) {
	id, upstream, accept := request.SandboxID, request.Upstream, request.Accept
	if err := p.beginCall(); err != nil {
		return nil, err
	}
	defer p.lifetime.End()
	up, ok := upstream.(*ephemeral.Connection)
	if !ok || !up.BelongsTo(p.registry) || up.Identity().SandboxID != id {
		return nil, fmt.Errorf("%w: connection belongs to another admission", errs.ErrInvalid)
	}
	unlock := p.sandboxConfigLocks.Lock(id)
	defer unlock()
	select {
	case <-up.Done():
		return nil, fmt.Errorf("%w: upstream admission is closed", errs.ErrConflict)
	default:
	}
	identity := up.Identity()
	if _, _, err := p.finalAdmission(ctx, principal, identity); err != nil {
		return nil, err
	}
	conn, err := accept()
	if err != nil {
		return nil, err
	}
	p.registrationMu.Lock()
	if p.store.Reviewed || !p.lifetime.Begin() {
		p.registrationMu.Unlock()
		_ = conn.Close()
		return nil, fmt.Errorf("%w: Haven is closing", errs.ErrConflict)
	}
	tracked := p.registry.Track(conn, identity)
	p.registrationMu.Unlock()
	go func() {
		defer p.lifetime.End()
		p.enforceLease(tracked, principal)
	}()
	return tracked, nil
}

func (p *Plane) Connect(ctx context.Context, principal authorization.Principal, id string) (net.Conn, error) {
	var admission streamAdmission
	if _, err := p.admit(ctx, principal, id, &admission); err != nil {
		return nil, err
	}
	return p.open(ctx, admission, 0)
}

func (p *Plane) enforceLease(conn *ephemeral.Connection, principal authorization.Principal) {
	identity := conn.Identity()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			_ = conn.Close()
			return
		case <-conn.Done():
			return
		case <-ticker.C:
			unlock := p.sandboxConfigLocks.Lock(identity.SandboxID)
			_, _, err := p.finalAdmission(p.ctx, principal, identity)
			unlock()
			if err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}
