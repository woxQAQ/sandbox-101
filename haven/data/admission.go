package data

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/sandbox"
)

func (service *Data) beginDataCall() error {
	if service.store.Reviewed || !service.lifetime.Begin() {
		return fmt.Errorf("%w: sandbox data admission is closed", errs.ErrConflict)
	}
	return nil
}

// DataReady verifies actual resource collaboration outside the short publication
// guard, then repeats lease/fence/generation checks before local admission.
func (service *Data) DataReady(ctx context.Context, principal authorization.Principal, id string) (sandbox.View, error) {
	if err := service.beginDataCall(); err != nil {
		return sandbox.View{}, err
	}
	defer service.lifetime.End()
	if err := service.ensureDataResume(ctx, principal, id); err != nil {
		return sandbox.View{}, err
	}
	lock := service.guards.Sandbox(id)
	lock.Lock()
	defer lock.Unlock()
	view, err := service.catalog.Get(ctx, principal, id)
	if err != nil {
		return view, err
	}
	if err = service.catalog.Unfenced(ctx, id); err != nil {
		return view, err
	}
	if view.State != "running" || !time.Now().Before(view.Deadline) {
		return view, fmt.Errorf("%w: sandbox lease is not active", errs.ErrConflict)
	}

	settings, err := sandbox.LoadSettings(ctx, service.store.Database, id)
	if err != nil {
		return view, err
	}
	if service.commands.CredentialsMissing(id, settings) {
		return view, fmt.Errorf("%w: host credentials must be supplied again after restart", errs.ErrConflict)
	}
	if settings.ActiveEpoch != service.epoch {
		if len(settings.Secrets) > 0 {
			return view, fmt.Errorf("%w: host credentials require explicit configuration after restart", errs.ErrConflict)
		}
		if _, err = service.waitRestartBinding(ctx, view, settings); err != nil {
			return view, err
		}
		view, err = service.catalog.Get(ctx, principal, id)
		if err != nil {
			return view, err
		}
		settings, err = sandbox.LoadSettings(ctx, service.store.Database, id)
		if err != nil {
			return view, err
		}
	}
	source, err := service.catalog.ResourceForData(ctx, view)
	if err != nil {
		return view, service.rejectDataNetwork(view, settings, err)
	}
	observation, err := service.opts.Backend.Inspect(ctx, source.Input.Resource)
	if err != nil {
		return view, service.rejectDataNetwork(view, settings, err)
	}
	if !observation.Alive || observation.Paused || !observation.Booted {
		return view, service.rejectDataNetwork(view, settings, fmt.Errorf("%w: sandbox runtime readiness unconfirmed", errs.ErrUnknown))
	}
	if err = service.verifyDataNetwork(ctx, view, settings, source, observation); err != nil {
		return view, service.rejectDataNetwork(view, settings, err)
	}
	if view.Policy.Mode == "none" {
		if err = service.activations.RevokeSandbox(id); err != nil {
			return view, err
		}
	}
	guard := service.guards.Configuration(id)
	guard.Lock()
	defer guard.Unlock()
	return service.finalDataAdmission(ctx, principal, id, view.Instance, settings.Generation, true)
}

func (service *Data) finalDataAdmission(ctx context.Context, principal authorization.Principal, id, instance, generation string, renew bool) (sandbox.View, error) {
	closed := service.lifetime.Closing() || service.store.Reviewed
	if closed {
		return sandbox.View{}, fmt.Errorf("%w: sandbox data admission is closed", errs.ErrConflict)
	}
	if err := service.catalog.Unfenced(ctx, id); err != nil {
		return sandbox.View{}, err
	}
	view, err := service.catalog.Get(ctx, principal, id)
	if err != nil {
		return view, err
	}

	settings, err := sandbox.LoadSettings(ctx, service.store.Database, id)
	if err != nil {
		return view, err
	}
	service.mu.Lock()
	closing := service.lifetime.Closing() || service.store.Reviewed
	bound := service.access.Binding(id)
	service.mu.Unlock()
	if closing || view.State != "running" || view.Instance != instance || settings.Generation != generation || settings.ActiveEpoch != service.epoch || !time.Now().Before(settings.Deadline) || service.commands.CredentialsMissing(id, settings) {
		return view, fmt.Errorf("%w: sandbox data admission changed", errs.ErrConflict)
	}
	source, err := service.catalog.ResourceForData(ctx, view)
	if err != nil {
		return view, err
	}
	activation, err := sandbox.LoadActivation(ctx, service.store.Database, settings.Generation)
	if err != nil {
		return view, err
	}
	if activation.Status != "succeeded" || activation.Phase != "complete" || activation.Input.ResourceOperation != source.OperationID || activation.Input.ResourceHash != source.Hash || activation.Input.ExpectedInstance != view.Instance || !reflect.DeepEqual(activation.Input.Settings.Env, settings.Env) || !reflect.DeepEqual(activation.Input.Settings.Policy, settings.Policy) || !reflect.DeepEqual(activation.Input.Settings.Secrets, settings.Secrets) {
		return view, fmt.Errorf("%w: data configuration differs from original activation witness", errs.ErrManualRecovery)
	}
	expectedBinding := service.sign("binding-v1", settings.Generation, guest.Socket(source.Input.Resource))
	if settings.Policy.Mode != "none" && bound != expectedBinding {
		return view, fmt.Errorf("%w: sandbox proxy binding is not ready", errs.ErrConflict)
	}
	if renew && settings.AutoRenew {
		settings.Deadline, err = sandbox.RenewActivityLease(ctx, service.store.Database, id, settings)
		if err != nil {
			return view, err
		}
		if err = service.store.Checkpoint(); err != nil {
			return view, err
		}
		view.Deadline = settings.Deadline
	}
	return view, nil
}

func (service *Data) openData(ctx context.Context, principal authorization.Principal, id string, port int) (net.Conn, error) {
	if err := service.beginDataCall(); err != nil {
		return nil, err
	}
	defer service.lifetime.End()
	view, err := service.DataReady(ctx, principal, id)
	if err != nil {
		return nil, err
	}
	settings, err := sandbox.LoadSettings(ctx, service.store.Database, id)
	if err != nil {
		return nil, err
	}
	source, err := service.catalog.ResourceForData(ctx, view)
	if err != nil {
		return nil, err
	}
	if service.opts.Guest == nil {
		return nil, fmt.Errorf("%w: guest workload effects unavailable", errs.ErrIncompatible)
	}
	var conn net.Conn
	if port == 0 {
		conn, err = service.opts.Guest.Connect(ctx, source.Input.Resource)
	} else {
		conn, err = service.opts.Guest.Forward(ctx, source.Input.Resource, port)
	}
	if err != nil {
		return nil, err
	}
	guard := service.guards.Configuration(id)
	guard.Lock()
	if _, err = service.finalDataAdmission(ctx, principal, id, view.Instance, settings.Generation, false); err != nil {
		guard.Unlock()
		_ = conn.Close()
		return nil, err
	}
	service.mu.Lock()
	if service.store.Reviewed || !service.lifetime.Begin() {
		service.mu.Unlock()
		guard.Unlock()
		_ = conn.Close()
		return nil, fmt.Errorf("%w: haven is closing", errs.ErrConflict)
	}
	tracked := service.access.Track(conn, access.ConnectionIdentity{
		SandboxID:  id,
		Instance:   view.Instance,
		Generation: settings.Generation,
	})
	service.mu.Unlock()
	guard.Unlock()
	go func() {
		defer service.lifetime.End()
		service.enforceDataLease(tracked, principal, view.Instance, settings.Generation)
	}()
	return tracked, nil
}

// AttachData holds the final configuration guard while the HTTP adapter takes
// ownership of its downstream socket. A fence admitted during dial rejects the
// handoff; both endpoints enter the same revocation and lease registry.
func (service *Data) AttachData(ctx context.Context, principal authorization.Principal, id string, upstream net.Conn, accept func() (net.Conn, error)) (net.Conn, error) {
	if err := service.beginDataCall(); err != nil {
		return nil, err
	}
	defer service.lifetime.End()
	up, ok := upstream.(*access.Connection)
	if !ok || !up.BelongsTo(service.access) || up.Identity().SandboxID != id {
		return nil, fmt.Errorf("%w: connection belongs to another admission", errs.ErrInvalid)
	}
	guard := service.guards.Configuration(id)
	guard.Lock()
	defer guard.Unlock()
	select {
	case <-up.Done():
		return nil, fmt.Errorf("%w: upstream admission is closed", errs.ErrConflict)
	default:
	}
	identity := up.Identity()
	if _, err := service.finalDataAdmission(ctx, principal, id, identity.Instance, identity.Generation, false); err != nil {
		return nil, err
	}
	conn, err := accept()
	if err != nil {
		return nil, err
	}
	service.mu.Lock()
	if service.store.Reviewed || !service.lifetime.Begin() {
		service.mu.Unlock()
		_ = conn.Close()
		return nil, fmt.Errorf("%w: Haven is closing", errs.ErrConflict)
	}
	tracked := service.access.Track(conn, identity)
	service.mu.Unlock()
	go func() {
		defer service.lifetime.End()
		service.enforceDataLease(tracked, principal, identity.Instance, identity.Generation)
	}()
	return tracked, nil
}

func (service *Data) Connect(ctx context.Context, principal authorization.Principal, id string) (net.Conn, error) {
	return service.openData(ctx, principal, id, 0)
}

func (service *Data) Forward(ctx context.Context, principal authorization.Principal, id string, port int) (net.Conn, error) {
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("%w: invalid forward port", errs.ErrInvalid)
	}
	return service.openData(ctx, principal, id, port)
}

func (service *Data) enforceDataLease(conn *access.Connection, principal authorization.Principal, instance, generation string) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-service.ctx.Done():
			_ = conn.Close()
			return
		case <-conn.Done():
			return
		case <-ticker.C:
			id := conn.Identity().SandboxID
			guard := service.guards.Configuration(id)
			guard.Lock()
			_, err := service.finalDataAdmission(service.ctx, principal, id, instance, generation, false)
			guard.Unlock()
			if err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}
