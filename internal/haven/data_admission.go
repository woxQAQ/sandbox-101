package haven

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) beginDataCall() error {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closing || service.store.reviewed {
		return loom.Fail(loom.Conflict, "sandbox data admission is closed")
	}
	service.wg.Add(1)
	return nil
}

func (service *Service) dataFence(ctx context.Context, id string) error {
	var operation string
	err := service.store.db.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", id).Scan(&operation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return loom.Fail(loom.Conflict, "sandbox configuration is fenced")
}

func (service *Service) credentialsMissing(id string, settings sandboxSettings) bool {
	if len(settings.Secrets) == 0 {
		return false
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	secrets := service.secrets[id]
	for name, descriptor := range settings.Secrets {
		secret, ok := secrets[name]
		if !ok || secret.Value == "" || secret.Header != descriptor.Header || secret.Placeholder != descriptor.Placeholder || !reflect.DeepEqual(secret.Hosts, descriptor.Hosts) {
			return true
		}
	}
	return false
}

// DataReady verifies actual resource collaboration outside the short publication
// guard, then repeats lease/fence/generation checks before local admission.
func (service *Service) DataReady(ctx context.Context, principal Principal, id string) (Sandbox, error) {
	if err := service.beginDataCall(); err != nil {
		return Sandbox{}, err
	}
	defer service.wg.Done()
	if err := service.ensureDataResume(ctx, principal, id); err != nil {
		return Sandbox{}, err
	}
	lock := service.vmLock("sandbox:" + id)
	lock.Lock()
	defer lock.Unlock()
	sandbox, err := service.Get(ctx, principal, id)
	if err != nil {
		return sandbox, err
	}
	if err = service.dataFence(ctx, id); err != nil {
		return sandbox, err
	}
	if sandbox.State != "running" || !time.Now().Before(sandbox.Deadline) {
		return sandbox, loom.Fail(loom.Conflict, "sandbox lease is not active")
	}
	settings, err := loadSettings(ctx, service.store.db, id)
	if err != nil {
		return sandbox, err
	}
	if service.credentialsMissing(id, settings) {
		return sandbox, loom.Fail(loom.Conflict, "host credentials must be supplied again after restart")
	}
	if settings.ActiveEpoch != service.epoch {
		if len(settings.Secrets) > 0 {
			return sandbox, loom.Fail(loom.Conflict, "host credentials require explicit configuration after restart")
		}
		if _, err = service.waitRestartBinding(ctx, sandbox, settings); err != nil {
			return sandbox, err
		}
		sandbox, err = service.Get(ctx, principal, id)
		if err != nil {
			return sandbox, err
		}
		settings, err = loadSettings(ctx, service.store.db, id)
		if err != nil {
			return sandbox, err
		}
	}
	source, err := service.dataResource(ctx, sandbox)
	if err != nil {
		return sandbox, service.rejectDataNetwork(sandbox, settings, err)
	}
	observation, err := service.opts.Backend.Inspect(ctx, source.Input.Resource)
	if err != nil {
		return sandbox, service.rejectDataNetwork(sandbox, settings, err)
	}
	if !observation.Alive || observation.Paused || !observation.Booted {
		return sandbox, service.rejectDataNetwork(sandbox, settings, loom.Fail(loom.Unknown, "sandbox runtime readiness unconfirmed"))
	}
	if err = service.verifyDataNetwork(ctx, sandbox, settings, source, observation); err != nil {
		return sandbox, service.rejectDataNetwork(sandbox, settings, err)
	}
	if sandbox.Policy.Mode == "none" {
		if err = service.revokeSandbox(id); err != nil {
			return sandbox, err
		}
	}
	guard := service.vmLock("configuration:" + id)
	guard.Lock()
	defer guard.Unlock()
	return service.finalDataAdmission(ctx, principal, id, sandbox.Instance, settings.Generation, true)
}

func (service *Service) dataResource(ctx context.Context, sandbox Sandbox) (resourceStep, error) {
	record, err := loadSandbox(ctx, service.store.db, sandbox.ID, sandbox.TenantID)
	if err != nil {
		return resourceStep{}, err
	}
	source, err := loadStep(ctx, service.store.db, record.SourceOperation)
	if err != nil {
		return source, err
	}
	vm, current, owner, err := service.loadVM(ctx, service.store.db, source)
	if err != nil {
		return source, err
	}
	if source.Status != "succeeded" || source.State.Witness != sandbox.Instance || current != sandbox.Instance || owner != "" || !vm.Verified || !vm.AgentReady || vm.Deleting {
		return source, loom.Fail(loom.Conflict, "sandbox instance is not verified ready")
	}
	return source, nil
}

func (service *Service) finalDataAdmission(ctx context.Context, principal Principal, id, instance, generation string, renew bool) (Sandbox, error) {
	service.mu.Lock()
	closed := service.closing || service.store.reviewed
	service.mu.Unlock()
	if closed {
		return Sandbox{}, loom.Fail(loom.Conflict, "sandbox data admission is closed")
	}
	if err := service.dataFence(ctx, id); err != nil {
		return Sandbox{}, err
	}
	sandbox, err := service.Get(ctx, principal, id)
	if err != nil {
		return sandbox, err
	}
	settings, err := loadSettings(ctx, service.store.db, id)
	if err != nil {
		return sandbox, err
	}
	service.mu.Lock()
	closing := service.closing || service.store.reviewed
	bound := service.bindings[id]
	service.mu.Unlock()
	if closing || sandbox.State != "running" || sandbox.Instance != instance || settings.Generation != generation || settings.ActiveEpoch != service.epoch || !time.Now().Before(settings.Deadline) || service.credentialsMissing(id, settings) {
		return sandbox, loom.Fail(loom.Conflict, "sandbox data admission changed")
	}
	source, err := service.dataResource(ctx, sandbox)
	if err != nil {
		return sandbox, err
	}
	activation, err := loadActivation(ctx, service.store.db, settings.Generation)
	if err != nil {
		return sandbox, err
	}
	if activation.Status != "succeeded" || activation.Phase != "complete" || activation.Input.ResourceOperation != source.OperationID || activation.Input.ResourceHash != source.Hash || activation.Input.ExpectedInstance != sandbox.Instance || !reflect.DeepEqual(activation.Input.Settings.Env, settings.Env) || !reflect.DeepEqual(activation.Input.Settings.Policy, settings.Policy) || !reflect.DeepEqual(activation.Input.Settings.Secrets, settings.Secrets) {
		return sandbox, loom.Fail(loom.ManualRecovery, "data configuration differs from original activation witness")
	}
	expectedBinding := service.store.sign("binding-v1", settings.Generation, guestSocket(source.Input.Resource))
	if settings.Policy.Mode != "none" && bound != expectedBinding {
		return sandbox, loom.Fail(loom.Conflict, "sandbox proxy binding is not ready")
	}
	if renew && settings.AutoRenew {
		old := settings
		settings.Deadline = time.Now().UTC().Add(time.Duration(settings.TTLSeconds) * time.Second)
		if err = requireWrite(service.store.db.ExecContext(ctx, "UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", encode(settings), id, encode(old))); err != nil {
			return sandbox, err
		}
		if err = service.store.checkpoint(); err != nil {
			return sandbox, err
		}
		sandbox.Deadline = settings.Deadline
	}
	return sandbox, nil
}

func (service *Service) closeConnections(id string) {
	service.mu.Lock()
	var connections []net.Conn
	for conn := range service.connections[id] {
		connections = append(connections, conn)
	}
	service.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

type admittedConnection struct {
	net.Conn

	service    *Service
	sandbox    string
	instance   string
	generation string
	done       chan struct{}
	once       sync.Once
}

func (conn *admittedConnection) Close() error {
	err := conn.Conn.Close()
	conn.once.Do(func() {
		close(conn.done)
		conn.service.mu.Lock()
		delete(conn.service.connections[conn.sandbox], conn)
		conn.service.mu.Unlock()
	})
	return err
}

func (conn *admittedConnection) CloseWrite() error {
	if half, ok := conn.Conn.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite()
	}
	return nil
}

func (service *Service) openData(ctx context.Context, principal Principal, id string, port int) (net.Conn, error) {
	if err := service.beginDataCall(); err != nil {
		return nil, err
	}
	defer service.wg.Done()
	sandbox, err := service.DataReady(ctx, principal, id)
	if err != nil {
		return nil, err
	}
	settings, err := loadSettings(ctx, service.store.db, id)
	if err != nil {
		return nil, err
	}
	source, err := service.dataResource(ctx, sandbox)
	if err != nil {
		return nil, err
	}
	if service.opts.Guest == nil {
		return nil, loom.Fail(loom.Incompatible, "guest workload effects unavailable")
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
	guard := service.vmLock("configuration:" + id)
	guard.Lock()
	if _, err = service.finalDataAdmission(ctx, principal, id, sandbox.Instance, settings.Generation, false); err != nil {
		guard.Unlock()
		_ = conn.Close()
		return nil, err
	}
	tracked := &admittedConnection{Conn: conn, service: service, sandbox: id, instance: sandbox.Instance, generation: settings.Generation, done: make(chan struct{})}
	service.mu.Lock()
	if service.closing || service.store.reviewed {
		service.mu.Unlock()
		guard.Unlock()
		_ = conn.Close()
		return nil, loom.Fail(loom.Conflict, "haven is closing")
	}
	service.wg.Add(1)
	if service.connections[id] == nil {
		service.connections[id] = map[net.Conn]struct{}{}
	}
	service.connections[id][tracked] = struct{}{}
	service.mu.Unlock()
	guard.Unlock()
	go func() {
		defer service.wg.Done()
		service.enforceDataLease(tracked, principal, sandbox.Instance, settings.Generation)
	}()
	return tracked, nil
}

// AttachData holds the final configuration guard while the HTTP adapter takes
// ownership of its downstream socket. A fence admitted during dial rejects the
// handoff; both endpoints enter the same revocation and lease registry.
func (service *Service) AttachData(ctx context.Context, principal Principal, id string, upstream net.Conn, accept func() (net.Conn, error)) (net.Conn, error) {
	if err := service.beginDataCall(); err != nil {
		return nil, err
	}
	defer service.wg.Done()
	up, ok := upstream.(*admittedConnection)
	if !ok || up.service != service || up.sandbox != id {
		return nil, loom.Fail(loom.Invalid, "connection belongs to another admission")
	}
	guard := service.vmLock("configuration:" + id)
	guard.Lock()
	defer guard.Unlock()
	select {
	case <-up.done:
		return nil, loom.Fail(loom.Conflict, "upstream admission is closed")
	default:
	}
	if _, err := service.finalDataAdmission(ctx, principal, id, up.instance, up.generation, false); err != nil {
		return nil, err
	}
	conn, err := accept()
	if err != nil {
		return nil, err
	}
	tracked := &admittedConnection{Conn: conn, service: service, sandbox: id, instance: up.instance, generation: up.generation, done: make(chan struct{})}
	service.mu.Lock()
	if service.closing || service.store.reviewed {
		service.mu.Unlock()
		_ = conn.Close()
		return nil, loom.Fail(loom.Conflict, "Haven is closing")
	}
	service.wg.Add(1)
	if service.connections[id] == nil {
		service.connections[id] = map[net.Conn]struct{}{}
	}
	service.connections[id][tracked] = struct{}{}
	service.mu.Unlock()
	go func() {
		defer service.wg.Done()
		service.enforceDataLease(tracked, principal, up.instance, up.generation)
	}()
	return tracked, nil
}

func (service *Service) Connect(ctx context.Context, principal Principal, id string) (net.Conn, error) {
	return service.openData(ctx, principal, id, 0)
}

func (service *Service) Forward(ctx context.Context, principal Principal, id string, port int) (net.Conn, error) {
	if port < 1 || port > 65535 {
		return nil, loom.Fail(loom.Invalid, "invalid forward port")
	}
	return service.openData(ctx, principal, id, port)
}

func (service *Service) enforceDataLease(conn *admittedConnection, principal Principal, instance, generation string) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-service.ctx.Done():
			_ = conn.Close()
			return
		case <-conn.done:
			return
		case <-ticker.C:
			guard := service.vmLock("configuration:" + conn.sandbox)
			guard.Lock()
			_, err := service.finalDataAdmission(service.ctx, principal, conn.sandbox, instance, generation, false)
			guard.Unlock()
			if err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}
