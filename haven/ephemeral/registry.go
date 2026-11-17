// Package ephemeral owns process-local credentials, bindings and admitted
// sockets. Nothing here survives a restart: the daemon epoch is regenerated,
// secrets stay memory-only by design, and tracked connections die with the
// process. It carries no database, workflow runner or application reference.
package ephemeral

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"slices"
	"sync"

	"github.com/woxQAQ/haven/egress"
)

type Registry struct {
	epoch         string
	mu            sync.Mutex
	operations    map[string]map[string]egress.Secret
	sandboxes     map[string]map[string]egress.Secret
	bindings      map[string]string
	bindingErrors map[string]error
	connections   map[string]map[*Connection]struct{}
}

func New() *Registry {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		panic(err)
	}
	return &Registry{
		epoch:         "daemon-" + hex.EncodeToString(entropy[:]),
		operations:    make(map[string]map[string]egress.Secret),
		sandboxes:     make(map[string]map[string]egress.Secret),
		bindings:      make(map[string]string),
		bindingErrors: make(map[string]error),
		connections:   make(map[string]map[*Connection]struct{}),
	}
}

// Epoch is the immutable process identity used by activation and data bindings.
func (r *Registry) Epoch() string {
	return r.epoch
}

func cloneSecrets(input map[string]egress.Secret) map[string]egress.Secret {
	if input == nil {
		return nil
	}
	out := make(map[string]egress.Secret, len(input))
	for name, secret := range input {
		secret.Hosts = slices.Clone(secret.Hosts)
		out[name] = secret
	}
	return out
}

// Reads and writes copy descriptors so callers cannot mutate registered secrets
// after releasing the registry mutex or sharing a fork's original credentials.
func (r *Registry) OperationSecrets(id string) map[string]egress.Secret {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneSecrets(r.operations[id])
}

func (r *Registry) SetOperationSecrets(id string, secrets map[string]egress.Secret) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.operations[id] = cloneSecrets(secrets)
}

func (r *Registry) CopyOperationSecrets(parent string, children ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, child := range children {
		r.operations[child] = cloneSecrets(r.operations[parent])
	}
}

func (r *Registry) DropOperationSecrets(ids ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		delete(r.operations, id)
	}
}

// StageSandboxSecrets freezes the current credentials for one accepted operation.
func (r *Registry) StageSandboxSecrets(operation, sandbox string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.operations[operation] = cloneSecrets(r.sandboxes[sandbox])
}

func (r *Registry) RetireCredentials(sandbox, operation string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sandboxes, sandbox)
	delete(r.operations, operation)
}

func (r *Registry) SandboxSecrets(id string) map[string]egress.Secret {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneSecrets(r.sandboxes[id])
}

func (r *Registry) SetSandboxSecrets(id string, secrets map[string]egress.Secret) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sandboxes[id] = cloneSecrets(secrets)
}

func (r *Registry) DropSandboxSecrets(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sandboxes, id)
}

// RecordBindingError retains the last maintenance failure for diagnostics.
// A later successful retry does not erase the previous failure.
func (r *Registry) RecordBindingError(id string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bindingErrors[id] = err
}

func (r *Registry) BindingError(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bindingErrors[id]
}

func (r *Registry) Binding(id string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bindings[id]
}

func (r *Registry) SetBinding(id, fingerprint string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bindings[id] = fingerprint
}

func (r *Registry) Bind(id, fingerprint string, secrets map[string]egress.Secret) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bindings[id] = fingerprint
	r.sandboxes[id] = cloneSecrets(secrets)
}

// ForgetBinding revokes only the local binding. Credentials can still be needed
// by a paused sandbox's explicit resume and are retired separately.
func (r *Registry) ForgetBinding(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.bindings, id)
}

// Track is called while the application's final admission/closing guard is held.
// The registry does not independently authorize a socket or extend its lease.
func (r *Registry) Track(conn net.Conn, identity ConnectionIdentity) *Connection {
	tracked := &Connection{
		Conn:     conn,
		registry: r,
		identity: identity,
		done:     make(chan struct{}),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.connections[identity.SandboxID] == nil {
		r.connections[identity.SandboxID] = make(map[*Connection]struct{})
	}
	r.connections[identity.SandboxID][tracked] = struct{}{}
	return tracked
}

func (r *Registry) remove(conn *Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	connections := r.connections[conn.identity.SandboxID]
	delete(connections, conn)
	if len(connections) == 0 {
		delete(r.connections, conn.identity.SandboxID)
	}
}

func (r *Registry) Connections(id string) []net.Conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []net.Conn
	for conn := range r.connections[id] {
		out = append(out, conn)
	}
	return out
}

// CloseSandboxConnections closes a snapshot so connection retirement can
// unregister each connection without holding the registry mutex.
func (r *Registry) CloseSandboxConnections(id string) {
	connections := r.Connections(id)
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func (r *Registry) AllConnections() []net.Conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []net.Conn
	for _, connections := range r.connections {
		for conn := range connections {
			out = append(out, conn)
		}
	}
	return out
}
