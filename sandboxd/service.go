// Package sandboxd implements single-node sandbox orchestration over the engine API.
package sandboxd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

var randRead = rand.Read

// Service owns durable sandbox operations, recovery, and live data-path admission.
// Host secret values are held in memory and must be supplied again after restart.
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

// Open opens the state store, recovers durable operations, and starts maintenance.
// Engine, Agent, and a root token are required.
func Open(opts Options) (*Service, error) {
	if opts.Engine == nil || opts.Agent == nil || opts.RootToken == "" {
		return nil, Fail(
			ErrInvalid,
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

// Close cancels workers, closes tracked connections, and waits before closing
// the egress manager and state store. Repeated calls return the same result.
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

// Drain prevents admission of new claim and fork operations.
func (s *Service) Drain() { s.mu.Lock(); s.draining = true; s.mu.Unlock() }

func (s *Service) String() string { return fmt.Sprintf("sandboxd(%s)", s.opts.StateDir) }

// Agent supplies guest activation and transport effects for engine VM identities.
type Agent interface {
	Info(context.Context, api.VM) error
	Activate(context.Context, api.VM, Activation) error
	PrepareTemplate(context.Context, api.VM) error
	Exec(context.Context, api.VM, string, string) error
	Connect(context.Context, api.VM) (net.Conn, error)
	Forward(context.Context, api.VM, int) (net.Conn, error)
}

// Egress installs and revokes host-side policy and secret bindings for sandboxes.
type Egress interface {
	Bind(
		context.Context,
		string,
		string,
		string,
		egress.Policy,
		map[string]egress.Secret,
	) error
	Revoke(string) error
	Close() error
}

// Options configures the state store, engine and host adapters, and lease and
// maintenance limits used by Service.
type Options struct {
	StateDir   string
	Initialize bool
	RootToken  string
	Engine     api.Client
	Agent      Agent
	Egress     Egress
	MaxTTL     time.Duration
	PausedTTL  time.Duration
	Interval   time.Duration
	MaxWarm    int
	CAPEM      string
}

func newID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b[:])
}
