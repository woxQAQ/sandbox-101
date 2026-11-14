package haven

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
)

var ErrInterrupted = errors.New("accepted Haven intent retained for recovery")

// Options requires an explicit host-effects implementation. The daemon
// command supplies loom/host; fixtures inject their own owned runtime state.
type Options struct {
	Context      context.Context
	StateDir     string
	Initialize   bool
	Backend      loom.Backend
	MaxMemoryMiB int64
	MaxVCPUs     int
	SafetyBytes  int64
	Guest        Guest
	Egress       Egress
	// EgressFactory binds callbacks before startup reconciliation can use portals.
	EgressFactory func(*Service) (Egress, string, error)
	RootToken     string
	CAPEM         string
	MaxTTL        time.Duration
	PausedTTL     time.Duration
	Interval      time.Duration
	MaxWarm       int
	Fault         func(string) error
}

// Service owns the single store, resource workflows and sandbox admission.
// A completed System step alone leaves its sandbox provisioning until activation.
type Service struct {
	artifacts   sync.Mutex
	gcMu        sync.Mutex
	opSecrets   map[string]map[string]egress.Secret
	secrets     map[string]map[string]egress.Secret
	bindings    map[string]string
	connections map[string]map[net.Conn]struct{}
	epoch       string
	draining    bool
	opts        Options
	store       *store
	ctx         context.Context
	cancel      context.CancelFunc
	admission   sync.Mutex
	capture     chan struct{}
	mu          sync.Mutex
	locks       map[string]*sync.Mutex
	active      map[string]bool
	lastErrors  map[string]error
	closing     bool
	wg          sync.WaitGroup
	shutdown    sync.Once
	stopped     chan struct{}
	closeErr    error
}

func Open(opts Options) (*Service, error) {
	if opts.Backend == nil || opts.MaxMemoryMiB <= 0 || opts.MaxVCPUs <= 0 || opts.SafetyBytes < 0 {
		return nil, fmt.Errorf("%w: runtime and positive host budgets are required", ErrInvalid)
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
	if opts.Egress != nil && opts.EgressFactory != nil {
		return nil, fmt.Errorf("%w: egress and egress factory are mutually exclusive", ErrInvalid)
	}
	state, err := openStore(opts.StateDir, opts.Initialize)
	if err != nil {
		return nil, err
	}
	parent := opts.Context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	service := &Service{
		epoch:       newID("daemon-"),
		opSecrets:   map[string]map[string]egress.Secret{},
		secrets:     map[string]map[string]egress.Secret{},
		bindings:    map[string]string{},
		connections: map[string]map[net.Conn]struct{}{},
		opts:        opts,
		store:       state,
		ctx:         ctx,
		cancel:      cancel,
		locks:       map[string]*sync.Mutex{},
		active:      map[string]bool{},
		lastErrors:  map[string]error{},
		stopped:     make(chan struct{}),
		capture:     make(chan struct{}, 1),
	}
	if opts.EgressFactory != nil {
		service.opts.Egress, service.opts.CAPEM, err = opts.EgressFactory(service)
		if err != nil {
			closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			return nil, errors.Join(err, service.Close(closeCtx))
		}
	}
	if err := service.Reconcile(); err != nil {
		closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		return nil, errors.Join(err, service.Close(closeCtx))
	}
	return service, nil
}

func (service *Service) Close(ctx context.Context) error {
	service.mu.Lock()
	service.closing = true
	service.cancel()
	service.shutdown.Do(func() {
		go func() {
			service.mu.Lock()
			var dataConnections []net.Conn
			for _, connections := range service.connections {
				for connection := range connections {
					dataConnections = append(dataConnections, connection)
				}
			}
			service.mu.Unlock()
			for _, connection := range dataConnections {
				_ = connection.Close()
			}
			service.wg.Wait()
			if service.opts.Egress != nil {
				service.closeErr = service.opts.Egress.Close()
			}
			service.admission.Lock()
			if closer, ok := service.opts.Backend.(interface{ Close() error }); ok {
				service.closeErr = errors.Join(service.closeErr, closer.Close())
			}
			service.closeErr = errors.Join(service.closeErr, service.store.close())
			service.admission.Unlock()
			close(service.stopped)
		}()
	})
	service.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err() // Finalization retains the store and writer lock.
	case <-service.stopped:
		return service.closeErr
	}
}

func (service *Service) checkAdmissionOpen() error {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.store.reviewed {
		return fmt.Errorf("%w: reviewed metadata rollback does not authorize new effects", ErrManualRecovery)
	}
	if service.closing {
		return fmt.Errorf("%w: Haven is closing", ErrConflict)
	}
	return nil
}

func (service *Service) vmLock(id string) *sync.Mutex {
	service.mu.Lock()
	defer service.mu.Unlock()
	lock := service.locks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		service.locks[id] = lock
	}
	return lock
}

func (service *Service) fault(point string) error {
	if service.opts.Fault != nil {
		return service.opts.Fault(point)
	}
	return nil
}

func (service *Service) dispatch(operation string) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closing || service.store.reviewed || service.active[operation] {
		return
	}
	service.active[operation] = true
	service.wg.Go(func() {
		err := service.run(operation)
		service.mu.Lock()
		delete(service.active, operation)
		service.lastErrors[operation] = err
		service.mu.Unlock()
	})
}

// Reconcile reads this store's unfinished steps; it never discovers ownership
// from resource directories or asks a second operation service for receipts.
func (service *Service) Reconcile() error {
	if service.store.reviewed {
		return nil
	}
	rows, err := service.store.db.QueryContext(service.ctx, "SELECT DISTINCT operation_id FROM operation_steps WHERE status IN ('pending','running') ORDER BY operation_id")
	if err != nil {
		return err
	}
	var operations []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		operations = append(operations, id)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	for _, operation := range operations {
		service.dispatch(operation)
	}
	return service.reconcileBindings()
}

func (service *Service) GetOperation(ctx context.Context, tenant, id string) (Operation, error) {
	return service.getOperation(ctx, "tenant", tenant, id)
}

func (service *Service) GetManagementOperation(ctx context.Context, principal Principal, id string) (Operation, error) {
	if !principal.Root || principal.SandboxID != "" {
		return Operation{}, fmt.Errorf("%w: not found", ErrNotFound)
	}
	return service.getOperation(ctx, "node", service.store.identity.Identity, id)
}

func (service *Service) getOperation(ctx context.Context, kind, scope, id string) (Operation, error) {
	return service.getOperationIn(ctx, service.store.db, kind, scope, id)
}

func (service *Service) getOperationIn(ctx context.Context, q queryer, kind, scope, id string) (Operation, error) {
	var operation Operation
	var body, request []byte
	var hash string
	err := q.QueryRowContext(ctx, "SELECT id,idem_key,status,body,request_hash,request FROM operations WHERE id=? AND scope_kind=? AND scope_id=?", id, kind, scope).Scan(&operation.ID, &operation.Key, &operation.Status, &body, &hash, &request)
	if errors.Is(err, sql.ErrNoRows) {
		return operation, fmt.Errorf("%w: operation not found", ErrNotFound)
	}
	if err != nil {
		return operation, err
	}
	if digest(request) != hash {
		return operation, fmt.Errorf("operation request identity corrupted")
	}
	if err := json.Unmarshal(body, &operation.Result); err != nil {
		return operation, err
	}
	return operation, nil
}

func newID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(bytes[:])
}

func encode(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func digest(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func requireWrite(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("required responsibility write has no result")
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("required responsibility write affected %d rows", count)
	}
	return nil
}
