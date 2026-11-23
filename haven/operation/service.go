package operation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/store"
)

var errFailed = fmt.Errorf("%w: operation failed", errs.ErrConflict)

// IsTerminalFailure reports whether the wait confirmed this operation's terminal
// failure after the worker released custody. A child worker can also fail
// while its parent operation is still pending.
func (r State) IsTerminalFailure(waitErr error) bool {
	return r.Status == "failed" && errors.Is(waitErr, errFailed)
}

// WorkerReader observes process-local completion without granting dispatch or
// shutdown authority. Durable status remains owned by the operation repository.
type WorkerReader interface {
	// State reports process-local worker custody and the last execution error.
	State(operationID string) execution.WorkerState
}

// Monitor authorizes scoped reads and observes accepted operations until their
// durable result is terminal and the worker has released custody.
type Monitor struct {
	repository *Repository
	workers    WorkerReader
	nodeID     string
}

func NewMonitor(database *store.Database, workers WorkerReader, nodeID string) *Monitor {
	return &Monitor{
		repository: New(database),
		workers:    workers,
		nodeID:     nodeID,
	}
}

// GetForTenant reads state in the tenant scope supplied by the caller.
// The caller is responsible for establishing authority to read that scope.
func (o *Monitor) GetForTenant(ctx context.Context, tenantID, operationID string) (State, error) {
	return o.repository.GetScopedState(ctx, "tenant", tenantID, operationID)
}

func (o *Monitor) GetManagementOperation(ctx context.Context, principal authorization.Principal, id string) (State, error) {
	if !principal.Root || principal.SandboxID != "" {
		return State{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	return o.repository.GetScopedState(ctx, "node", o.nodeID, id)
}

// Get returns operation state after checking the principal's access.
func (o *Monitor) Get(ctx context.Context, principal authorization.Principal, operationID string) (State, error) {
	record, _, err := o.authorized(ctx, principal, operationID)
	return record, err
}

// authorized resolves root reads to the immutable tenant scope and preserves
// root-only command credentials and sandbox capability confinement.
func (o *Monitor) authorized(ctx context.Context, principal authorization.Principal, id string) (State, string, error) {
	tenant := principal.TenantID
	if principal.Root {
		var err error
		tenant, err = o.repository.TenantScope(ctx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return State{}, "", fmt.Errorf("%w: operation not found", errs.ErrNotFound)
		}
		if err != nil {
			return State{}, "", err
		}
	}
	record, err := o.GetForTenant(ctx, tenant, id)
	if err != nil {
		return record, "", err
	}
	mac, lookup := o.repository.CommandMAC(ctx, id)
	if lookup == nil && strings.HasPrefix(mac, "root:") && !principal.Root {
		return State{}, "", fmt.Errorf("%w: operation not found", errs.ErrNotFound)
	}
	if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return State{}, "", lookup
	}
	if principal.SandboxID != "" && record.Result.SandboxID != principal.SandboxID {
		return State{}, "", fmt.Errorf("%w: operation not found", errs.ErrNotFound)
	}
	return record, tenant, nil
}

func (o *Monitor) Wait(ctx context.Context, principal authorization.Principal, id string) (State, error) {
	_, tenant, err := o.authorized(ctx, principal, id)
	if err != nil {
		return State{}, err
	}
	return o.wait(ctx, Scope{
		Kind: "tenant",
		ID:   tenant,
	}, id, principal.SandboxID)
}

// WaitAccepted waits for an already accepted tenant operation during workflow
// orchestration. Its caller must separately verify frozen parent/child authority;
// observing completion here does not grant permission to hand off responsibility.
func (o *Monitor) WaitAccepted(ctx context.Context, tenant, id string) (State, error) {
	return o.wait(ctx, Scope{
		Kind: "tenant",
		ID:   tenant,
	}, id, "")
}

func (o *Monitor) WaitManagement(ctx context.Context, principal authorization.Principal, id string) (State, error) {
	if !principal.Root || principal.SandboxID != "" {
		return State{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	return o.wait(ctx, Scope{
		Kind: "node",
		ID:   o.nodeID,
	}, id, "")
}

// wait observes the durable result and process-local worker custody. Recovery and
// dispatch are owned by the workflow that admitted the operation.
func (o *Monitor) wait(ctx context.Context, scope Scope, id, sandbox string) (State, error) {
	for {
		record, err := o.repository.GetScopedState(ctx, scope.Kind, scope.ID, id)
		if err != nil {
			return record, err
		}
		if sandbox != "" && record.Result.SandboxID != sandbox {
			return State{}, fmt.Errorf("%w: operation not found", errs.ErrNotFound)
		}
		worker := o.workers.State(id)
		terminal := record.Status == "succeeded" || record.Status == "failed"
		// Synchronous delivery waits for the worker to release its workflow
		// guard and finish the durable HEAD update before follow-up admission.
		if terminal && !worker.Active {
			if record.Status == "failed" {
				return record, errFailed
			}
			return record, nil
		}
		if !terminal && !worker.Active && worker.Err != nil {
			return record, worker.Err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return record, ctx.Err()
		case <-timer.C:
		}
	}
}
