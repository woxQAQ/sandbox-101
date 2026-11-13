package haven

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
	"github.com/woxQAQ/haven/loom"
)

type Sandbox struct {
	LastError       string            `json:"last_error,omitempty"`
	Epoch           string            `json:"creation_epoch"`
	CreatedAt       time.Time         `json:"created_at"`
	ProvisionSource string            `json:"provision_source,omitempty"`
	ResumeAttempt   uint64            `json:"resume_attempt,omitempty"`
	ID              string            `json:"id"`
	TenantID        string            `json:"tenant_id"`
	State           string            `json:"state"`
	VMID            string            `json:"vm_id,omitempty"`
	Instance        string            `json:"instance,omitempty"`
	SnapshotID      string            `json:"snapshot_id,omitempty"`
	TemplateID      string            `json:"template_id"`
	Shape           loom.Shape        `json:"shape"`
	Deadline        time.Time         `json:"deadline"`
	PausedUntil     time.Time         `json:"paused_until"`
	TTLSeconds      int64             `json:"ttl_seconds"`
	OnExpire        string            `json:"on_expire"`
	AutoRenew       bool              `json:"auto_renew"`
	Env             map[string]string `json:"-"`
	Policy          egress.Policy     `json:"policy"`
	RequiresSecrets bool              `json:"requires_secrets"`
	OperationID     string            `json:"operation_id,omitempty"`
}

func newSandboxIdentity(template templateRecord, source string) sandboxIdentity {
	return sandboxIdentity{TemplateID: template.ID, Shape: template.Config.Shape, Epoch: newID("epoch-"), CreatedAt: time.Now().UTC(), ProvisionSource: source}
}

func ownedSandbox(ctx context.Context, q queryer, principal Principal, id string) (sandboxRecord, error) {
	var tenant string
	if err := q.QueryRowContext(ctx, "SELECT tenant FROM sandboxes WHERE id=?", id).Scan(&tenant); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sandboxRecord{}, loom.Fail(loom.NotFound, "not found")
		}
		return sandboxRecord{}, err
	}
	sandbox, err := loadSandbox(ctx, q, id, tenant)
	if err != nil {
		return sandbox, err
	}
	if !visibleSandbox(principal, sandbox) {
		return sandboxRecord{}, loom.Fail(loom.NotFound, "not found")
	}
	return sandbox, nil
}

func (service *Service) Get(ctx context.Context, principal Principal, id string) (Sandbox, error) {
	return getSandbox(ctx, service.store.db, principal, id)
}

func getSandbox(ctx context.Context, q queryer, principal Principal, id string) (Sandbox, error) {
	record, err := ownedSandbox(ctx, q, principal, id)
	if err != nil {
		return Sandbox{}, err
	}
	if record.Identity.TemplateID == "" || record.Identity.Epoch == "" || record.Identity.CreatedAt.IsZero() {
		return Sandbox{}, loom.Fail(loom.ManualRecovery, "sandbox birth identity is incomplete")
	}
	settings, err := loadSettings(ctx, q, id)
	if err != nil {
		return Sandbox{}, err
	}
	state, operationID := record.State, record.OperationID
	var fence string
	fenceErr := q.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", id).Scan(&fence)
	if fenceErr != nil && !errors.Is(fenceErr, sql.ErrNoRows) {
		return Sandbox{}, fenceErr
	}
	if fenceErr == nil {
		lifecycle, lookup := loadLifecycle(ctx, q, fence)
		if lookup == nil && !terminal(lifecycle.Status) && lifecycle.Input.OwnerTenantID == record.TenantID && lifecycle.Input.Sandbox.ID == id {
			switch lifecycle.Input.Command.Action {
			case "rollback":
				state = "rolling_back"
			case "pause":
				state = "pausing"
			case "resume":
				state = "resuming"
			case "release":
				state = "terminated"
			}
			operationID = fence
		} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
			return Sandbox{}, lookup
		}
	}
	return Sandbox{Epoch: record.Identity.Epoch, CreatedAt: record.Identity.CreatedAt, ProvisionSource: record.Identity.ProvisionSource, LastError: record.LastError, ID: record.ID, TenantID: record.TenantID, State: state, VMID: record.VMID, Instance: record.Instance, SnapshotID: record.SnapshotID, TemplateID: record.Identity.TemplateID, Shape: record.Identity.Shape, Deadline: settings.Deadline, PausedUntil: settings.PausedUntil, TTLSeconds: settings.TTLSeconds, OnExpire: settings.OnExpire, AutoRenew: settings.AutoRenew, Env: settings.Env, Policy: settings.Policy, RequiresSecrets: len(settings.Secrets) > 0, OperationID: operationID}, nil
}

func (service *Service) Submit(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	switch command.Action {
	case "claim":
		return service.AdmitClaim(ctx, principal, key, command)
	case "configure":
		return service.AdmitConfigure(ctx, principal, key, command)
	case "checkpoint_delete":
		return service.AdmitCheckpointDelete(ctx, principal, key, command)
	case "refill":
		return service.AdmitRefill(ctx, principal, key, command)
	case "template_put", "template_build":
		return service.AdmitTemplate(ctx, principal, key, command)
	case "renew":
		return service.AdmitRenew(ctx, principal, key, command)
	case "rollback":
		return service.AdmitRollback(ctx, principal, key, command)
	case "fork":
		return service.AdmitFork(ctx, principal, key, command)
	case "resume":
		return service.AdmitResume(ctx, principal, key, command)
	case "release", "checkpoint", "pause":
		return service.AdmitLifecycle(ctx, principal, key, command)
	default:
		return Operation{}, loom.Fail(loom.Invalid, "unsupported sandbox action")
	}
}

func (service *Service) Operation(ctx context.Context, principal Principal, id string) (Operation, error) {
	if principal.Root {
		if err := service.store.db.QueryRowContext(ctx, "SELECT scope_id FROM operations WHERE id=? AND scope_kind='tenant'", id).Scan(&principal.TenantID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return Operation{}, loom.Fail(loom.NotFound, "operation not found")
			}
			return Operation{}, err
		}
	}
	operation, err := service.GetOperation(ctx, principal.TenantID, id)
	if err != nil {
		return operation, err
	}
	var mac string
	lookup := service.store.db.QueryRowContext(ctx, "SELECT request_mac FROM operation_credentials WHERE operation_id=?", id).Scan(&mac)
	if lookup == nil && strings.HasPrefix(mac, "root:") && !principal.Root {
		return Operation{}, loom.Fail(loom.NotFound, "operation not found")
	}
	if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return Operation{}, lookup
	}
	if principal.SandboxID != "" && operation.Result.SandboxID != principal.SandboxID {
		return Operation{}, loom.Fail(loom.NotFound, "operation not found")
	}
	return operation, nil
}

func (service *Service) Wait(ctx context.Context, principal Principal, id string) (Operation, error) {
	if _, err := service.Operation(ctx, principal, id); err != nil {
		return Operation{}, err
	}
	if principal.Root {
		if err := service.store.db.QueryRowContext(ctx, "SELECT scope_id FROM operations WHERE id=? AND scope_kind='tenant'", id).Scan(&principal.TenantID); err != nil {
			return Operation{}, err
		}
	}
	return service.waitOperation(ctx, "tenant", principal.TenantID, id, principal.SandboxID)
}

func (service *Service) WaitManagement(ctx context.Context, principal Principal, id string) (Operation, error) {
	if !principal.Root || principal.SandboxID != "" {
		return Operation{}, loom.Fail(loom.NotFound, "not found")
	}
	return service.waitOperation(ctx, "node", service.store.identity.Identity, id, "")
}

func (service *Service) waitOperation(ctx context.Context, kind, scope, id, sandbox string) (Operation, error) {
	for {
		operation, err := service.getOperation(ctx, kind, scope, id)
		if err != nil {
			return operation, err
		}
		if sandbox != "" && operation.Result.SandboxID != sandbox {
			return Operation{}, loom.Fail(loom.NotFound, "operation not found")
		}
		service.mu.Lock()
		active := service.active[id]
		last := service.lastErrors[id]
		service.mu.Unlock()
		// Synchronous delivery waits for the worker to release its workflow
		// guard and finish the durable HEAD update before follow-up admission.
		if terminal(operation.Status) && !active {
			if operation.Status == "failed" {
				return operation, loom.Fail(loom.Conflict, "sandbox operation failed")
			}
			return operation, nil
		}
		if !terminal(operation.Status) && !active && last != nil {
			return operation, last
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return operation, ctx.Err()
		case <-timer.C:
		}
	}
}

func (service *Service) Execute(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	operation, err := service.Submit(ctx, principal, key, command)
	if err != nil {
		return operation, err
	}
	return service.Wait(ctx, principal, operation.ID)
}
func (service *Service) Drain() { service.mu.Lock(); service.draining = true; service.mu.Unlock() }

type Tenant struct {
	ID            string `json:"id"`
	Token         string `json:"token,omitempty"`
	MaxSandboxes  int64  `json:"max_sandboxes"`
	MaxStorageMiB int64  `json:"max_storage_mib"`
}

func (service *Service) PutTenant(ctx context.Context, principal Principal, tenant Tenant) (Tenant, error) {
	if !principal.Root || tenant.ID == "" || tenant.Token == "" || tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 {
		return Tenant{}, loom.Fail(loom.Invalid, "tenant administration requires root, token and finite budgets")
	}
	record := tenantRecord{MaxSandboxes: tenant.MaxSandboxes, MaxStorageMiB: tenant.MaxStorageMiB}
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.checkAdmissionOpen(); err != nil {
		return Tenant{}, err
	}
	if err := requireWrite(service.store.db.ExecContext(ctx, "INSERT INTO tenants VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET token_hash=excluded.token_hash,body=excluded.body", tenant.ID, tokenHash(tenant.Token), encode(record))); err != nil {
		return Tenant{}, err
	}
	tenant.Token = ""
	return tenant, service.store.checkpoint()
}
