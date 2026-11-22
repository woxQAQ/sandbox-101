package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// Catalog projects domain facts and verified lifecycle state from the same
// connection or caller-owned transaction used for frozen publications.
type Catalog struct {
	database store.DBTX
}

func NewCatalog(database store.DBTX) *Catalog {
	return &Catalog{
		database: database,
	}
}

// OwnedRecord checks the persisted owner and authenticated visibility before
// returning the domain record used by admission and transaction proofs.
func (c *Catalog) OwnedRecord(ctx context.Context, principal authorization.Principal, id string) (ownership.Sandbox, error) {
	tenant, err := sandboxOwner(ctx, c.database, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ownership.Sandbox{}, errs.ErrNotFound
		}
		return ownership.Sandbox{}, err
	}
	// Foreign and absent objects have the same public response. Check visibility
	// before decoding any private state belonging to the owner.
	if !principal.Visible(tenant, id) {
		return ownership.Sandbox{}, errs.ErrNotVisible
	}
	return LoadSandbox(ctx, c.database, id, tenant)
}

func (c *Catalog) Get(ctx context.Context, principal authorization.Principal, id string) (types.Sandbox, error) {
	record, err := c.OwnedRecord(ctx, principal, id)
	if err != nil {
		return types.Sandbox{}, err
	}
	if record.Identity.TemplateID == "" || record.Identity.Epoch == "" || record.Identity.CreatedAt.IsZero() {
		return types.Sandbox{}, fmt.Errorf("%w: sandbox birth identity is incomplete", errs.ErrManualRecovery)
	}

	settings, err := LoadSettings(ctx, c.database, id)
	if err != nil {
		return types.Sandbox{}, err
	}

	state, operationID := record.State, record.OperationID
	fence, fenceErr := LifecycleFence(ctx, c.database, id)
	if fenceErr != nil && !errors.Is(fenceErr, sql.ErrNoRows) {
		return types.Sandbox{}, fenceErr
	}
	if fenceErr == nil {
		// Configure also holds this fence. Only a live lifecycle step changes the displayed state.
		lifecycle, err := LoadLifecycleStep(ctx, c.database, fence)
		if err == nil && !operation.Terminal(lifecycle.Status) && lifecycle.Input.OwnerTenantID == record.TenantID && lifecycle.Input.Sandbox.ID == id {
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
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return types.Sandbox{}, err
		}
	}
	return types.Sandbox{
		Epoch:           record.Identity.Epoch,
		CreatedAt:       record.Identity.CreatedAt,
		ProvisionSource: record.Identity.ProvisionSource,
		LastError:       record.LastError,
		ID:              record.ID,
		TenantID:        record.TenantID,
		State:           state,
		VMID:            record.VMID,
		Instance:        record.Instance,
		SnapshotID:      record.SnapshotID,
		TemplateID:      record.Identity.TemplateID,
		Shape:           types.Shape(record.Identity.Shape),
		Deadline:        settings.Deadline,
		PausedUntil:     settings.PausedUntil,
		TTLSeconds:      settings.TTLSeconds,
		OnExpire:        settings.OnExpire,
		AutoRenew:       settings.AutoRenew,
		Policy:          settings.Policy,
		RequiresSecrets: len(settings.Secrets) > 0,
		OperationID:     operationID,
	}, nil
}

func (c *Catalog) List(ctx context.Context, principal authorization.Principal) ([]types.Sandbox, error) {
	if principal.SandboxID != "" {
		return nil, fmt.Errorf("%w: tenant access required", errs.ErrConflict)
	}
	ids, err := dbgen.New(c.database).ListSandboxIDs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]types.Sandbox, 0, len(ids))
	for _, item := range ids {
		if !principal.Root && item.Tenant != principal.TenantID {
			continue
		}
		value, err := c.Get(ctx, principal, item.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func (c *Catalog) GetCheckpoint(ctx context.Context, principal authorization.Principal, id string) (Checkpoint, error) {
	checkpoint, err := LoadCheckpoint(ctx, c.database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, errs.ErrNotFound
	}
	if err != nil {
		return checkpoint, err
	}
	if principal.SandboxID != "" || (!principal.Root && checkpoint.TenantID != principal.TenantID) {
		return Checkpoint{}, errs.ErrNotVisible
	}
	return checkpoint, nil
}

func (c *Catalog) Checkpoints(ctx context.Context, principal authorization.Principal) ([]types.Checkpoint, error) {
	if principal.SandboxID != "" {
		return nil, fmt.Errorf("%w: tenant access required", errs.ErrConflict)
	}
	checkpoints, err := CheckpointInventory(ctx, c.database)
	out := make([]types.Checkpoint, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if principal.Root || checkpoint.TenantID == principal.TenantID {
			out = append(out, checkpoint.Public())
		}
	}
	return out, err
}

// RunningSubject identifies a running sandbox with persisted settings. It does
// not establish runtime readiness or a current proxy binding.
type RunningSubject struct {
	ID       string
	TenantID string
}

func BoundSubjects(ctx context.Context, query store.DBTX) ([]RunningSubject, error) {
	rows, err := dbgen.New(query).ListRunningSubjects(ctx)
	if err != nil {
		return nil, err
	}
	var subjects []RunningSubject
	for _, row := range rows {
		subjects = append(subjects, RunningSubject{
			ID:       row.ID,
			TenantID: row.Tenant,
		})
	}
	return subjects, nil
}

func (c *Catalog) Unfenced(ctx context.Context, id string) error {
	_, err := LifecycleFence(ctx, c.database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: sandbox configuration is fenced", errs.ErrConflict)
}

// ResourceForData verifies persisted source and instance readiness for an
// authorized view. Callers separately check the lease and live runtime state.
func (c *Catalog) ResourceForData(ctx context.Context, view types.Sandbox) (machine.ResourceStep, error) {
	record, err := LoadSandbox(ctx, c.database, view.ID, view.TenantID)
	if err != nil {
		return machine.ResourceStep{}, err
	}
	return machine.LoadReadyInstance(ctx, c.database, record.SourceOperation, view.Instance)
}
