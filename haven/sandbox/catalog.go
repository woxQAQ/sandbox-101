package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
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
func (s *Catalog) OwnedRecord(ctx context.Context, principal authorization.Principal, id string) (ownership.Sandbox, error) {
	tenant, err := sandboxOwner(ctx, s.database, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ownership.Sandbox{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
		}
		return ownership.Sandbox{}, err
	}
	record, err := LoadSandbox(ctx, s.database, id, tenant)
	if err != nil {
		return record, err
	}
	if !principal.Visible(record.TenantID, record.ID) {
		return ownership.Sandbox{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	return record, nil
}

func (s *Catalog) Get(ctx context.Context, principal authorization.Principal, id string) (View, error) {
	record, err := s.OwnedRecord(ctx, principal, id)
	if err != nil {
		return View{}, err
	}
	if record.Identity.TemplateID == "" || record.Identity.Epoch == "" || record.Identity.CreatedAt.IsZero() {
		return View{}, fmt.Errorf("%w: sandbox birth identity is incomplete", errs.ErrManualRecovery)
	}
	settings, err := LoadSettings(ctx, s.database, id)
	if err != nil {
		return View{}, err
	}
	state, operationID := record.State, record.OperationID
	fence, fenceErr := LifecycleFence(ctx, s.database, id)
	if fenceErr != nil && !errors.Is(fenceErr, sql.ErrNoRows) {
		return View{}, fenceErr
	}
	if fenceErr == nil {
		lifecycle, lookup := LoadLifecycle(ctx, s.database, fence)
		if lookup == nil && lifecycle.Status != "succeeded" && lifecycle.Status != "failed" && lifecycle.Input.OwnerTenantID == record.TenantID && lifecycle.Input.Sandbox.ID == id {
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
			return View{}, lookup
		}
	}
	return View{
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
		Shape:           record.Identity.Shape,
		Deadline:        settings.Deadline,
		PausedUntil:     settings.PausedUntil,
		TTLSeconds:      settings.TTLSeconds,
		OnExpire:        settings.OnExpire,
		AutoRenew:       settings.AutoRenew,
		Env:             settings.Env,
		Policy:          settings.Policy,
		RequiresSecrets: len(settings.Secrets) > 0,
		OperationID:     operationID,
	}, nil
}

func (s *Catalog) List(ctx context.Context, principal authorization.Principal) ([]types.Sandbox, error) {
	if principal.SandboxID != "" {
		return nil, fmt.Errorf("%w: tenant access required", errs.ErrConflict)
	}
	ids, err := dbgen.New(s.database).ListSandboxIDs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]types.Sandbox, 0, len(ids))
	for _, item := range ids {
		if !principal.Root && item.Tenant != principal.TenantID {
			continue
		}
		value, err := s.Get(ctx, principal, item.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, value.Public())
	}
	return out, nil
}

func (s *Catalog) GetCheckpoint(ctx context.Context, principal authorization.Principal, id string) (Checkpoint, error) {
	checkpoint, err := LoadCheckpoint(ctx, s.database, id)
	if err != nil {
		return checkpoint, err
	}
	if principal.SandboxID != "" || (!principal.Root && checkpoint.TenantID != principal.TenantID) {
		return Checkpoint{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	return checkpoint, nil
}

func (s *Catalog) Checkpoints(ctx context.Context, principal authorization.Principal) ([]types.Checkpoint, error) {
	if principal.SandboxID != "" {
		return nil, fmt.Errorf("%w: tenant access required", errs.ErrConflict)
	}
	checkpoints, err := CheckpointInventory(ctx, s.database)
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

func (s *Catalog) Unfenced(ctx context.Context, id string) error {
	_, err := LifecycleFence(ctx, s.database, id)
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
func (s *Catalog) ResourceForData(ctx context.Context, view View) (machine.ResourceStep, error) {
	record, err := LoadSandbox(ctx, s.database, view.ID, view.TenantID)
	if err != nil {
		return machine.ResourceStep{}, err
	}
	source, err := machine.LoadResourceStep(ctx, s.database, record.SourceOperation)
	if err != nil {
		return source, err
	}
	resource, current, owner, err := machine.LoadVM(ctx, s.database, source)
	if err != nil {
		return source, err
	}
	if source.Status != "succeeded" || source.State.Witness != view.Instance || current != view.Instance || owner != "" || !resource.Verified || !resource.AgentReady || resource.Deleting {
		return source, fmt.Errorf("%w: sandbox instance is not verified ready", errs.ErrConflict)
	}
	return source, nil
}

type CapabilityLookup struct {
	queries *dbgen.Queries
}

func NewCapabilityLookup(db store.DBTX) *CapabilityLookup {
	return &CapabilityLookup{
		queries: dbgen.New(db),
	}
}

// FindCapability includes retired identities only for historical operation
// authorization. Decoding stops at the matching capability, preserving the
// original ordered lookup's failure boundary.
func (r *CapabilityLookup) FindCapability(ctx context.Context, retired bool, matches func(ownership.Sandbox) bool) (ownership.Sandbox, error) {
	var bodies [][]byte
	var err error
	if retired {
		bodies, err = r.queries.ListAllCapabilities(ctx)
	} else {
		bodies, err = r.queries.ListActiveCapabilities(ctx)
	}
	if err != nil {
		return ownership.Sandbox{}, err
	}
	for _, body := range bodies {
		var record ownership.Sandbox
		if err := json.Unmarshal(body, &record); err != nil {
			return ownership.Sandbox{}, err
		}
		if matches(record) {
			return record, nil
		}
	}
	return ownership.Sandbox{}, sql.ErrNoRows
}
