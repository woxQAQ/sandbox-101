package supply

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

// ResourceUsage reads retained supply users through the caller's database snapshot.
type ResourceUsage struct{}

func (ResourceUsage) VMUsers(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountWarmVM(ctx, id)
}

func (ResourceUsage) TemplateSnapshotUsers(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountSnapshotUsers(ctx, id)
}

func (ResourceUsage) PoolSnapshotUsers(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountWarmSnapshot(ctx, id)
}

// Reservation retains the template revision and original resource generation.
// Current VM ownership cannot replace Source when validating a warm handoff.
type PoolReservation struct {
	Template  TemplateRecord          `json:"template"`
	SandboxID string                  `json:"sandbox_id"`
	VMID      string                  `json:"vm_id"`
	Source    ownership.GenerationRef `json:"source"`
	Instance  string                  `json:"instance,omitempty"`
	CreatedAt time.Time               `json:"created_at"`
}

// UnresolvedTemplates verifies every catalog root against its frozen publisher.
// The caller supplies the same snapshot used for all other GC root families.
func UnresolvedTemplates(ctx context.Context, query store.DBTX) ([]string, error) {
	rows, err := dbgen.New(query).ListTemplateIDs(ctx)
	if err != nil {
		return nil, err
	}
	var unresolved []string
	catalog := NewTemplateCatalog(query)
	for _, row := range rows {
		if _, err = catalog.Owned(ctx, row.Tenant, row.ID); err != nil {
			unresolved = append(unresolved, row.ID)
		}
	}
	return unresolved, nil
}

// LoadTemplate decodes the frozen catalog value. Publication authorization is
// checked separately by TemplateCatalog against the original supply workflow.
func LoadTemplate(ctx context.Context, query store.DBTX, id string) (TemplateRecord, error) {
	version, err := dbgen.New(query).GetTemplate(ctx, id)
	if err != nil {
		return TemplateRecord{}, err
	}
	return loadTemplateVersion(ctx, query, version)
}

func loadTemplateVersion(ctx context.Context, query store.DBTX, version string) (TemplateRecord, error) {
	queries := dbgen.New(query)
	row, err := queries.GetTemplateVersion(ctx, version)
	if errors.Is(err, sql.ErrNoRows) {
		return TemplateRecord{}, fmt.Errorf("%w: template version %q missing", errs.ErrManualRecovery, version)
	}
	if err != nil {
		return TemplateRecord{}, err
	}
	record := TemplateRecord{
		ID:         row.TemplateID,
		TenantID:   row.TenantID,
		Version:    row.OperationID,
		State:      "ready",
		SnapshotID: row.SnapshotID,
		Warmup:     row.Warmup,
		TargetWarm: int(row.TargetWarm),
		Config: loom.Config{
			Image: row.Image,
			Shape: loom.Shape{
				VCPUs:     int(row.Vcpus),
				MemoryMiB: row.MemoryMib,
				DiskMiB:   row.DiskMib,
			},
		},
	}
	labels, err := queries.GetTemplateLabels(ctx, version)
	if err != nil {
		return TemplateRecord{}, err
	}
	if len(labels) != 0 {
		record.Config.Labels = make(map[string]string, len(labels))
		for _, label := range labels {
			record.Config.Labels[label.Name] = label.Value
		}
	}
	return record, nil
}

// CurrentTemplate distinguishes absence from unreadable or corrupted catalog
// data. It grants no right to replace permanent publication ownership.
func CurrentTemplate(ctx context.Context, query store.DBTX, id string) (*TemplateRecord, error) {
	record, err := LoadTemplate(ctx, query, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if record.ID != id {
		return nil, fmt.Errorf("%w: template catalog identity changed", errs.ErrManualRecovery)
	}
	return &record, nil
}

func RequireTemplateOwner(ctx context.Context, query store.DBTX, id, tenant string) error {
	count, err := dbgen.New(query).CountOwnedIntent(ctx, dbgen.CountOwnedIntentParams{
		TemplateID: id,
		TenantID:   tenant,
	})
	if err != nil || count != 1 {
		return fmt.Errorf("template owner proof missing: %v", err)
	}
	return nil
}

// reservePublication selects the candidate allowed to publish for the permanent tenant.
func (record TemplateRecord) reservePublication(ctx context.Context, query store.DBTX, previous *TemplateRecord, operationID string) error {
	current, err := CurrentTemplate(ctx, query, record.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, previous) {
		return fmt.Errorf("%w: template catalog changed before admission", errs.ErrConflict)
	}
	if current != nil && current.TenantID != record.TenantID {
		return fmt.Errorf("%w: template owner cannot change", errs.ErrConflict)
	}
	queries := dbgen.New(query)
	intent, err := queries.GetIntent(ctx, record.ID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if intent.TenantID != "" && intent.TenantID != record.TenantID {
		return fmt.Errorf("%w: template permanent owner cannot change", errs.ErrConflict)
	}
	return store.RequireOne(queries.ReservePublication(ctx, dbgen.ReservePublicationParams{
		TemplateID:           record.ID,
		TenantID:             record.TenantID,
		CandidateOperationID: operationID,
	}))
}

func publishTemplate(ctx context.Context, query store.DBTX, desired TemplateRecord, previous *TemplateRecord) error {
	queries := dbgen.New(query)
	if err := store.RequireOne(queries.InsertTemplateVersion(ctx, dbgen.InsertTemplateVersionParams{
		OperationID: desired.Version,
		TemplateID:  desired.ID,
		Image:       desired.Config.Image,
		Vcpus:       int64(desired.Config.Shape.VCPUs),
		MemoryMib:   desired.Config.Shape.MemoryMiB,
		DiskMib:     desired.Config.Shape.DiskMiB,
		Warmup:      desired.Warmup,
		SnapshotID:  desired.SnapshotID,
		TargetWarm:  int64(desired.TargetWarm),
	})); err != nil {
		return err
	}
	for name, value := range desired.Config.Labels {
		if err := store.RequireOne(queries.InsertTemplateLabel(ctx, dbgen.InsertTemplateLabelParams{
			VersionID: desired.Version,
			Name:      name,
			Value:     value,
		})); err != nil {
			return err
		}
	}
	if previous == nil {
		return store.RequireOne(queries.InsertTemplate(ctx, dbgen.InsertTemplateParams{
			ID:                   desired.ID,
			PublishedOperationID: desired.Version,
		}))
	}
	return store.RequireOne(queries.ReplaceTemplate(ctx, dbgen.ReplaceTemplateParams{
		ID:                   desired.ID,
		PreviousOperationID:  previous.Version,
		PublishedOperationID: desired.Version,
	}))
}
