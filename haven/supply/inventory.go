package supply

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
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
	var record TemplateRecord
	body, err := dbgen.New(query).GetTemplate(ctx, id)
	if err != nil {
		return record, err
	}
	err = json.Unmarshal(body, &record)
	return record, err
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

// reservePublication advances the publication pointer without transferring the
// permanent tenant. The comparison and conditional upsert share the caller's tx.
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
	owner, err := queries.GetPermanentOwner(ctx, record.ID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if owner != "" && owner != record.TenantID {
		return fmt.Errorf("%w: template permanent owner cannot change", errs.ErrConflict)
	}
	return store.RequireOne(queries.ReservePublication(ctx, dbgen.ReservePublicationParams{
		TemplateID:  record.ID,
		TenantID:    record.TenantID,
		OperationID: operationID,
	}))
}

func publishTemplate(ctx context.Context, query store.DBTX, desired TemplateRecord, previous *TemplateRecord) error {
	body, err := json.Marshal(desired)
	if err != nil {
		return err
	}
	queries := dbgen.New(query)
	if previous == nil {
		return store.RequireOne(queries.InsertTemplate(ctx, dbgen.InsertTemplateParams{
			ID:     desired.ID,
			Tenant: desired.TenantID,
			Body:   body,
		}))
	}
	before, err := json.Marshal(previous)
	if err != nil {
		return err
	}
	return store.RequireOne(queries.ReplaceTemplate(ctx, dbgen.ReplaceTemplateParams{
		NewBody:      body,
		ID:           desired.ID,
		Tenant:       desired.TenantID,
		PreviousBody: before,
	}))
}
