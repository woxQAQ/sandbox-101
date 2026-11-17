package supply

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
	"github.com/woxQAQ/haven/types"
)

// TemplateCatalog verifies catalog ownership against the original publication.
// The catalog and frozen publisher are read from the same database or transaction.
type TemplateCatalog struct {
	queries  *dbgen.Queries
	database store.DBTX
}

func NewTemplateCatalog(database store.DBTX) *TemplateCatalog {
	return &TemplateCatalog{
		queries:  dbgen.New(database),
		database: database,
	}
}

func (s *TemplateCatalog) Cold(ctx context.Context, tenant, id string) (TemplateRecord, error) {
	record, err := s.Owned(ctx, tenant, id)
	if err != nil {
		return record, err
	}
	if record.SnapshotID != "" || record.TargetWarm != 0 {
		return record, fmt.Errorf("%w: cold workflow requires a cold template without a warm pool", errs.ErrIncompatible)
	}
	return record, nil
}

func (s *TemplateCatalog) Owned(ctx context.Context, tenant, id string) (TemplateRecord, error) {
	var record TemplateRecord
	body, err := s.queries.GetOwnedTemplate(ctx, dbgen.GetOwnedTemplateParams{
		ID:     id,
		Tenant: tenant,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return record, fmt.Errorf("%w: template not found", errs.ErrNotFound)
	}
	if err != nil {
		return record, err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return record, err
	}
	owner, err := s.queries.GetPublisher(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return record, fmt.Errorf("%w: template permanent ownership proof is missing", errs.ErrManualRecovery)
	}
	if err != nil {
		return record, err
	}
	proof, err := LoadSupply(ctx, s.database, owner.OperationID)
	if err != nil {
		return record, fmt.Errorf("%w: template immutable publication step is missing", errs.ErrManualRecovery)
	}
	if owner.TenantID != tenant || owner.ScopeKind != "tenant" || owner.ScopeID != proof.Input.RequestTenantID || (!proof.Input.RequesterRoot && owner.ScopeID != tenant) || proof.Input.Template.ID != id || proof.Input.Template.TenantID != tenant || (proof.Input.Command.Action != "template_put" && proof.Input.Command.Action != "template_build") {
		return record, fmt.Errorf("%w: template publisher authorization is inconsistent", errs.ErrManualRecovery)
	}
	if (owner.Status == "pending" || owner.Status == "running") && proof.Status != "succeeded" && proof.Status != "failed" {
		return record, fmt.Errorf("%w: template publication is in progress", errs.ErrConflict)
	}
	expected := proof.Input.Template
	expected.State = "ready"
	expected.SnapshotID = proof.State.SnapshotID
	if record.ID != id || record.TenantID != tenant || record.Version != owner.OperationID || owner.Status != "succeeded" || proof.Status != "succeeded" || proof.State.Phase != "complete" || !reflect.DeepEqual(record, expected) {
		return record, fmt.Errorf("%w: template immutable publication differs from catalog", errs.ErrManualRecovery)
	}
	return record, nil
}

func (s *TemplateCatalog) GetTemplate(ctx context.Context, principal authorization.Principal, id string) (operation.TemplateSpec, error) {
	if principal.SandboxID != "" {
		return operation.TemplateSpec{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	record, err := LoadTemplate(ctx, s.database, id)
	if err != nil {
		return operation.TemplateSpec{}, err
	}
	if !principal.Root && principal.TenantID != record.TenantID {
		return operation.TemplateSpec{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	verified, err := s.Owned(ctx, record.TenantID, id)
	if err != nil {
		return operation.TemplateSpec{}, err
	}
	return operation.TemplateSpec{
		ID:         verified.ID,
		TenantID:   verified.TenantID,
		Image:      verified.Config.Image,
		Shape:      verified.Config.Shape,
		SnapshotID: verified.SnapshotID,
		State:      verified.State,
		TargetWarm: verified.TargetWarm,
		Warmup:     verified.Warmup,
	}, nil
}

func (s *TemplateCatalog) Templates(ctx context.Context, principal authorization.Principal) ([]types.Template, error) {
	if principal.SandboxID != "" {
		return nil, fmt.Errorf("%w: tenant access required", errs.ErrConflict)
	}
	ids, err := s.queries.ListTemplateIDs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]types.Template, 0, len(ids))
	for _, item := range ids {
		if !principal.Root && item.Tenant != principal.TenantID {
			continue
		}
		record, err := s.Owned(ctx, item.Tenant, item.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, record.Public())
	}
	return out, nil
}
