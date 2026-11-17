// Package quota owns durable resource references, costs and reservations.
package quota

import (
	"context"

	"github.com/woxQAQ/haven/quota/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// Holder identifies one durable responsibility destination. Kind and ID travel
// together when verifying a cost against its original owner.
type Holder struct {
	Kind string
	ID   string
}

// ResourceReference names a holder that prevents a resource from becoming
// unreachable. It is distinct from the holder of a tenant's charged cost.
type ResourceReference struct {
	Kind       string
	ID         string
	HolderKind string
	HolderID   string
}

// CostReference identifies the holder of an existing tenant cost row. Removing
// a reference alone never removes the cost or proves external reclamation.
type CostReference struct {
	TenantID   string
	Kind       string
	Key        string
	HolderKind string
	HolderID   string
}

type Repository struct {
	queries *dbgen.Queries
}

func New(query store.DBTX) *Repository {
	return &Repository{
		queries: dbgen.New(query),
	}
}

// CountStorageCostReference checks the original storage charge and its holder.
// Slot units are not part of this historical disk proof.
func (r *Repository) CountStorageCostReference(ctx context.Context, cost CostIdentity, storageMiB int64, holder Holder) (int64, error) {
	return r.queries.CountStorageCostReference(ctx, dbgen.CountStorageCostReferenceParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		StorageMib:        storageMiB,
		SourceOperationID: cost.SourceOperation,
		HolderKind:        holder.Kind,
		HolderID:          holder.ID,
	})
}

func (r *Repository) CountTenantCostHolders(ctx context.Context, tenant, kind, key string) (int64, error) {
	return r.queries.CountTenantCostHolders(ctx, dbgen.CountTenantCostHoldersParams{
		TenantID:    tenant,
		Kind:        kind,
		ResourceKey: key,
	})
}

func (r *Repository) CountCostReference(ctx context.Context, reference CostReference) (int64, error) {
	return r.queries.CountCostReference(ctx, dbgen.CountCostReferenceParams{
		TenantID:    reference.TenantID,
		Kind:        reference.Kind,
		ResourceKey: reference.Key,
		HolderKind:  reference.HolderKind,
		HolderID:    reference.HolderID,
	})
}

func (r *Repository) ResourceHolderIDs(ctx context.Context, kind, id, holderKind string) ([]string, error) {
	return r.queries.ResourceHolderIDs(ctx, dbgen.ResourceHolderIDsParams{
		ResourceKind: kind,
		ResourceID:   id,
		HolderKind:   holderKind,
	})
}

func (r *Repository) CountResourceHolders(ctx context.Context, kind, id string) (int64, error) {
	return r.queries.CountResourceHolders(ctx, dbgen.CountResourceHoldersParams{
		ResourceKind: kind,
		ResourceID:   id,
	})
}

// CountCostHolders includes every tenant's reference to the resource key.
func (r *Repository) CountCostHolders(ctx context.Context, kind, key string) (int64, error) {
	return r.queries.CountCostHolders(ctx, dbgen.CountCostHoldersParams{
		Kind:        kind,
		ResourceKey: key,
	})
}

func (r *Repository) CountResourceReference(ctx context.Context, reference ResourceReference) (int64, error) {
	return r.queries.CountResourceReference(ctx, dbgen.CountResourceReferenceParams{
		ResourceKind: reference.Kind,
		ResourceID:   reference.ID,
		HolderKind:   reference.HolderKind,
		HolderID:     reference.HolderID,
	})
}

func (r *Repository) TransferResourceReference(ctx context.Context, previous ResourceReference, holderKind, holderID string) error {
	return store.RequireOne(r.queries.TransferResourceReference(ctx, dbgen.TransferResourceReferenceParams{
		ResourceKind: previous.Kind,
		ResourceID:   previous.ID,
		PreviousKind: previous.HolderKind,
		PreviousID:   previous.HolderID,
		NextKind:     holderKind,
		NextID:       holderID,
	}))
}

func (r *Repository) MoveCostReference(ctx context.Context, previous CostReference, holderID string) error {
	return store.RequireOne(r.queries.MoveCostReference(ctx, dbgen.MoveCostReferenceParams{
		NextID:      holderID,
		TenantID:    previous.TenantID,
		CostKind:    previous.Kind,
		ResourceKey: previous.Key,
		HolderKind:  previous.HolderKind,
		PreviousID:  previous.HolderID,
	}))
}

func (r *Repository) TransferCostReference(ctx context.Context, previous CostReference, holderKind, holderID string) error {
	return store.RequireOne(r.queries.TransferCostReference(ctx, dbgen.TransferCostReferenceParams{
		TenantID:     previous.TenantID,
		CostKind:     previous.Kind,
		ResourceKey:  previous.Key,
		PreviousKind: previous.HolderKind,
		PreviousID:   previous.HolderID,
		NextKind:     holderKind,
		NextID:       holderID,
	}))
}

func (r *Repository) AddResourceReference(ctx context.Context, reference ResourceReference) error {
	return store.RequireOne(r.queries.InsertResourceReference(ctx, dbgen.InsertResourceReferenceParams{
		ResourceKind: reference.Kind,
		ResourceID:   reference.ID,
		HolderKind:   reference.HolderKind,
		HolderID:     reference.HolderID,
	}))
}

func (r *Repository) RemoveResourceReference(ctx context.Context, reference ResourceReference) error {
	return store.RequireOne(r.queries.DeleteResourceReference(ctx, dbgen.DeleteResourceReferenceParams{
		ResourceKind: reference.Kind,
		ResourceID:   reference.ID,
		HolderKind:   reference.HolderKind,
		HolderID:     reference.HolderID,
	}))
}

func (r *Repository) AddCostReference(ctx context.Context, reference CostReference) error {
	return store.RequireOne(r.queries.InsertCostReference(ctx, dbgen.InsertCostReferenceParams{
		TenantID:    reference.TenantID,
		Kind:        reference.Kind,
		ResourceKey: reference.Key,
		HolderKind:  reference.HolderKind,
		HolderID:    reference.HolderID,
	}))
}

func (r *Repository) RemoveCostReference(ctx context.Context, reference CostReference) error {
	return store.RequireOne(r.queries.DeleteCostReference(ctx, dbgen.DeleteCostReferenceParams{
		TenantID:    reference.TenantID,
		Kind:        reference.Kind,
		ResourceKey: reference.Key,
		HolderKind:  reference.HolderKind,
		HolderID:    reference.HolderID,
	}))
}
