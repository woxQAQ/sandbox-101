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

func (reference ResourceReference) params() dbgen.InsertResourceReferenceParams {
	return dbgen.InsertResourceReferenceParams{
		ResourceKind: reference.Kind,
		ResourceID:   reference.ID,
		HolderKind:   reference.HolderKind,
		HolderID:     reference.HolderID,
	}
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

func (reference CostReference) params() dbgen.InsertCostReferenceParams {
	return dbgen.InsertCostReferenceParams{
		TenantID:    reference.TenantID,
		Kind:        reference.Kind,
		ResourceKey: reference.Key,
		HolderKind:  reference.HolderKind,
		HolderID:    reference.HolderID,
	}
}

// CountStorageCostReference checks the original storage charge and its holder.
// Slot units are not part of this historical disk proof.
func CountStorageCostReference(ctx context.Context, db store.DBTX, cost CostIdentity, storageMiB int64, holder Holder) (int64, error) {
	return dbgen.New(db).CountStorageCostReference(ctx, dbgen.CountStorageCostReferenceParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		StorageMib:        storageMiB,
		SourceOperationID: cost.SourceOperation,
		HolderKind:        holder.Kind,
		HolderID:          holder.ID,
	})
}

func CountTenantCostHolders(ctx context.Context, db store.DBTX, tenant, kind, key string) (int64, error) {
	return dbgen.New(db).CountTenantCostHolders(ctx, dbgen.CountTenantCostHoldersParams{
		TenantID:    tenant,
		Kind:        kind,
		ResourceKey: key,
	})
}

func CountCostReference(ctx context.Context, db store.DBTX, reference CostReference) (int64, error) {
	return dbgen.New(db).CountCostReference(ctx, dbgen.CountCostReferenceParams(reference.params()))
}

func ResourceHolderIDs(ctx context.Context, db store.DBTX, kind, id, holderKind string) ([]string, error) {
	return dbgen.New(db).ResourceHolderIDs(ctx, dbgen.ResourceHolderIDsParams{
		ResourceKind: kind,
		ResourceID:   id,
		HolderKind:   holderKind,
	})
}

func CountResourceHolders(ctx context.Context, db store.DBTX, kind, id string) (int64, error) {
	return dbgen.New(db).CountResourceHolders(ctx, dbgen.CountResourceHoldersParams{
		ResourceKind: kind,
		ResourceID:   id,
	})
}

// CountCostHolders includes every tenant's reference to the resource key.
func CountCostHolders(ctx context.Context, db store.DBTX, kind, key string) (int64, error) {
	return dbgen.New(db).CountCostHolders(ctx, dbgen.CountCostHoldersParams{
		Kind:        kind,
		ResourceKey: key,
	})
}

func CountResourceReference(ctx context.Context, db store.DBTX, reference ResourceReference) (int64, error) {
	return dbgen.New(db).CountResourceReference(ctx, dbgen.CountResourceReferenceParams(reference.params()))
}

func TransferResourceReference(ctx context.Context, db store.DBTX, previous ResourceReference, holderKind, holderID string) error {
	return store.RequireOne(dbgen.New(db).TransferResourceReference(ctx, dbgen.TransferResourceReferenceParams{
		ResourceKind: previous.Kind,
		ResourceID:   previous.ID,
		PreviousKind: previous.HolderKind,
		PreviousID:   previous.HolderID,
		NextKind:     holderKind,
		NextID:       holderID,
	}))
}

func MoveCostReference(ctx context.Context, db store.DBTX, previous CostReference, holderID string) error {
	return store.RequireOne(dbgen.New(db).MoveCostReference(ctx, dbgen.MoveCostReferenceParams{
		NextID:      holderID,
		TenantID:    previous.TenantID,
		CostKind:    previous.Kind,
		ResourceKey: previous.Key,
		HolderKind:  previous.HolderKind,
		PreviousID:  previous.HolderID,
	}))
}

func TransferCostReference(ctx context.Context, db store.DBTX, previous CostReference, holderKind, holderID string) error {
	return store.RequireOne(dbgen.New(db).TransferCostReference(ctx, dbgen.TransferCostReferenceParams{
		TenantID:     previous.TenantID,
		CostKind:     previous.Kind,
		ResourceKey:  previous.Key,
		PreviousKind: previous.HolderKind,
		PreviousID:   previous.HolderID,
		NextKind:     holderKind,
		NextID:       holderID,
	}))
}

func AddResourceReference(ctx context.Context, db store.DBTX, reference ResourceReference) error {
	return store.RequireOne(dbgen.New(db).InsertResourceReference(ctx, reference.params()))
}

func RemoveResourceReference(ctx context.Context, db store.DBTX, reference ResourceReference) error {
	return store.RequireOne(dbgen.New(db).DeleteResourceReference(ctx, dbgen.DeleteResourceReferenceParams(reference.params())))
}

func AddCostReference(ctx context.Context, db store.DBTX, reference CostReference) error {
	return store.RequireOne(dbgen.New(db).InsertCostReference(ctx, reference.params()))
}

func RemoveCostReference(ctx context.Context, db store.DBTX, reference CostReference) error {
	return store.RequireOne(dbgen.New(db).DeleteCostReference(ctx, dbgen.DeleteCostReferenceParams(reference.params())))
}
