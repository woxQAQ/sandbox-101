package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// Manifest preserves the frozen capture representation, including field order.
type Manifest struct {
	Format          int               `json:"format"`
	Backend         string            `json:"backend"`
	Architecture    string            `json:"architecture"`
	CPU             string            `json:"cpu"`
	Config          loom.Config       `json:"config"`
	MemoryMode      string            `json:"memory_mode"`
	PublicationMode string            `json:"publication_mode"`
	Files           map[string]string `json:"files"`
	Image           json.RawMessage   `json:"image"`
}

type SnapshotRecord struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	OperationID    string    `json:"operation_id"`
	SourceVM       string    `json:"source_vm"`
	SourceInstance string    `json:"source_instance"`
	Dir            string    `json:"dir"`
	Manifest       *Manifest `json:"manifest,omitempty"`
}

// SnapshotSource retains immutable provenance without importing a resource workflow.
// The application must verify the original capture and current responsibility.
type SnapshotSource struct {
	Snapshot         SnapshotRecord    `json:"snapshot"`
	CaptureOperation string            `json:"capture_operation"`
	CaptureHash      string            `json:"capture_hash"`
	Publication      loom.FileIdentity `json:"publication"`
}

// admitCapturePublication joins the catalog and permanent owner in the caller's transaction.
func admitCapturePublication(ctx context.Context, tx *sql.Tx, record SnapshotRecord) error {
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	queries := dbgen.New(tx)
	if err = store.RequireOne(queries.InsertSnapshot(ctx, dbgen.InsertSnapshotParams{
		ID:         record.ID,
		SourceVmID: record.SourceVM,
		Body:       body,
	})); err != nil {
		return err
	}
	return store.RequireOne(queries.InsertCaptureOwner(ctx, dbgen.InsertCaptureOwnerParams{
		SnapshotID:        record.ID,
		TenantID:          record.TenantID,
		SourceOperationID: record.OperationID,
	}))
}

// SnapshotRoot keeps the catalog key distinct from decoded ownership and hashes
// the original bytes. Collection eligibility still requires historical proof.
type SnapshotRoot struct {
	ID        string
	Record    SnapshotRecord
	Hash      string
	Published bool
	Deleting  bool
}

func LoadOwnedSnapshot(ctx context.Context, query store.DBTX, tenant, id string) (SnapshotRecord, error) {
	var record SnapshotRecord
	body, err := dbgen.New(query).GetOwnedBody(ctx, dbgen.GetOwnedBodyParams{
		ID:       id,
		TenantID: tenant,
	})
	if err != nil {
		return record, err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return record, err
	}
	if record.ID != id || record.TenantID != tenant {
		return record, fmt.Errorf("snapshot provenance inconsistent")
	}
	return record, nil
}

func SnapshotRoots(ctx context.Context, query store.DBTX) ([]SnapshotRoot, error) {
	rows, err := dbgen.New(query).ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	var roots []SnapshotRoot
	for _, row := range rows {
		var record SnapshotRecord
		if err := json.Unmarshal(row.Body, &record); err != nil {
			return roots, err
		}
		roots = append(roots, SnapshotRoot{
			ID:        row.ID,
			Record:    record,
			Hash:      operation.Digest(row.Body),
			Published: row.Published == 1,
			Deleting:  row.Deleting != 0,
		})
	}
	return roots, nil
}

func SnapshotReaders(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountSnapshotLeases(ctx, id)
}

// Backings enumerates explicit retained users. No name or current VM query
// substitutes for their immutable material and lease handoff proof.
func (proofs SnapshotProofs) Backings(ctx context.Context, tx *sql.Tx, source SnapshotSource) ([]ResourceStep, error) {
	holders, err := quota.New(tx).ResourceHolderIDs(ctx, "snapshot", source.Snapshot.ID, "backing")
	if err != nil {
		return nil, err
	}
	var result []ResourceStep
	for _, holder := range holders {
		id, err := dbgen.New(tx).RestoreBackingOperation(ctx, dbgen.RestoreBackingOperationParams{
			HolderID:   holder,
			SnapshotID: source.Snapshot.ID,
		})
		if err != nil {
			return nil, err
		}
		step, err := LoadResourceStep(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err = ValidateResourceStep(proofs.directory, step); err != nil {
			return nil, err
		}
		if step.Input.Restore == nil || !reflect.DeepEqual(*step.Input.Restore, source) || step.Input.BackingHolder != holder || !step.State.BackingReady {
			return nil, fmt.Errorf("snapshot retained backing history changed")
		}
		if err = proofs.RestoreRelations(ctx, tx, step); err != nil {
			return nil, err
		}
		result = append(result, step)
	}
	return result, nil
}

func BackingRoots(ctx context.Context, query store.DBTX) ([]string, error) {
	return dbgen.New(query).ListBackingPaths(ctx)
}

// SourceLease pins a source publication for one frozen operation step.
type SourceLease struct {
	SnapshotID  string
	OperationID string
	StepKey     string
}

func SourceLeases(ctx context.Context, query store.DBTX) ([]SourceLease, error) {
	rows, err := dbgen.New(query).ListSourceLeases(ctx)
	if err != nil {
		return nil, err
	}
	var leases []SourceLease
	for _, row := range rows {
		leases = append(leases, SourceLease{
			SnapshotID:  row.SnapshotID,
			OperationID: row.OperationID,
			StepKey:     row.StepKey,
		})
	}
	return leases, nil
}

// Source pinning follows the caller's frozen parent/source proof. These writes
// only join the lease and both responsibilities in that caller's transaction.
func PinSnapshotSource(ctx context.Context, tx *sql.Tx, tenant string, lease SourceLease) error {
	if err := store.RequireOne(dbgen.New(tx).InsertSourceLease(ctx, dbgen.InsertSourceLeaseParams{
		SnapshotID:  lease.SnapshotID,
		OperationID: lease.OperationID,
		StepKey:     lease.StepKey,
	})); err != nil {
		return err
	}
	holder := lease.OperationID + ":" + lease.StepKey
	responsibilities := quota.New(tx)
	if err := responsibilities.AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         lease.SnapshotID,
		HolderKind: "step",
		HolderID:   holder,
	}); err != nil {
		return err
	}
	return responsibilities.AddCostReference(ctx, quota.CostReference{
		TenantID:   tenant,
		Kind:       "snapshot",
		Key:        lease.SnapshotID,
		HolderKind: "engine_dependency",
		HolderID:   holder,
	})
}

// Release requires the caller's completed handoff or reclamation proof; absence
// of a lease is not accepted as evidence that another path discharged it.
func ReleaseSnapshotSource(ctx context.Context, tx *sql.Tx, tenant string, lease SourceLease) error {
	if err := store.RequireOne(dbgen.New(tx).DeleteSourceLease(ctx, dbgen.DeleteSourceLeaseParams{
		SnapshotID:  lease.SnapshotID,
		OperationID: lease.OperationID,
		StepKey:     lease.StepKey,
	})); err != nil {
		return err
	}
	holder := lease.OperationID + ":" + lease.StepKey
	responsibilities := quota.New(tx)
	if err := responsibilities.RemoveResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         lease.SnapshotID,
		HolderKind: "step",
		HolderID:   holder,
	}); err != nil {
		return err
	}
	return responsibilities.RemoveCostReference(ctx, quota.CostReference{
		TenantID:   tenant,
		Kind:       "snapshot",
		Key:        lease.SnapshotID,
		HolderKind: "engine_dependency",
		HolderID:   holder,
	})
}
