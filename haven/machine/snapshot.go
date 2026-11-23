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
	queries := dbgen.New(tx)
	if err := store.RequireOne(queries.InsertSnapshot(ctx, dbgen.InsertSnapshotParams{
		ID:                 record.ID,
		SourceVmID:         record.SourceVM,
		TenantID:           record.TenantID,
		CaptureOperationID: record.OperationID,
		SourceInstance:     record.SourceInstance,
		Directory:          record.Dir,
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

func captureManifest(step CaptureStep) Manifest {
	return Manifest{
		Format:          1,
		Backend:         step.Input.Backend,
		Architecture:    step.Input.Architecture,
		CPU:             step.Input.CPU,
		Config:          step.Input.Source.Input.Resource.Config,
		MemoryMode:      "CopyOnWrite",
		PublicationMode: "IndependentInodes",
		Files:           step.State.Files,
		Image:           operation.Encode(step.Input.Source.Input.Image),
	}
}

func manifestHash(m *Manifest) sql.NullString {
	if m == nil {
		return sql.NullString{}
	}
	return sql.NullString{
		String: operation.Digest(operation.Encode(m)),
		Valid:  true,
	}
}

// CapturePublication reads the original capture independently of the current snapshot catalog.
func CapturePublication(ctx context.Context, q store.DBTX, id string) (SnapshotSource, error) {
	step, err := LoadCapture(ctx, q, id)
	if err != nil {
		return SnapshotSource{}, err
	}
	if step.Status != "succeeded" || step.State.Phase != "complete" || step.State.Boundary == nil {
		return SnapshotSource{}, fmt.Errorf("capture %s lacks a completed publication", id)
	}
	manifest := captureManifest(step)
	snapshot := step.Input.Snapshot
	snapshot.Manifest = &manifest
	return SnapshotSource{
		Snapshot:         snapshot,
		CaptureOperation: id,
		CaptureHash:      step.Hash,
		Publication:      step.State.Boundary.Publication,
	}, nil
}

func snapshotRecord(ctx context.Context, q store.DBTX, row dbgen.Snapshot) (SnapshotRecord, error) {
	record := SnapshotRecord{
		ID:             row.ID,
		TenantID:       row.TenantID,
		OperationID:    row.CaptureOperationID,
		SourceVM:       row.SourceVmID,
		SourceInstance: row.SourceInstance,
		Dir:            row.Directory,
	}
	if row.ManifestHash.Valid {
		source, err := CapturePublication(ctx, q, row.CaptureOperationID)
		if err != nil {
			return record, err
		}
		record.Manifest = source.Snapshot.Manifest
		if !reflect.DeepEqual(record, source.Snapshot) || manifestHash(record.Manifest) != row.ManifestHash {
			return record, fmt.Errorf("snapshot manifest differs from its capture")
		}
	}
	return record, nil
}

func LoadOwnedSnapshot(ctx context.Context, query store.DBTX, tenant, id string) (SnapshotRecord, error) {
	var record SnapshotRecord
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		row, err := dbgen.New(tx).GetOwnedSnapshot(ctx, dbgen.GetOwnedSnapshotParams{
			ID:       id,
			TenantID: tenant,
		})
		if err != nil {
			return err
		}
		record, err = snapshotRecord(ctx, tx, row)
		if err != nil {
			return err
		}
		if record.ID != id || record.TenantID != tenant {
			return fmt.Errorf("snapshot provenance inconsistent")
		}
		return nil
	})
	return record, err
}

func SnapshotRoots(ctx context.Context, query store.DBTX) ([]SnapshotRoot, error) {
	var roots []SnapshotRoot
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		rows, err := dbgen.New(tx).ListSnapshots(ctx)
		if err != nil {
			return err
		}
		for _, row := range rows {
			record, err := snapshotRecord(ctx, tx, row)
			if err != nil {
				return err
			}
			roots = append(roots, SnapshotRoot{
				ID:        row.ID,
				Record:    record,
				Hash:      operation.Digest(operation.Encode(record)),
				Published: row.Published == 1,
				Deleting:  row.Deleting != 0,
			})
		}
		return nil
	})
	return roots, err
}

func SnapshotReaders(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountSnapshotLeases(ctx, id)
}

// Backings enumerates explicit retained users. No name or current VM query
// substitutes for their immutable material and lease handoff proof.
func (proofs SnapshotProofs) Backings(ctx context.Context, tx *sql.Tx, source SnapshotSource) ([]ResourceStep, error) {
	holders, err := quota.ResourceHolderIDs(ctx, tx, "snapshot", source.Snapshot.ID, "backing")
	if err != nil {
		return nil, err
	}
	var result []ResourceStep
	for _, holder := range holders {
		id, err := dbgen.New(tx).RestoreBackingOperation(ctx, dbgen.RestoreBackingOperationParams{
			BackingHolder: sql.NullString{
				String: holder,
				Valid:  true,
			},
			RestoreSnapshotID: sql.NullString{
				String: source.Snapshot.ID,
				Valid:  true,
			},
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
	if err := quota.AddResourceReference(ctx, tx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         lease.SnapshotID,
		HolderKind: "step",
		HolderID:   holder,
	}); err != nil {
		return err
	}
	return quota.AddCostReference(ctx, tx, quota.CostReference{
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
	if err := quota.RemoveResourceReference(ctx, tx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         lease.SnapshotID,
		HolderKind: "step",
		HolderID:   holder,
	}); err != nil {
		return err
	}
	return quota.RemoveCostReference(ctx, tx, quota.CostReference{
		TenantID:   tenant,
		Kind:       "snapshot",
		Key:        lease.SnapshotID,
		HolderKind: "engine_dependency",
		HolderID:   holder,
	})
}
