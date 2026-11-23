package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// Record is the persisted candidate and its observed readiness. It supplies no
// substitute for an independently retained instance/version ownership witness.
type VMRecord struct {
	Resource    loom.Resource     `json:"resource"`
	Observation *loom.Observation `json:"observation,omitempty"`
	Verified    bool              `json:"verified"`
	AgentReady  bool              `json:"agent_ready"`
	Deleting    bool              `json:"deleting"`
}

// VMRoot retains the catalog identity even when the serialized resource has
// changed. Inconsistent roots still protect their observed paths from collection.
type VMRoot struct {
	ID         string
	Directory  string
	Consistent bool
}

// VMRoots returns the decoded prefix with an error when later metadata is
// unreadable. The caller can preserve that prefix in its diagnostic inventory.
func VMRoots(ctx context.Context, query store.DBTX, directory string) ([]VMRoot, error) {
	rows, err := dbgen.New(query).ListCurrentBodies(ctx)
	if err != nil {
		return nil, err
	}
	var roots []VMRoot
	for _, row := range rows {
		roots = append(roots, VMRoot{
			ID:         row.ID,
			Directory:  row.Directory,
			Consistent: row.Directory == filepath.Join(directory, "vms", row.ID),
		})
	}
	return roots, nil
}

func CurrentVMCount(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountCurrent(ctx, id)
}

// RestoreGenerationClaim joins the stopped predecessor identity to the exact
// replacement record. The VM key and serialized body come from that same record.
type RestoreGenerationClaim struct {
	ExpectedInstance string
	OperationID      string
	VM               VMRecord
}

func ClaimRestoreGeneration(ctx context.Context, query store.DBTX, claim RestoreGenerationClaim) error {
	return store.RequireOne(dbgen.New(query).ClaimRestoreGeneration(ctx, dbgen.ClaimRestoreGenerationParams{
		ID:       claim.VM.Resource.VMID,
		Instance: claim.ExpectedInstance,
		Owner: sql.NullString{
			String: claim.OperationID,
			Valid:  true,
		},
		GenerationOperationID: claim.OperationID,
		Directory:             claim.VM.Resource.Dir,
		ResourceHash:          operation.Digest(operation.Encode(claim.VM.Resource)),
	}))
}

// CurrentOwnership separates the mutable VM row's ownership from its persisted body.
// The body is decoded by the workflow that verifies the frozen resource input;
// this row never supplies a missing historical witness.
type CurrentOwnership struct {
	Instance string
	State    string
	Owner    string
	Owned    bool // Preserves SQL NULL independently of the owner string.
	Record   VMRecord
}

// Repository reads current ownership and retained generation facts through the
// caller's database or transaction. Generated SQL types stay inside this package.

func LoadOwnership(ctx context.Context, query store.DBTX, id string) (CurrentOwnership, error) {
	var result CurrentOwnership
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		var err error
		result, err = readOwnership(ctx, tx, id)
		return err
	})
	return result, err
}

func readOwnership(ctx context.Context, query *sql.Tx, id string) (CurrentOwnership, error) {
	row, err := dbgen.New(query).GetCurrent(ctx, id)
	if err != nil {
		return CurrentOwnership{}, err
	}
	vm := row.Vm
	var input ResourceInput
	if !row.InputHash.Valid || operation.Digest(row.Input) != row.InputHash.String {
		return CurrentOwnership{}, fmt.Errorf("VM generation input missing or changed")
	}
	if err = json.Unmarshal(row.Input, &input); err != nil {
		return CurrentOwnership{}, err
	}
	if input.Resource.VMID != id || input.Resource.Dir != vm.Directory || int64(input.Resource.Slot) != vm.Slot || operation.Digest(operation.Encode(input.Resource)) != vm.ResourceHash {
		return CurrentOwnership{}, fmt.Errorf("VM generation allocation changed")
	}
	record := VMRecord{
		Resource:   input.Resource,
		Verified:   vm.Verified == 1,
		AgentReady: vm.AgentReady == 1,
		Deleting:   vm.Deleting == 1,
	}
	if vm.ObservationHash.Valid {
		progress, err := dbgen.New(query).GetResourceProgress(ctx, vm.GenerationOperationID)
		if err != nil {
			return CurrentOwnership{}, fmt.Errorf("VM launch history unavailable: %v", err)
		}
		record.Observation = launchObservation(progress)
		if observationHash(record.Observation) != vm.ObservationHash {
			return CurrentOwnership{}, fmt.Errorf("VM launch observation changed")
		}
	}
	return CurrentOwnership{
		Instance: vm.CurrentInstance,
		State:    vm.State,
		Owner:    vm.OperationID.String,
		Owned:    vm.OperationID.Valid,
		Record:   record,
	}, nil
}

func observationHash(observation *loom.Observation) sql.NullString {
	if observation == nil {
		return sql.NullString{}
	}
	return sql.NullString{
		String: operation.Digest(operation.Encode(observation)),
		Valid:  true,
	}
}

// VerifyInstance binds the original allocation and its published launch observation to their retained hashes.
func VerifyInstance(ctx context.Context, query store.DBTX, operationID string, resource loom.Resource, observation *loom.Observation) error {
	row, err := dbgen.New(query).GetInstance(ctx, dbgen.GetInstanceParams{
		ID:          resource.Instance,
		VmID:        resource.VMID,
		OperationID: operationID,
	})
	if err != nil {
		return err
	}
	if row.ResourceHash != operation.Digest(operation.Encode(resource)) || row.ObservationHash != observationHash(observation) {
		return fmt.Errorf("retained instance birth proof changed")
	}
	return nil
}

// VerifyVersion checks the independently retained binding against the frozen step and disk witness.
func VerifyVersion(ctx context.Context, query store.DBTX, step ResourceStep) error {
	row, err := dbgen.New(query).GetVersion(ctx, dbgen.GetVersionParams{
		VmID:        step.Candidate,
		OperationID: step.OperationID,
		StepKey:     step.Key,
	})
	if err != nil {
		return err
	}
	resource := step.Input.Resource
	if row.Instance != resource.Instance || row.ResourceHash != operation.Digest(operation.Encode(resource)) {
		return fmt.Errorf("retained resource generation identity changed")
	}
	device, inode, mode := diskColumns(step.State.DiskIdentity)
	if row.DiskDevice != device || row.DiskInode != inode || row.DiskMode != mode {
		return fmt.Errorf("retained generation disk identity changed")
	}
	return nil
}

// Linux dev_t and ino_t retain all 64 bits in SQLite's signed INTEGER storage.
func diskColumns(identity *loom.FileIdentity) (device, inode, mode sql.NullInt64) {
	if identity != nil {
		device = sql.NullInt64{
			Int64: int64(identity.Device),
			Valid: true,
		}
		inode = sql.NullInt64{
			Int64: int64(identity.Inode),
			Valid: true,
		}
		mode = sql.NullInt64{
			Int64: int64(identity.Mode),
			Valid: true,
		}
	}
	return
}

func recordVersionDisk(ctx context.Context, tx *sql.Tx, step *ResourceStep, disk *loom.FileIdentity) error {
	if disk == nil || step.State.DiskIdentity != nil {
		return fmt.Errorf("disk publication requires a new generation witness")
	}
	device, inode, mode := diskColumns(disk)
	return store.RequireOne(dbgen.New(tx).RecordVersionDisk(ctx, dbgen.RecordVersionDiskParams{
		VmID:         step.Candidate,
		OperationID:  step.OperationID,
		Instance:     step.Input.Resource.Instance,
		ResourceHash: operation.Digest(operation.Encode(step.Input.Resource)),
		DiskDevice:   device,
		DiskInode:    inode,
		DiskMode:     mode,
	}))
}

func VersionRoots(ctx context.Context, query store.DBTX) ([]string, error) {
	return dbgen.New(query).ListVersionIDs(ctx)
}

func RetainedInstanceCount(ctx context.Context, query store.DBTX, vmID string) (int64, error) {
	return dbgen.New(query).CountInstances(ctx, vmID)
}

func RetainedVersionCount(ctx context.Context, query store.DBTX, vmID string) (int64, error) {
	return dbgen.New(query).CountVersions(ctx, vmID)
}
