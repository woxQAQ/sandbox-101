package machine

import (
	"context"
	"database/sql"
	"encoding/json"
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

// RetainedVersion retains the admitted resource and disk identity for one generation.
type RetainedVersion struct {
	Resource     loom.Resource      `json:"resource"`
	DiskIdentity *loom.FileIdentity `json:"disk_identity,omitempty"`
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
		var record VMRecord
		if err := json.Unmarshal(row.Body, &record); err != nil {
			return roots, err
		}
		roots = append(roots, VMRoot{
			ID:         row.ID,
			Directory:  record.Resource.Dir,
			Consistent: record.Resource.VMID == row.ID && record.Resource.Dir == filepath.Join(directory, "vms", row.ID),
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
	return store.RequireOne(dbgen.New(query).ClaimStoppedBody(ctx, dbgen.ClaimStoppedBodyParams{
		ID:       claim.VM.Resource.VMID,
		Instance: claim.ExpectedInstance,
		Owner: sql.NullString{
			String: claim.OperationID,
			Valid:  true,
		},
		Body: operation.Encode(claim.VM),
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
	Body     []byte
}

// Repository reads current ownership and retained generation facts through the
// caller's database or transaction. Generated SQL types stay inside this package.

func LoadOwnership(ctx context.Context, query store.DBTX, id string) (CurrentOwnership, error) {
	row, err := dbgen.New(query).GetCurrent(ctx, id)
	if err != nil {
		return CurrentOwnership{}, err
	}
	return CurrentOwnership{
		Instance: row.CurrentInstance,
		State:    row.State,
		Owner:    row.OperationID.String,
		Owned:    row.OperationID.Valid,
		Body:     row.Body,
	}, nil
}

// GenerationIdentity identifies a retained birth independently of current VM
// ownership. Instance witnesses and version step keys remain explicit inputs.
type GenerationIdentity struct {
	VMID        string
	OperationID string
}

// InstanceHistory requires the original instance, VM and operation identities together.
func InstanceHistory(ctx context.Context, query store.DBTX, generation GenerationIdentity, instance string) ([]byte, error) {
	return dbgen.New(query).GetInstance(ctx, dbgen.GetInstanceParams{
		ID:          instance,
		VmID:        generation.VMID,
		OperationID: generation.OperationID,
	})
}

// VersionHistory reads the retained version named by the original operation and step.
// Missing history remains an error; the current VM row is never substituted.
func VersionHistory(ctx context.Context, query store.DBTX, generation GenerationIdentity, stepKey string) ([]byte, error) {
	return dbgen.New(query).GetVersion(ctx, dbgen.GetVersionParams{
		ID:          generation.OperationID,
		VmID:        generation.VMID,
		OperationID: generation.OperationID,
		StepKey:     stepKey,
	})
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
