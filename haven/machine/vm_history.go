package machine

import (
	"context"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

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
