package sandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// Checkpoint is a tenant-owned business binding to a retained snapshot. Its
// identity is independent of the sandbox's current VM generation.
type Checkpoint struct {
	TemplateID string     `json:"template_id,omitempty"`
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	SandboxID  string     `json:"sandbox_id"`
	SnapshotID string     `json:"snapshot_id"`
	Shape      loom.Shape `json:"shape"`
	CreatedAt  time.Time  `json:"created_at"`
}

func (checkpoint Checkpoint) Public() types.Checkpoint {
	return types.Checkpoint{
		ID:         checkpoint.ID,
		TenantID:   checkpoint.TenantID,
		SandboxID:  checkpoint.SandboxID,
		SnapshotID: checkpoint.SnapshotID,
		Shape:      types.Shape(checkpoint.Shape),
		CreatedAt:  checkpoint.CreatedAt,
	}
}

func bindCheckpoint(ctx context.Context, query store.DBTX, checkpoint Checkpoint, operationID string) error {
	body, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(query).InsertCheckpoint(ctx, dbgen.InsertCheckpointParams{
		ID:          checkpoint.ID,
		Tenant:      checkpoint.TenantID,
		SnapshotID:  checkpoint.SnapshotID,
		OperationID: operationID,
		Body:        body,
	}))
}

func unbindCheckpoint(ctx context.Context, query store.DBTX, checkpoint Checkpoint) error {
	body, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(query).DeleteCheckpoint(ctx, dbgen.DeleteCheckpointParams{
		ID:         checkpoint.ID,
		Tenant:     checkpoint.TenantID,
		SnapshotID: checkpoint.SnapshotID,
		Body:       body,
	}))
}

func LoadCheckpoint(ctx context.Context, query store.DBTX, id string) (Checkpoint, error) {
	var checkpoint Checkpoint
	body, err := dbgen.New(query).GetCheckpoint(ctx, id)
	if err != nil {
		return checkpoint, err
	}
	err = json.Unmarshal(body, &checkpoint)
	return checkpoint, err
}

func LoadOwnedCheckpoint(ctx context.Context, query store.DBTX, id, tenant string) (Checkpoint, error) {
	var checkpoint Checkpoint
	body, err := dbgen.New(query).GetOwnedCheckpoint(ctx, dbgen.GetOwnedCheckpointParams{
		ID:     id,
		Tenant: tenant,
	})
	if err != nil {
		return checkpoint, err
	}
	if err = json.Unmarshal(body, &checkpoint); err != nil {
		return checkpoint, err
	}
	if checkpoint.ID != id || checkpoint.TenantID != tenant {
		return checkpoint, fmt.Errorf("%w: checkpoint binding identity changed", errs.ErrManualRecovery)
	}
	return checkpoint, nil
}

func CheckpointInventory(ctx context.Context, query store.DBTX) ([]Checkpoint, error) {
	bodies, err := dbgen.New(query).ListCheckpoints(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Checkpoint, 0, len(bodies))
	for _, body := range bodies {
		var checkpoint Checkpoint
		if err = json.Unmarshal(body, &checkpoint); err != nil {
			return out, err
		}
		out = append(out, checkpoint)
	}
	return out, nil
}
