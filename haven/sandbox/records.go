package sandbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func optionalID(id string) sql.NullString {
	return sql.NullString{
		String: id,
		Valid:  id != "",
	}
}

func sandboxRow(record ownership.Sandbox) dbgen.Sandbox {
	birth := record.Identity
	row := dbgen.Sandbox{
		ID:                record.ID,
		Tenant:            record.TenantID,
		State:             record.State,
		TemplateID:        birth.TemplateID,
		CreationEpoch:     birth.Epoch,
		CreatedAt:         birth.CreatedAt.Format(time.RFC3339Nano),
		ProvisionSource:   birth.ProvisionSource,
		Vcpus:             int64(birth.Shape.VCPUs),
		MemoryMib:         birth.Shape.MemoryMiB,
		DiskMib:           birth.Shape.DiskMiB,
		OperationID:       optionalID(record.OperationID),
		SourceOperationID: optionalID(record.SourceOperation),
		LastError:         record.LastError,
		VmID:              record.VMID,
		Instance:          record.Instance,
		SnapshotID:        record.SnapshotID,
	}
	if ref := record.SlotAssignment; ref != nil {
		row.SlotOperationID = optionalID(ref.OperationID)
		row.SlotInputHash = sql.NullString{
			String: ref.InputHash,
			Valid:  true,
		}
	}
	return row
}

func LoadSandbox(ctx context.Context, query store.DBTX, id, tenant string) (ownership.Sandbox, error) {
	row, err := dbgen.New(query).GetSandbox(ctx, dbgen.GetSandboxParams{
		ID:     id,
		Tenant: tenant,
	})
	if err != nil {
		return ownership.Sandbox{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, row.CreatedAt)
	if err != nil {
		return ownership.Sandbox{}, err
	}
	record := ownership.Sandbox{
		ID:              row.ID,
		TenantID:        row.Tenant,
		State:           row.State,
		LastError:       row.LastError,
		OperationID:     row.OperationID.String,
		SourceOperation: row.SourceOperationID.String,
		VMID:            row.VmID,
		Instance:        row.Instance,
		SnapshotID:      row.SnapshotID,
		Identity: ownership.Birth{
			TemplateID:      row.TemplateID,
			Epoch:           row.CreationEpoch,
			CreatedAt:       createdAt,
			ProvisionSource: row.ProvisionSource,
			Shape: loom.Shape{
				VCPUs:     int(row.Vcpus),
				MemoryMiB: row.MemoryMib,
				DiskMiB:   row.DiskMib,
			},
		},
	}
	if row.SlotOperationID.Valid {
		record.SlotAssignment = &ownership.GenerationRef{
			OperationID: row.SlotOperationID.String,
			InputHash:   row.SlotInputHash.String,
		}
	}
	return record, nil
}

func insertSandbox(ctx context.Context, tx *sql.Tx, record ownership.Sandbox) error {
	return store.RequireOne(dbgen.New(tx).InsertSandbox(ctx, dbgen.InsertSandboxParams(sandboxRow(record))))
}

// replaceSandbox compares the complete prior row in the same write transaction.
// Lifecycle compensation may supply a historical value as the replacement.
func replaceSandbox(ctx context.Context, tx *sql.Tx, before, after ownership.Sandbox) error {
	queries := dbgen.New(tx)
	current, err := queries.GetSandbox(ctx, dbgen.GetSandboxParams{
		ID:     before.ID,
		Tenant: before.TenantID,
	})
	if err != nil {
		return err
	}
	if current != sandboxRow(before) || before.ID != after.ID || before.TenantID != after.TenantID {
		return fmt.Errorf("sandbox %s prior ownership changed", before.ID)
	}
	row := sandboxRow(after)
	return store.RequireOne(queries.ReplaceSandbox(ctx, dbgen.ReplaceSandboxParams{
		ID:                row.ID,
		Tenant:            row.Tenant,
		State:             row.State,
		TemplateID:        row.TemplateID,
		Vcpus:             row.Vcpus,
		MemoryMib:         row.MemoryMib,
		DiskMib:           row.DiskMib,
		CreationEpoch:     row.CreationEpoch,
		CreatedAt:         row.CreatedAt,
		ProvisionSource:   row.ProvisionSource,
		LastError:         row.LastError,
		OperationID:       row.OperationID,
		VmID:              row.VmID,
		Instance:          row.Instance,
		SnapshotID:        row.SnapshotID,
		SourceOperationID: row.SourceOperationID,
		SlotOperationID:   row.SlotOperationID,
		SlotInputHash:     row.SlotInputHash,
		PreviousState:     current.State,
		PreviousInstance:  current.Instance,
		PreviousSource:    current.SourceOperationID,
		PreviousOperation: current.OperationID,
	}))
}
