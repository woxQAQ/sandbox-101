package gc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/gc/internal/dbgen"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/supply"
)

func (c *Collector) internalSnapshot(ctx context.Context, source machine.SnapshotSource) (bool, bool, error) {
	entries, err := dbgen.New(c.store.Database).ListCaptureLinks(ctx, source.CaptureOperation)
	if err != nil {
		return false, false, err
	}
	if len(entries) == 0 {
		return false, false, nil
	}
	if len(entries) != 1 {
		return false, false, fmt.Errorf("snapshot has ambiguous control publication")
	}
	parentID := entries[0]
	link, status, err := machine.LoadResourceLink(ctx, c.store.Database, parentID, "capture")
	if err != nil {
		return false, false, err
	}
	if link.ChildID != source.CaptureOperation || link.Kind != "capture" || link.Candidate != source.Snapshot.ID {
		return false, false, fmt.Errorf("snapshot control link identity changed")
	}
	if lifecycle, err := sandbox.LoadLifecycleStep(ctx, c.store.Database, parentID); err == nil {
		if lifecycle.Input.OwnerTenantID != source.Snapshot.TenantID || (lifecycle.Input.Command.Action != "pause" && lifecycle.Input.Command.Action != "checkpoint") {
			return false, false, fmt.Errorf("snapshot lifecycle publication scope changed")
		}
		return true, operation.Terminal(lifecycle.Status) && status == "succeeded", nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, false, err
	}
	supply, err := supply.LoadSupply(ctx, c.store.Database, parentID)
	if err != nil {
		return false, false, err
	}
	if supply.Input.Command.Action != "template_build" || supply.Input.Template.TenantID != source.Snapshot.TenantID {
		return false, false, fmt.Errorf("snapshot supply publication scope changed")
	}
	return true, operation.Terminal(supply.Status) && status == "succeeded", nil
}

func (c *Collector) snapshotCandidate(ctx context.Context, record machine.SnapshotRecord) (bool, string, error) {
	source, err := c.proofs.SnapshotPublication(ctx, c.store.Database, record.TenantID, record.ID)
	if err != nil {
		return false, "", err
	}
	internal, complete, err := c.internalSnapshot(ctx, source)
	if err != nil {
		return false, "", err
	}
	if !internal {
		return false, "", nil
	}
	if !complete {
		return false, "owning control workflow remains unresolved", nil
	}
	readers, err := machine.SnapshotReaders(ctx, c.store.Database, record.ID)
	if err != nil {
		return false, "", err
	}
	if readers != 0 {
		return false, "source lease retained", nil
	}
	readers, err = quota.ActiveSnapshotReferences(ctx, c.store.Database, record.ID)
	if err != nil {
		return false, "", err
	}
	if readers != 0 {
		return false, "active or unresolved snapshot use retained", nil
	}
	raw, err := dbgen.New(c.store.Database).GetCleanupKindStatus(ctx, dbgen.GetCleanupKindStatusParams{
		ID:           "capture:" + source.CaptureOperation,
		OperationID:  source.CaptureOperation,
		ResourceKind: "capture",
	})
	if err != nil {
		return false, "", err
	}
	if raw != "done" {
		return false, "raw capture cleanup not retired", nil
	}
	return true, "", nil
}
