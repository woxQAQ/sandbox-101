package sandbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/ownership"
)

type ProvisionTargetValidator interface {
	ValidateProvisionTarget(context.Context, *sql.Tx, ownership.Sandbox, machine.Source, string) (*machine.QuotaDestination, error)
}

func (parent *LifecycleAdmission) ValidateProvisionTarget(_ context.Context, _ *sql.Tx, record ownership.Sandbox, source machine.Source, holder string) (*machine.QuotaDestination, error) {
	if parent == nil || parent.Input.Command.Action != "resume" || record.State != "paused" || record.VMID != "" || record.Instance != "" || record.SnapshotID != source.Snapshot.ID || holder != "sleep:"+record.ID {
		return nil, fmt.Errorf("%w: resume target differs from frozen sleep responsibility", errs.ErrConflict)
	}
	return &machine.QuotaDestination{
		OperationID: parent.Parent.ID,
		Kind:        "sleep",
		ID:          record.ID,
		SnapshotID:  record.SnapshotID,
	}, nil
}
