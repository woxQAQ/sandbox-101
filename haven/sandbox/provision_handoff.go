package sandbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/ownership"
)

// ProvisionSlotClaim binds the current sandbox and restore source to the
// original funded slot holder. It is verified in the allocation transaction.
type ProvisionSlotClaim struct {
	Sandbox ownership.Sandbox
	Source  machine.SnapshotSource
	Holder  string
}

type ProvisionTargetValidator interface {
	ValidateProvisionTarget(context.Context, *sql.Tx, ProvisionSlotClaim) (*machine.QuotaDestination, error)
}

func (parent *LifecycleAdmission) ValidateProvisionTarget(_ context.Context, _ *sql.Tx, claim ProvisionSlotClaim) (*machine.QuotaDestination, error) {
	record, source, holder := claim.Sandbox, claim.Source, claim.Holder
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
