package sandbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/ownership"
)

func (admission *WarmClaimAdmission) ReleaseDestination(_ context.Context, _ *sql.Tx, record ownership.Sandbox, _ machine.ResourceStep) (*machine.QuotaDestination, bool, error) {
	if admission == nil {
		return nil, false, nil
	}
	if admission.Role != "warm_release" {
		return nil, false, fmt.Errorf("%w: warm claim release role invalid", errs.ErrInvalid)
	}
	return &machine.QuotaDestination{
		OperationID: admission.ParentID,
		Kind:        "claim",
		ID:          record.ID,
		SnapshotID:  admission.Input.Pool.Template.SnapshotID,
	}, true, nil
}

func (admission *LifecycleAdmission) ReleaseDestination(ctx context.Context, tx *sql.Tx, record ownership.Sandbox, source machine.ResourceStep) (*machine.QuotaDestination, bool, error) {
	if admission == nil {
		return nil, false, nil
	}
	if admission.Input.Command.Action == "pause" {
		parent, err := LoadLifecycle(ctx, tx, admission.Parent.ID)
		if err != nil {
			return nil, false, err
		}
		if parent.State.Phase != "captured" || parent.State.SnapshotID == "" {
			return nil, false, fmt.Errorf("%w: pause source release lacks published snapshot", errs.ErrConflict)
		}
		return &machine.QuotaDestination{
			OperationID: admission.Parent.ID,
			Kind:        "sleep",
			ID:          record.ID,
			SnapshotID:  parent.State.SnapshotID,
		}, true, nil
	}
	if admission.Input.Command.Action == "resume" && admission.Role == "compensate" {
		activation, err := LoadActivation(ctx, tx, admission.Parent.ID)
		if err != nil {
			return nil, false, err
		}
		if activation.Status != "failed" || activation.Input.ResourceOperation != source.OperationID || activation.Input.ExpectedInstance != source.State.Witness {
			return nil, false, fmt.Errorf("%w: resume compensation lacks failed delivery proof", errs.ErrConflict)
		}
		return &machine.QuotaDestination{
			OperationID: admission.Parent.ID,
			Kind:        "sleep",
			ID:          record.ID,
			SnapshotID:  admission.Input.Sandbox.SnapshotID,
		}, true, nil
	}
	return nil, false, nil
}
