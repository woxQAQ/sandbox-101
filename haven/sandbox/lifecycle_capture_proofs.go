package sandbox

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/machine"
)

func checkReleaseCaptures(ctx context.Context, tx *sql.Tx, input LifecycleInput) error {
	source, err := machine.LoadResourceStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return err
	}
	current, err := machine.FailedCaptureReferences(ctx, tx, source)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, input.CaptureFailures) {
		return fmt.Errorf("release failed capture responsibilities changed")
	}
	return nil
}

func checkRollbackCaptures(ctx context.Context, tx *sql.Tx, input LifecycleInput) error {
	source, err := machine.LoadResourceStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return err
	}
	refs, err := machine.PendingPublishedCaptures(ctx, tx, source)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(refs, input.Rollback.RawCaptures) {
		return fmt.Errorf("rollback original capture responsibilities changed")
	}
	for _, ref := range refs {
		capture, err := machine.LoadCapture(ctx, tx, ref.OperationID)
		if err != nil {
			return err
		}
		if err = machine.CaptureCleanupProof(ctx, tx, capture, source.Candidate, input.SourceInstance, "pending"); err != nil {
			return err
		}
	}
	return nil
}
