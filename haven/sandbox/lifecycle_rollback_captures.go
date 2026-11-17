package sandbox

import (
	"database/sql"
	"errors"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func (service *Lifecycle) cleanupRollbackCaptures(step LifecycleStep, stop string) error {
	for _, ref := range step.Input.Rollback.RawCaptures {
		role := "raw:" + ref.OperationID
		if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, role); errors.Is(lookup, sql.ErrNoRows) {
			link := &LifecycleAdmission{
				History: service.proofs.Snapshots(),
				Parent: operation.Record{
					ID: step.OperationID,
				},
				Input: step.Input,
				Role:  role,
			}
			if _, err := service.captureCleanup.Admit(service.ctx, machine.CaptureCleanupRequest{
				TenantID:           step.Input.OwnerTenantID,
				Key:                step.OperationID + ":" + role,
				CaptureOperationID: ref.OperationID,
				StopOperationID:    stop,
			}, link); err != nil {
				return err
			}
		} else if lookup != nil {
			return lookup
		}
		if _, err := service.runResourceLink(step.OperationID, role); err != nil {
			return err
		}
	}
	return nil
}
