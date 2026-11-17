package sandbox

import (
	"database/sql"
	"errors"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func (service *Lifecycle) runReleaseCaptures(step LifecycleStep) error {
	stopped, err := service.runResourceLink(step.OperationID, "stop")
	if err != nil {
		return err
	}
	for _, ref := range step.Input.CaptureFailures {
		role := "failed-capture:" + ref.OperationID
		if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, role); errors.Is(lookup, sql.ErrNoRows) {
			link := &LifecycleAdmission{
				History: service.proofs.Snapshots(),
				Parent: operation.Record{
					ID: step.OperationID,
				},
				Input: step.Input,
				Role:  role,
			}
			discards := service.captureDiscard
			_, err = discards.AdmitForParent(service.ctx, machine.CaptureDiscardRequest{
				TenantID:          step.Input.OwnerTenantID,
				Key:               step.OperationID + ":" + role,
				SandboxID:         step.Input.Sandbox.ID,
				FailedOperationID: ref.OperationID,
				ExpectedInstance:  step.Input.SourceInstance,
				StopOperationID:   stopped.ChildID,
			}, link)
			if err != nil {
				return err
			}
		} else if lookup != nil {
			return lookup
		}
		if _, err = service.runResourceLink(step.OperationID, role); err != nil {
			return err
		}
	}
	if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, "release"); errors.Is(lookup, sql.ErrNoRows) {
		link := &LifecycleAdmission{
			History: service.proofs.Snapshots(),
			Parent: operation.Record{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "release",
		}
		if _, err = service.deletion.AdmitReleaseForParent(service.ctx, machine.ReleaseRequest{
			TenantID:  step.Input.OwnerTenantID,
			Key:       step.OperationID + ":release",
			SandboxID: step.Input.Sandbox.ID,
		}, link); err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	return nil
}
