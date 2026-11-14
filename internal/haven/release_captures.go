package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

func failedCaptureReferences(ctx context.Context, q rowsQueryer, source resourceStep) ([]generationRef, error) {
	rows, err := q.QueryContext(ctx, "SELECT holder_id FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='capture' ORDER BY holder_id", source.Candidate)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return nil, err
	}
	var refs []generationRef
	for _, id := range ids {
		capture, err := loadCapture(ctx, q, id)
		if err != nil {
			return nil, err
		}
		if !terminal(capture.Status) || capture.Input.Source.OperationID != source.OperationID || capture.Input.Source.Hash != source.Hash || capture.Input.Request.ExpectedInstance != source.State.Witness {
			return nil, fmt.Errorf("release capture user lacks original terminal generation proof")
		}
		if capture.Status == "failed" {
			if capture.State.ResumeRequired || capture.State.Error == "" {
				return nil, fmt.Errorf("release capture failure has unresolved source resume")
			}
			refs = append(refs, generationRef{
				OperationID: id,
				InputHash:   capture.Hash,
			})
		}
	}
	return refs, nil
}

func (service *Service) runReleaseCaptures(step lifecycleStep) error {
	stopped, err := service.runResourceLink(step.OperationID, "stop")
	if err != nil {
		return err
	}
	for _, ref := range step.Input.CaptureFailures {
		role := "failed-capture:" + ref.OperationID
		if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, role); errors.Is(lookup, sql.ErrNoRows) {
			link := &lifecycleAdmission{
				Parent: Operation{
					ID: step.OperationID,
				},
				Input: step.Input,
				Role:  role,
			}
			_, err = service.admitCaptureDiscard(service.ctx, CaptureDiscardRequest{
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
	if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, "release"); errors.Is(lookup, sql.ErrNoRows) {
		link := &lifecycleAdmission{
			Parent: Operation{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "release",
		}
		if _, err = service.admitRelease(service.ctx, ReleaseRequest{
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

func (service *Service) checkReleaseCaptures(ctx context.Context, tx *sql.Tx, input lifecycleInput) error {
	source, err := loadStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return err
	}
	current, err := failedCaptureReferences(ctx, tx, source)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, input.CaptureFailures) {
		return fmt.Errorf("release failed capture responsibilities changed")
	}
	return nil
}
