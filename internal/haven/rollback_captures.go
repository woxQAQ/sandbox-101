package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type rowsQueryer interface {
	queryer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func pendingPublishedCaptures(ctx context.Context, q rowsQueryer, source resourceStep) ([]generationRef, error) {
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
		if capture.Status != "succeeded" || capture.State.Phase != "complete" || capture.State.ResumeRequired || capture.Input.Source.OperationID != source.OperationID || capture.Input.Source.Hash != source.Hash || capture.Input.Request.ExpectedInstance != source.State.Witness {
			return nil, fmt.Errorf("rollback capture user lacks original published generation proof")
		}
		refs = append(refs, generationRef{
			OperationID: id,
			InputHash:   capture.Hash,
		})
	}
	return refs, nil
}

func (service *Service) checkRollbackCaptures(ctx context.Context, tx *sql.Tx, input lifecycleInput) error {
	source, err := loadStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return err
	}
	refs, err := pendingPublishedCaptures(ctx, tx, source)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(refs, input.Rollback.RawCaptures) {
		return fmt.Errorf("rollback original capture responsibilities changed")
	}
	for _, ref := range refs {
		capture, err := loadCapture(ctx, tx, ref.OperationID)
		if err != nil {
			return err
		}
		if err = service.captureCleanupProof(ctx, tx, capture, source.Candidate, input.SourceInstance, "pending"); err != nil {
			return err
		}
	}
	return nil
}

func (service *Service) cleanupRollbackCaptures(step lifecycleStep, stop string) error {
	for _, ref := range step.Input.Rollback.RawCaptures {
		role := "raw:" + ref.OperationID
		if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, role); errors.Is(lookup, sql.ErrNoRows) {
			link := &lifecycleAdmission{
				Parent: Operation{
					ID: step.OperationID,
				},
				Input: step.Input,
				Role:  role,
			}
			if _, err := service.admitCaptureCleanup(service.ctx, CaptureCleanupRequest{
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
