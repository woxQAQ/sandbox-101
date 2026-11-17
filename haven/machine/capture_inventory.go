package machine

import (
	"context"
	"fmt"

	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func FailedCaptureReferences(ctx context.Context, database store.DBTX, source ResourceStep) ([]ownership.GenerationRef, error) {
	ids, err := quota.New(database).ResourceHolderIDs(ctx, "vm", source.Candidate, "capture")
	if err != nil {
		return nil, err
	}
	var refs []ownership.GenerationRef
	for _, id := range ids {
		capture, err := LoadCapture(ctx, database, id)
		if err != nil {
			return nil, err
		}
		if !operation.Terminal(capture.Status) || capture.Input.Source.OperationID != source.OperationID || capture.Input.Source.Hash != source.Hash || capture.Input.Request.ExpectedInstance != source.State.Witness {
			return nil, fmt.Errorf("release capture user lacks original terminal generation proof")
		}
		if capture.Status == "failed" {
			if capture.State.ResumeRequired || capture.State.Error == "" {
				return nil, fmt.Errorf("release capture failure has unresolved source resume")
			}
			refs = append(refs, ownership.GenerationRef{
				OperationID: id,
				InputHash:   capture.Hash,
			})
		}
	}
	return refs, nil
}

func PendingPublishedCaptures(ctx context.Context, database store.DBTX, source ResourceStep) ([]ownership.GenerationRef, error) {
	ids, err := quota.New(database).ResourceHolderIDs(ctx, "vm", source.Candidate, "capture")
	if err != nil {
		return nil, err
	}
	var refs []ownership.GenerationRef
	for _, id := range ids {
		capture, err := LoadCapture(ctx, database, id)
		if err != nil {
			return nil, err
		}
		if capture.Status != "succeeded" || capture.State.Phase != "complete" || capture.State.ResumeRequired || capture.Input.Source.OperationID != source.OperationID || capture.Input.Source.Hash != source.Hash || capture.Input.Request.ExpectedInstance != source.State.Witness {
			return nil, fmt.Errorf("rollback capture user lacks original published generation proof")
		}
		refs = append(refs, ownership.GenerationRef{
			OperationID: id,
			InputHash:   capture.Hash,
		})
	}
	return refs, nil
}
