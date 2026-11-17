package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

// captureDiscardAdmissionProof requires the original failed capture to retain
// custody. The discard operation and its transferred holders do not exist yet.
func (service *CaptureDiscard) captureDiscardAdmissionProof(ctx context.Context, tx *sql.Tx, step CaptureDiscardStep) error {
	if _, err := service.verifyCaptureDiscardSource(ctx, tx, step, ""); err != nil {
		return err
	}
	return verifyCaptureDiscardCustody(ctx, tx, step, captureDiscardCustody{
		Deleting:      0,
		CleanupStatus: "blocked",
		Holder: quota.Holder{
			Kind: "engine_dependency",
			ID:   step.Input.Source.OperationID + ":capture",
		},
	})
}

// captureDiscardProof requires admitted ownership and the exact transferred
// cleanup responsibility before effects or completion can proceed.
func (service *CaptureDiscard) captureDiscardProof(ctx context.Context, tx *sql.Tx, step CaptureDiscardStep) error {
	record, err := service.verifyCaptureDiscardSource(ctx, tx, step, step.OperationID)
	if err != nil {
		return err
	}
	if step.Input.Lifecycle != "" {
		if err = service.ownership.VerifyCaptureDiscardLink(ctx, tx, step); err != nil {
			return err
		}
	}
	if err = verifyCaptureDiscardCustody(ctx, tx, step, captureDiscardCustody{
		Deleting:      1,
		CleanupStatus: "transferred",
		Holder: quota.Holder{
			Kind: "cleanup",
			ID:   step.OperationID + ":capture_discard",
		},
	}); err != nil {
		return err
	}
	input, request := step.Input, step.Input.Request
	snapshot := input.Source.Input.Snapshot
	resource := input.Source.Input.Source
	count, err := dbgen.New(tx).MatchingCleanupCount(ctx, dbgen.MatchingCleanupCountParams{
		ID:           "capture_discard:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "snapshot",
		ResourceID:   snapshot.ID,
		Status:       "pending",
		Body:         operation.Encode(input),
	})
	if err != nil || count != 1 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	var parent struct {
		Action  string
		Request CaptureDiscardRequest
	}
	// Decode the discard result independently; missing fields cannot inherit
	// identity from the original failed capture result.
	var result operation.Result
	projection, err := operation.New(tx).Projection(ctx, step.OperationID)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(projection.Request, &parent); err != nil {
		return err
	}
	if err = json.Unmarshal(projection.Body, &result); err != nil {
		return err
	}
	if operation.Digest(projection.Request) != projection.RequestHash || operation.Terminal(projection.Status) || projection.ScopeKind != "tenant" || projection.ScopeID != request.TenantID || parent.Action != "capture_discard" || parent.Request != request || result.SnapshotID != snapshot.ID || result.VMID != resource.Candidate || result.SandboxID != record.ID {
		return fmt.Errorf("capture discard parent identity changed")
	}
	return nil
}
