package machine

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

// CaptureCleanupProof checks historical publication separately from the current
// cleanup destination. Publication never by itself retires raw VMM output.
func CaptureCleanupProof(ctx context.Context, tx *sql.Tx, step CaptureStep, vm, instance, status string) error {
	current, err := LoadCapture(ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.Status != "succeeded" || step.State.Phase != "complete" || step.State.ResumeRequired || !step.State.Materialized || !step.State.ArtifactsSynced || len(step.State.Files) != 4 || step.State.Boundary == nil || step.Input.Source.Candidate != vm || step.Input.Request.ExpectedInstance != instance {
		return fmt.Errorf("raw capture lacks frozen publication proof")
	}
	r := step.Input.Source.Input.Resource
	if step.State.Boundary.Path != filepath.Join(r.Dir, "capture", step.OperationID) {
		return fmt.Errorf("raw capture boundary differs from historical source")
	}
	body, err := dbgen.New(tx).CleanupBody(ctx, dbgen.CleanupBodyParams{
		ID:           "capture:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "capture",
		ResourceID:   vm,
		Status:       status,
	})
	if err != nil {
		return err
	}
	if string(body) != string(operation.Encode(step.Input)) {
		return fmt.Errorf("raw capture cleanup input changed")
	}
	count, err := quota.New(tx).CountResourceReference(ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         r.Config.Image,
		HolderKind: "capture",
		HolderID:   step.OperationID,
	})
	if err != nil || count != 1 {
		return fmt.Errorf("raw capture image dependency missing: %v", err)
	}
	return nil
}

func ReleaseCaptures(ctx context.Context, tx *sql.Tx, vm, instance string) ([]CaptureStep, error) {
	ids, err := quota.New(tx).ResourceHolderIDs(ctx, "vm", vm, "capture")
	if err != nil {
		return nil, err
	}
	var captures []CaptureStep
	for _, id := range ids {
		step, err := LoadCapture(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err = CaptureCleanupProof(ctx, tx, step, vm, instance, "pending"); err != nil {
			return nil, err
		}
		captures = append(captures, step)
	}
	return captures, nil
}

func CheckTransferredCaptures(ctx context.Context, tx *sql.Tx, step ResourceStep) error {
	seen := map[string]bool{}
	for _, capture := range step.Input.Captures {
		if seen[capture.OperationID] {
			return fmt.Errorf("duplicate capture cleanup destination")
		}
		seen[capture.OperationID] = true
		if capture.Input.Request.TenantID != step.Input.TenantID || capture.Input.Request.SandboxID != step.Input.SandboxID {
			return fmt.Errorf("capture cleanup tenant or sandbox changed")
		}
		if err := CaptureCleanupProof(ctx, tx, capture, step.Candidate, step.ExpectedInstance, "transferred"); err != nil {
			return err
		}
	}
	count, err := dbgen.New(tx).CleanupResourceCount(ctx, dbgen.CleanupResourceCountParams{
		ResourceKind: "capture",
		ResourceID:   step.Candidate,
		Status:       "transferred",
	})

	if err != nil || count != int64(len(seen)) {
		return fmt.Errorf("capture cleanup handoff differs from frozen release: %v", err)
	}
	return nil
}

func CaptureBoundaries(step ResourceStep) []loom.CaptureBoundary {
	var boundaries []loom.CaptureBoundary
	for _, capture := range step.Input.Captures {
		if capture.State.Boundary != nil {
			boundaries = append(boundaries, *capture.State.Boundary)
		}
	}
	return boundaries
}
