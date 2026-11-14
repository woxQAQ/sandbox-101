package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

// captureCleanupProof checks historical publication separately from the current
// cleanup destination. Publication never by itself retires raw VMM output.
func (service *Service) captureCleanupProof(ctx context.Context, tx *sql.Tx, step captureStep, vm, instance, status string) error {
	current, err := loadCapture(ctx, tx, step.OperationID)
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
	var body []byte
	if err = tx.QueryRowContext(ctx, "SELECT body FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind='capture' AND resource_id=? AND status=?", "capture:"+step.OperationID, step.OperationID, vm, status).Scan(&body); err != nil {
		return err
	}
	if string(body) != string(encode(step.Input)) {
		return fmt.Errorf("raw capture cleanup input changed")
	}
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='capture' AND holder_id=?", r.Config.Image, step.OperationID).Scan(&count); err != nil || count != 1 {
		return fmt.Errorf("raw capture image dependency missing: %v", err)
	}
	return nil
}

func (service *Service) releaseCaptures(ctx context.Context, tx *sql.Tx, vm, instance string) ([]captureStep, error) {
	rows, err := tx.QueryContext(ctx, "SELECT holder_id FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='capture' ORDER BY holder_id", vm)
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
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	var captures []captureStep
	for _, id := range ids {
		step, err := loadCapture(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err = service.captureCleanupProof(ctx, tx, step, vm, instance, "pending"); err != nil {
			return nil, err
		}
		captures = append(captures, step)
	}
	return captures, nil
}

func (service *Service) checkTransferredCaptures(ctx context.Context, tx *sql.Tx, step resourceStep) error {
	seen := map[string]bool{}
	for _, capture := range step.Input.Captures {
		if seen[capture.OperationID] {
			return fmt.Errorf("duplicate capture cleanup destination")
		}
		seen[capture.OperationID] = true
		if capture.Input.Request.TenantID != step.Input.TenantID || capture.Input.Request.SandboxID != step.Input.SandboxID {
			return fmt.Errorf("capture cleanup tenant or sandbox changed")
		}
		if err := service.captureCleanupProof(ctx, tx, capture, step.Candidate, step.ExpectedInstance, "transferred"); err != nil {
			return err
		}
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind='capture' AND resource_id=? AND status='transferred'", step.Candidate).Scan(&count); err != nil || count != len(seen) {
		return fmt.Errorf("capture cleanup handoff differs from frozen release: %v", err)
	}
	return nil
}

func captureBoundaries(step resourceStep) []loom.CaptureBoundary {
	var boundaries []loom.CaptureBoundary
	for _, capture := range step.Input.Captures {
		if capture.State.Boundary != nil {
			boundaries = append(boundaries, *capture.State.Boundary)
		}
	}
	return boundaries
}
