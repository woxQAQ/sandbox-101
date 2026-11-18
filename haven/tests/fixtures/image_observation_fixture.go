package fixtures

import (
	"context"
	"encoding/json"

	image "github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/store"
)

// The fixture observes only the frozen path and output witnesses under test.
// Workflow validation remains the same public entry used by store recovery.
type imageImportEvidence struct {
	Input struct {
		Work string `json:"work"`
	}
	State struct {
		Boundary *imageimport.ImageImportBoundary `json:"boundary,omitempty"`
		Proof    *imageimport.ImageImportProof    `json:"proof,omitempty"`
	}
}

func ObserveImageImport(ctx context.Context, database store.DBTX, id string) (imageImportEvidence, error) {
	var evidence imageImportEvidence
	if err := image.ValidateImageImport(ctx, database, id); err != nil {
		return evidence, err
	}
	step, err := testfixture.ObserveStep(ctx, database, id, "image_import")
	if err != nil {
		return evidence, err
	}
	if err = json.Unmarshal(step.Input, &evidence.Input); err != nil {
		return evidence, err
	}
	err = json.Unmarshal(step.Body, &evidence.State)
	return evidence, err
}
