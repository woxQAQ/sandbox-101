package component

import (
	"context"
	"path/filepath"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/store"
)

type CaptureDiscardRequest = machine.CaptureDiscardRequest

type captureDiscardStep = machine.CaptureDiscardStep

func loadCaptureDiscard(ctx context.Context, database store.DBTX, id string) (captureDiscardStep, error) {
	return machine.LoadCaptureDiscard(ctx, database, id)
}

func captureDiscardPaths(step captureDiscardStep) []string {
	source := step.Input.Source
	return []string{
		filepath.Join(source.Input.Source.Input.Resource.Dir, "capture", source.OperationID),
		source.Input.Snapshot.Dir,
	}
}
