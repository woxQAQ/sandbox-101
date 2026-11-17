package component

import (
	"context"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/store"
)

type CaptureCleanupRequest = machine.CaptureCleanupRequest

type captureCleanupStep = machine.CaptureCleanupStep

func loadCaptureCleanup(ctx context.Context, database store.DBTX, id string) (captureCleanupStep, error) {
	return machine.LoadCaptureCleanup(ctx, database, id)
}
