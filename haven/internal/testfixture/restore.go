package fixtures

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

// The runtime fixture supplies a complete static RAM topology and sparse file.
// The guest image test observes writable-private mappings across VM restore.
type RestorableRuntime struct {
	*testfixture.ColdRuntime
}

func RestorableBackend(runtime *testfixture.ColdRuntime) loom.Backend {
	return &RestorableRuntime{runtime}
}

func (backend *RestorableRuntime) Capture(ctx context.Context, r loom.Resource, directory string) error {
	if err := backend.ColdRuntime.Capture(ctx, r, directory); err != nil {
		return err
	}
	size := r.Config.Shape.MemoryMiB << 20
	manager := map[string]any{
		"boot_ram":    size,
		"current_ram": size,
		"guest_ram_mappings": []any{
			map[string]any{
				"gpa":         0,
				"size":        size,
				"zone_id":     "mem0",
				"virtio_mem":  false,
				"file_offset": 0,
			},
		},
		"memory_ranges": map[string]any{
			"data": []any{
				map[string]any{
					"gpa":    0,
					"length": size,
				},
			},
		},
	}
	state := map[string]any{
		"snapshots": map[string]any{
			"memory-manager": map[string]any{
				"snapshot_data": map[string]any{
					"state": string(testfixture.Encode(manager)),
				},
			},
		},
	}
	if err := os.WriteFile(filepath.Join(directory, "state.json"), testfixture.Encode(state), 0o600); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(directory, "memory-ranges"), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	truncateErr := file.Truncate(size)
	return errors.Join(truncateErr, file.Close())
}

func CloneFixtureWithFault(t *testing.T, fault func(string) error) (*application.App, *RestorableRuntime, operation.Record) {
	t.Helper()
	service, _ := ColdServiceWithBackend(t, fault, RestorableBackend)
	backend := service.Options.Backend.(*RestorableRuntime)
	capture, _ := CaptureReady(t, service, "clone-source")
	step := AwaitCapture(t, service, capture.ID)
	CapturePublished(t, service, step, true)
	return service, backend, capture
}

func AdmitCloneFixture(t *testing.T, service *application.App, snapshot, key string) operation.Record {
	t.Helper()
	acceptedOperation, err := service.Provisioning.AdmitClone(context.Background(), machine.CloneRequest{
		TenantID:   "tenant",
		Key:        key,
		SnapshotID: snapshot,
	})
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation
}
