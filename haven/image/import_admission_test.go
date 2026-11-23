package image_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func TestActualMergedOCIMaterializationConflict(t *testing.T) {
	t.Parallel()
	e, layout, m := importMergedFixture(t, testfixture.BootLayerEntries())
	_, err := e.ImportOCI(context.Background(), imageimport.ImageImportOptions{
		Layout: layout,
		Digest: m.Digest,
	})
	if !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("digest layout change must conflict: %v", err)
	}
	if again, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:          layout,
			Digest:          m.Digest,
			Materialization: host.MergedOCIEROFSLayout,
		},
	); err != nil || again.Builder != m.Builder {
		t.Fatalf("same layout reimport: %v", err)
	}
	layer := testfixture.PutOCILayer(t, layout, testfixture.BootLayerEntries(), false)
	image := testfixture.PutOCIManifest(t, layout, "layered-first", []testfixture.OCILayerFixture{
		layer,
	})
	opts := imageimport.ImageImportOptions{
		Layout:   layout,
		Digest:   image,
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	}
	if _, err := e.ImportOCI(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	opts.Materialization = host.MergedOCIEROFSLayout
	_, err = e.ImportOCI(context.Background(), opts)
	if !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("layered-first materialization change must conflict: %v", err)
	}
}

func TestHavenImageReplayDoesNotRunConflictingRequest(t *testing.T) {
	for _, action := range []string{
		"import",
		"cleanup",
	} {
		t.Run(action, func(t *testing.T) {
			service := newImageFixture(t)
			id, options := testfixture.SeedPendingImageImport(t, service.State)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			root := authorization.Principal{
				Root:     true,
				TenantID: "root",
			}
			before, err := testfixture.ObserveStep(ctx, service.State.Database(), id, "image_import")
			if err != nil {
				t.Fatal(err)
			}
			if action == "import" {
				changed := options
				changed.MaxBytes++
				_, err = service.Images.AdmitImport(ctx, root, "original", changed)
			} else {
				_, err = service.Images.AdmitCleanup(ctx, root, "original", image.ImageCleanupRequest{
					ImportOperationID: id,
				})
			}
			if !errors.Is(err, errs.ErrConflict) {
				t.Fatal("changed request did not conflict", err)
			}
			if worker := service.Workers.State(id); worker.Active || worker.Completed {
				t.Fatal("conflicting request dispatched retained work", worker)
			}
			if _, err = os.Stat(filepath.Join(service.State.Directory(), "imports", id)); !os.IsNotExist(err) {
				t.Fatal("conflicting request prepared import work", err)
			}
			replay, err := service.Images.AdmitImport(ctx, root, "original", options)
			if err != nil || replay.ID != id {
				t.Fatal(replay, err)
			}
			failed, err := service.OperationMonitor.WaitManagement(ctx, root, id)
			if err == nil || failed.Status != "failed" {
				t.Fatal("matching replay did not resume the frozen import", failed, err)
			}
			after, err := testfixture.ObserveStep(ctx, service.State.Database(), id, "image_import")
			if err != nil || before.Hash != after.Hash || !bytes.Equal(before.Input, after.Input) {
				t.Fatal("replay changed the frozen plan", err)
			}
		})
	}
}

type imageFixture struct {
	State            *store.State
	Images           *image.Images
	OperationMonitor *operation.Monitor
	Workers          *execution.Workers
}

func newImageFixture(t *testing.T) *imageFixture {
	t.Helper()
	state := testfixture.OpenState(t)
	lifetime := execution.NewLifetime(t.Context())
	workers := execution.NewWorkers(lifetime)
	gate := execution.NewAdmissionGate(new(sync.Mutex), lifetime, state.Reviewed())
	fixture := &imageFixture{
		State: state,
		Images: image.NewImages(state, image.ImageDependencies{
			StoreWriteMu:         new(sync.Mutex),
			ImageArtifactsMu:     new(sync.Mutex),
			NodeIdempotencyLocks: new(operation.IdempotencyLocks),
			Gate:                 gate,
			Workers:              workers,
		}, image.ImageOptions{}),
		OperationMonitor: operation.NewMonitor(state.Database(), workers, state.Identity().Identity),
		Workers:          workers,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := lifetime.Close(ctx, func() {}, state.Close); err != nil {
			t.Error(err)
		}
	})
	return fixture
}
