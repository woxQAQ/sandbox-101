package image_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
)

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
			failed, err := service.Operations.WaitManagement(ctx, root, id)
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
