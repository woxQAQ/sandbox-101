package native

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/tests/fixtures"
	"github.com/woxQAQ/haven/types"
)

func TestHavenActualOCIImportPublishesSingleCatalog(t *testing.T) {
	for _, layout := range []string{
		host.LayeredEROFSLayout,
		host.MergedOCIEROFSLayout,
	} {
		t.Run(layout, func(t *testing.T) {
			options := fixtures.PrivateOCIImport(t)
			options.Materialization = layout
			service, _, _, _ := testapp.SandboxFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			operation, err := service.Images.AdmitImport(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "import", options)
			if err != nil {
				t.Fatal(err)
			}
			result, err := service.Operations.WaitManagement(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, operation.ID)
			if err != nil {
				last := service.Workers.State(operation.ID).Err
				t.Fatal(result, err, last)
			}
			manifest, err := image.LoadManifest(ctx, service.State.Database(), options.Digest)
			if err != nil || manifest.Layout != layout || result.Result.ImageDigest != options.Digest {
				t.Fatal(manifest, result, err)
			}
			if err = image.VerifyFiles(manifest, filepath.Join(service.State.Directory(), "images", options.Digest[7:])); err != nil {
				t.Fatal(err)
			}
			for {
				active := service.Workers.State(operation.ID).Active
				if !active {
					break
				}
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(time.Millisecond)
			}
			report, err := service.GC.Collect(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "work-gc", types.GCOptions{
				Budget: 20 * time.Second,
			})
			if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "image_work" {
				for _, item := range report.Deferred {
					if item.OperationID != "" {
						last := service.Workers.State(item.OperationID).Err
						t.Log(item.Kind, item.OperationID, last)
					}
				}
				t.Fatal(report, err)
			}
			if err = image.VerifyFiles(manifest, filepath.Join(service.State.Directory(), "images", options.Digest[7:])); err != nil {
				t.Fatal("work cleanup affected publication", err)
			}
			replay, err := service.Images.AdmitImport(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "import", options)
			if err != nil || replay.ID != operation.ID {
				t.Fatal(replay, err)
			}
			conflict := options
			if layout == host.LayeredEROFSLayout {
				conflict.Materialization = host.MergedOCIEROFSLayout
			} else {
				conflict.Materialization = host.LayeredEROFSLayout
			}
			if _, err = service.Images.AdmitImport(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "conflict", conflict); err == nil {
				t.Fatal("digest changed materialization")
			}
		})
	}
}
