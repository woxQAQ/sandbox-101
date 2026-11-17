package native

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	ocidigest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/types"
)

func privateOCIImport(t *testing.T) imageimport.ImageImportOptions {
	t.Helper()
	ctx := context.Background()
	_, err := host.ImageTools(ctx)
	if err == nil {
		err = host.RunImageTool(ctx, t.TempDir(), "mkfs.erofs", "--version")
	}
	if err != nil {
		if os.Getenv("SANDBOX_REQUIRE_EROFS_TESTS") == "1" {
			t.Fatal(err)
		}
		t.Skipf("actual EROFS tool/namespace unavailable: %v", err)
	}
	layout := t.TempDir()
	if err = os.MkdirAll(filepath.Join(layout, "blobs", "sha256"), 0o700); err != nil {
		t.Fatal(err)
	}
	put := func(payload []byte, media string) v1.Descriptor {
		hash := ocidigest.FromBytes(payload)
		if err = os.WriteFile(filepath.Join(layout, "blobs", "sha256", hash.Encoded()), payload, 0o400); err != nil {
			t.Fatal(err)
		}
		return v1.Descriptor{
			MediaType: media,
			Digest:    hash,
			Size:      int64(len(payload)),
		}
	}
	var layer bytes.Buffer
	writer := tar.NewWriter(&layer)
	for _, name := range []string{
		"boot/",
		"boot/vmlinux",
		"boot/initramfs",
		"usr/",
		"usr/lib/",
		"usr/lib/systemd/",
		"usr/lib/systemd/systemd",
		"usr/local/",
		"usr/local/libexec/",
		"usr/local/libexec/sandbox-agent",
		"etc/",
		"etc/systemd/",
		"etc/systemd/system/",
		"etc/systemd/system/sysinit.target.wants/",
		"etc/systemd/system/sysinit.target.wants/sandbox-agent.service",
	} {
		header := tar.Header{
			Name:     name,
			Mode:     0o755,
			Typeflag: tar.TypeReg,
			Size:     7,
		}
		if name[len(name)-1] == '/' {
			header.Typeflag = tar.TypeDir
			header.Size = 0
		}
		if err = writer.WriteHeader(&header); err != nil {
			t.Fatal(err)
		}
		if header.Size != 0 {
			if _, err = writer.Write([]byte("fixture")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	layerDescriptor := put(layer.Bytes(), v1.MediaTypeImageLayer)
	config := v1.Image{
		Platform: v1.Platform{
			Architecture: runtime.GOARCH,
			OS:           "linux",
		},
		RootFS: v1.RootFS{
			Type: "layers",
			DiffIDs: []ocidigest.Digest{
				layerDescriptor.Digest,
			},
		},
	}
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	manifest := v1.Manifest{
		Versioned: specs.Versioned{
			SchemaVersion: 2,
		},
		MediaType: v1.MediaTypeImageManifest,
		Config:    put(configBytes, v1.MediaTypeImageConfig),
		Layers: []v1.Descriptor{
			layerDescriptor,
		},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := put(manifestBytes, v1.MediaTypeImageManifest)
	return imageimport.ImageImportOptions{
		Layout:   layout,
		Digest:   descriptor.Digest.String(),
		MaxBytes: 1 << 20,
		MaxFiles: 128,
	}
}

func TestHavenActualOCIImportPublishesSingleCatalog(t *testing.T) {
	for _, layout := range []string{
		host.LayeredEROFSLayout,
		host.MergedOCIEROFSLayout,
	} {
		t.Run(layout, func(t *testing.T) {
			options := privateOCIImport(t)
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
				last := service.Workers.Snapshot(operation.ID).Err
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
				active := service.Workers.Snapshot(operation.ID).Active
				if !active {
					break
				}
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(time.Millisecond)
			}
			report, err := service.GarbageCollection.CollectGarbage(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "work-gc", types.GCOptions{
				Budget: 20 * time.Second,
			})
			if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "image_work" {
				for _, item := range report.Deferred {
					if item.OperationID != "" {
						last := service.Workers.Snapshot(item.OperationID).Err
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
