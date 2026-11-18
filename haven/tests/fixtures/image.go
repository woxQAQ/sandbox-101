package fixtures

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	ocidigest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	image "github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func PrivateOCIImport(t *testing.T) imageimport.ImageImportOptions {
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

func FailedCachedImport(t *testing.T, fault func(string) error) (*application.App, operation.Record) {
	t.Helper()
	options := PrivateOCIImport(t)
	var abort atomic.Bool
	abort.Store(true)
	service, _, _, _ := testapp.SandboxFixtureWithFault(t, func(point string) error {
		if abort.Load() && point == "image_import_layer_saved" {
			return errors.New("known fixture abort after cache responsibility persisted")
		}
		if fault != nil {
			return fault(point)
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	acceptedOperation, err := service.Images.AdmitImport(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "import", options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Operations.WaitManagement(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, acceptedOperation.ID)
	if err == nil || result.Status != "failed" {
		t.Fatal(result, err)
	}
	WaitImportInterruption(t, service, acceptedOperation.ID)
	abort.Store(false)
	return service, result
}

func WaitImportInterruption(t *testing.T, service *application.App, id string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		worker := service.Workers.State(id)
		active, last := worker.Active, worker.Err
		if !active && last != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("image effect interruption missing")
		}
		time.Sleep(time.Millisecond)
	}
}

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

// This private recovery fixture starts with a frozen plan whose source vanished
// before staging. The real runner prepares owned directories and records failure;
// no EROFS materialization, namespace or VM behavior is represented by this fixture.
func PendingImageRecovery(t *testing.T) (*application.App, string, imageimport.ImageImportOptions) {
	t.Helper()
	service, _ := ColdService(t, nil)
	id, options := testfixture.SeedPendingImageImport(t, service.State)
	return service, id, options
}
