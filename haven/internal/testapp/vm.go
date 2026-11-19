package testapp

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/operation"
)

func ColdServiceWithOptions(t *testing.T, options application.Options, factory func(*testfixture.ColdRuntime) loom.Backend) (*application.App, *testfixture.ColdRuntime) {
	t.Helper()
	fault := options.Fault
	backend := testfixture.NewColdRuntime()
	var configured loom.Backend = backend
	if factory != nil {
		configured = factory(backend)
	}
	options.StateDir = filepath.Join(t.TempDir(), "state")
	options.Initialize = true
	options.Backend = configured
	options.MaxMemoryMiB = 8192
	options.MaxVCPUs = 64
	options.SafetyBytes = 1
	service, err := application.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	manifest := host.ImageManifest{
		Format:       1,
		Layout:       "merged-erofs-v1",
		Digest:       "sha256:" + strings.Repeat("a", 64),
		Architecture: runtime.GOARCH,
		Files:        map[string]string{},
	}
	imageDir := filepath.Join(service.State.Directory(), "images", manifest.Digest[7:])
	if err = os.MkdirAll(imageDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"kernel",
		"initramfs",
		"root.erofs",
	} {
		path := filepath.Join(imageDir, name)
		if err = os.WriteFile(path, []byte(name), 0o444); err != nil {
			t.Fatal(err)
		}
		manifest.Files[name], err = loom.DigestFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = loom.WriteJSON(filepath.Join(imageDir, "image.json"), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err = service.State.Database().Exec("INSERT INTO tenants VALUES(?,?,?)", "tenant", "fixture-hash", testfixture.Encode(authorization.Limits{
		MaxSandboxes:  10,
		MaxStorageMiB: 4096,
	})); err != nil {
		t.Fatal(err)
	}
	if _, err = service.State.Database().Exec("INSERT INTO images VALUES(?,?)", manifest.Digest, testfixture.Encode(manifest)); err != nil {
		t.Fatal(err)
	}
	service.Options.Fault = nil
	_, err = service.Supply.AdmitTemplate(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, "publish-cold", operation.Command{
		Action: "template_put",
		Template: &operation.TemplateSpec{
			ID:    "cold",
			Image: manifest.Digest,
			Shape: loom.Shape{
				VCPUs:     1,
				MemoryMiB: 128,
				DiskMiB:   64,
			},
		},
	})
	service.Options.Fault = fault
	if err != nil {
		t.Fatal(err)
	}
	return service, backend
}

// ColdService starts a private App on the cold runtime fixture.
func ColdService(t *testing.T, fault func(string) error) (*application.App, *testfixture.ColdRuntime) {
	t.Helper()
	return ColdServiceWithBackend(t, fault, nil)
}

// ColdServiceWithBackend starts a private App with an optional backend wrapper.
func ColdServiceWithBackend(t *testing.T, fault func(string) error, factory func(*testfixture.ColdRuntime) loom.Backend) (*application.App, *testfixture.ColdRuntime) {
	t.Helper()
	return ColdServiceWithOptions(t, application.Options{
		Fault: fault,
	}, factory)
}
