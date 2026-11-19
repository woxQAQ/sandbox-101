package testfixture

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/store"
)

func RequireMergedOCI(t *testing.T) {
	t.Helper()
	RequireEROFS(t)
	tools, err := host.ImageTools(context.Background())
	if err == nil {
		_, err = host.MergedOCITools(context.Background(), tools)
	}
	if err == nil {
		err = host.RunImageTool(context.Background(), t.TempDir(), "umoci", "--version")
	}
	if err != nil {
		if os.Getenv("SANDBOX_REQUIRE_EROFS_TESTS") == "1" {
			t.Fatal(err)
		}
		t.Skipf("actual OCI compatibility tool/namespace test unavailable: %v", err)
	}
}

func ExtractMergedManifest(t *testing.T, e *NativeImageFixture, m host.ImageManifest) string {
	t.Helper()
	bindings, err := m.ReadOnlyBindings(filepath.Join(e.Opts.StateDir, "images", m.Digest[7:]))
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	fsckArgs := []string{
		"--extract=" + filepath.Join(out, "tree"),
		"--xattrs",
		"--no-preserve-owner",
		"--preserve-perms",
	}
	for i, binding := range bindings {
		if i != 0 {
			fsckArgs = append(fsckArgs, "--device="+binding.HostPath)
		}
	}
	fsckArgs = append(fsckArgs, bindings[0].HostPath)
	if err = host.RunImageTool(context.Background(), out, "fsck.erofs", fsckArgs...); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(out, "tree")
}

// NativeImageFixture exercises Haven's durable importer with an injected VM backend.
type NativeImageFixture struct {
	Service *application.App
	DB      *store.Database
	Faults  *SnapshotFaults
	Opts    struct {
		StateDir string
	}
}

func NewNativeImageFixture(t *testing.T) *NativeImageFixture {
	t.Helper()
	return NativeImageFixtureAt(t, filepath.Join(t.TempDir(), "state"))
}

func NativeImageFixtureAt(t *testing.T, directory string) *NativeImageFixture {
	t.Helper()
	faults := &SnapshotFaults{}
	service, err := application.Open(application.Options{
		Fault:        faults.Check,
		StateDir:     directory,
		Initialize:   true,
		Backend:      NewColdRuntime(),
		MaxMemoryMiB: 8192,
		MaxVCPUs:     64,
		SafetyBytes:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &NativeImageFixture{
		Service: service,
		DB:      service.State.Database(),
		Faults:  faults,
	}
	fixture.Opts.StateDir = service.State.Directory()
	if err := os.Mkdir(filepath.Join(service.State.Directory(), "images"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	return fixture
}

func (fixture *NativeImageFixture) Close() error {
	return fixture.Service.Close(context.Background())
}

func (fixture *NativeImageFixture) ImportOCI(ctx context.Context, options imageimport.ImageImportOptions) (host.ImageManifest, error) {
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	operation, err := fixture.Service.Images.AdmitImport(ctx, root, "native-"+Digest(Encode(options)), options)
	if err != nil {
		return host.ImageManifest{}, err
	}
	_, err = fixture.Service.Operations.WaitManagement(ctx, root, operation.ID)
	if err != nil {
		cause := fixture.Service.Workers.State(operation.ID).Err
		if cause != nil {
			return host.ImageManifest{}, cause
		}
		return host.ImageManifest{}, err
	}
	return image.LoadManifest(ctx, fixture.DB, options.Digest)
}

func (fixture *NativeImageFixture) ImportWork(t *testing.T, imageID string) string {
	t.Helper()
	var operation string
	if err := fixture.DB.QueryRow("SELECT operation_id FROM image_intents WHERE digest=?", imageID).Scan(&operation); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(fixture.Opts.StateDir, "imports", operation)
}
