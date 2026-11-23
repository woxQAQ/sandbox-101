package testapp

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// NativeImageFixture runs the durable importer in a private App.
type NativeImageFixture struct {
	Service  *application.App
	DB       *store.Database
	Faults   *testfixture.SnapshotFaults
	StateDir string
}

func NewNativeImageFixture(t *testing.T) *NativeImageFixture {
	t.Helper()
	return NativeImageFixtureAt(t, filepath.Join(t.TempDir(), "state"))
}

func NativeImageFixtureAt(t *testing.T, directory string) *NativeImageFixture {
	t.Helper()
	faults := &testfixture.SnapshotFaults{}
	service, err := application.Open(application.Options{
		Fault:        faults.Check,
		StateDir:     directory,
		Initialize:   true,
		Backend:      testfixture.NewColdRuntime(),
		MaxMemoryMiB: 8192,
		MaxVCPUs:     64,
		SafetyBytes:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := os.Mkdir(filepath.Join(service.State.Directory(), "images"), 0o700); err != nil {
		t.Fatal(err)
	}
	return &NativeImageFixture{
		Service:  service,
		DB:       service.State.Database(),
		Faults:   faults,
		StateDir: service.State.Directory(),
	}
}

func (fixture *NativeImageFixture) ImportOCI(ctx context.Context, options imageimport.ImageImportOptions) (host.ImageManifest, error) {
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	accepted, err := fixture.Service.Images.AdmitImport(ctx, root, "native-"+operation.Digest(operation.Encode(options)), options)
	if err != nil {
		return host.ImageManifest{}, err
	}
	_, err = fixture.Service.OperationMonitor.WaitManagement(ctx, root, accepted.ID)
	if err != nil {
		if cause := fixture.Service.Workers.State(accepted.ID).Err; cause != nil {
			return host.ImageManifest{}, cause
		}
		return host.ImageManifest{}, err
	}
	return image.LoadManifest(ctx, fixture.DB, options.Digest)
}

func (fixture *NativeImageFixture) ImportWork(t *testing.T, imageID string) string {
	t.Helper()
	var id string
	if err := fixture.DB.QueryRow("SELECT operation_id FROM image_intents WHERE deleted_at IS NULL AND digest=?", imageID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(fixture.StateDir, "imports", id)
}

func ImportFixture(t *testing.T) (*NativeImageFixture, string, []testfixture.OCILayerFixture, host.ImageManifest) {
	t.Helper()
	testfixture.RequireEROFS(t)
	layout, layers, digest := testfixture.LayeredOCILayout(t)
	fixture := NewNativeImageFixture(t)
	manifest, err := fixture.ImportOCI(t.Context(), imageimport.ImageImportOptions{
		Layout:   layout,
		Digest:   digest,
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fixture, layout, layers, manifest
}
