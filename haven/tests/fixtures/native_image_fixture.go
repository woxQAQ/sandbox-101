package fixtures

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
	"github.com/woxQAQ/haven/store"
)

// nativeImageFixture runs the original materialization scenarios through Haven's
// durable importer. Only the unused VM effects are supplied by a host fixture.
// No operation, import witness or successful publication is inserted by it.
type nativeImageFixture struct {
	Service *application.App
	DB      *store.Database
	Faults  *testfixture.SnapshotFaults
	Opts    struct {
		StateDir string
	}
}

func NewNativeImageFixture(t *testing.T) *nativeImageFixture {
	t.Helper()
	return NativeImageFixtureAt(t, filepath.Join(t.TempDir(), "state"))
}

func NativeImageFixtureAt(t *testing.T, directory string) *nativeImageFixture {
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
	fixture := &nativeImageFixture{
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

func (fixture *nativeImageFixture) Close() error {
	return fixture.Service.Close(context.Background())
}

func (fixture *nativeImageFixture) ImportOCI(ctx context.Context, options imageimport.ImageImportOptions) (host.ImageManifest, error) {
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	operation, err := fixture.Service.Images.AdmitImport(ctx, root, "native-"+testfixture.Digest(testfixture.Encode(options)), options)
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

func (fixture *nativeImageFixture) ImportWork(t *testing.T, imageID string) string {
	t.Helper()
	var operation string
	if err := fixture.DB.QueryRow("SELECT operation_id FROM image_intents WHERE digest=?", imageID).Scan(&operation); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(fixture.Opts.StateDir, "imports", operation)
}
