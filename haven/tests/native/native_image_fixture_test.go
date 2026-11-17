package native

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/store"
)

// nativeImageFixture runs the original materialization scenarios through Haven's
// durable importer. Only the unused VM effects are supplied by a host fixture.
// No operation, import witness or successful publication is inserted by it.
type nativeImageFixture struct {
	service *application.App
	db      *store.Database
	faults  *testfixture.SnapshotFaults
	opts    struct {
		StateDir string
	}
}

func newNativeImageFixture(t *testing.T) *nativeImageFixture {
	t.Helper()
	return nativeImageFixtureAt(t, filepath.Join(t.TempDir(), "state"))
}

func nativeImageFixtureAt(t *testing.T, directory string) *nativeImageFixture {
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
		service: service,
		db:      service.State.Database(),
		faults:  faults,
	}
	fixture.opts.StateDir = service.State.Directory()
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
	return fixture.service.Close(context.Background())
}

func (fixture *nativeImageFixture) ImportOCI(ctx context.Context, options ImportOptions) (ImageManifest, error) {
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	operation, err := fixture.service.Images.AdmitImport(ctx, root, "native-"+testfixture.Digest(testfixture.Encode(options)), options)
	if err != nil {
		return ImageManifest{}, err
	}
	_, err = fixture.service.Operations.WaitManagement(ctx, root, operation.ID)
	if err != nil {
		cause := fixture.service.Workers.Snapshot(operation.ID).Err
		if cause != nil {
			return ImageManifest{}, cause
		}
		return ImageManifest{}, err
	}
	return image.LoadManifest(ctx, fixture.db, options.Digest)
}

func (fixture *nativeImageFixture) importWork(t *testing.T, imageID string) string {
	t.Helper()
	var operation string
	if err := fixture.db.QueryRow("SELECT operation_id FROM image_intents WHERE digest=?", imageID).Scan(&operation); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(fixture.opts.StateDir, "imports", operation)
}

// These names keep the retained native scenarios readable while calling the
// resource effects directly; they contain no control state or publication logic.
type (
	ImageManifest   = host.ImageManifest
	ImageArtifact   = host.ImageArtifact
	ImportOptions   = imageimport.ImageImportOptions
	layerCache      = host.LayerCache
	cachedHeader    = host.CachedHeader
	headerAttribute = host.HeaderAttribute
)

const (
	LayeredEROFSLayout   = host.LayeredEROFSLayout
	MergedOCIEROFSLayout = host.MergedOCIEROFSLayout
	MaxImageDataDevices  = host.MaxImageDataDevices
)

var (
	imageTools         = host.ImageTools
	imageBuilder       = host.ImageBuilder
	mergedOCITools     = host.MergedOCITools
	mergedOCIBuilder   = host.MergedOCIBuilder
	runImageTool       = host.RunImageTool
	blob               = host.Blob
	readBlobJSON       = host.ReadBlobJSON
	validateLayer      = host.ValidateLayer
	validateLayerFor   = host.ValidateLayerFor
	validateLayerEntry = host.ValidateLayerEntry
	buildLayerCache    = host.BuildLayerCache
	materializeFsmeta  = host.MaterializeFsmeta
	cacheHeader        = host.CacheHeader
	sharedHeaders      = host.SharedHeaders
	dataArtifact       = host.DataArtifact
	writeJSON          = loom.WriteJSON
	digestFile         = loom.DigestFile
	hashTree           = loom.HashTree
	sealTree           = loom.SealTree
)

func layerFixture(t *testing.T, headers ...*tar.Header) (string, string) {
	t.Helper()
	var buf bytes.Buffer
	writer := tar.NewWriter(&buf)
	for _, header := range headers {
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size > 0 {
			if _, err := writer.Write(make([]byte, header.Size)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "layer.tar")
	if err := os.WriteFile(file, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return file, "sha256:" + testfixture.Digest(buf.Bytes())
}
