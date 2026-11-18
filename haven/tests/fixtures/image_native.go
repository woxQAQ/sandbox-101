package fixtures

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
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

func ImportMergedFixture(t *testing.T, layers ...[]TarEntry) (*nativeImageFixture, string, host.ImageManifest) {
	t.Helper()
	RequireMergedOCI(t)
	layout := t.TempDir()
	var fixtures []OCILayerFixture
	for i, entries := range layers {
		fixtures = append(fixtures, PutOCILayer(t, layout, entries, i%2 == 0))
	}
	image := PutOCIManifest(t, layout, "compat", fixtures)
	e := NewNativeImageFixture(t)
	t.Cleanup(func() {
		e.Close()
	})
	m, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:          layout,
			Digest:          image,
			Materialization: host.MergedOCIEROFSLayout,
			MaxBytes:        4 << 20,
			MaxFiles:        1000,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return e, layout, m
}

func ExtractMergedManifest(t *testing.T, e *nativeImageFixture, m host.ImageManifest) string {
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

func dumpMergedPath(t *testing.T, e *nativeImageFixture, m host.ImageManifest, path string) string {
	t.Helper()
	image := filepath.Join(e.Opts.StateDir, "images", m.Digest[7:], "root.erofs")
	if len(m.Artifacts) != 0 {
		image = filepath.Join(e.Opts.StateDir, m.Artifacts[0].Path)
	}
	out, err := exec.Command("dump.erofs", "--path="+path, image).CombinedOutput()
	if err != nil {
		t.Fatalf("native inode inspection: %v: %s", err, out)
	}
	return string(out)
}

//nolint:staticcheck // Verify binary capability and ACL xattrs across the legacy tar representation and both materializers.
func CheckOCIHardlinksAndMetadata(t *testing.T, materialization string) {
	root := DirEntry(".")
	root.Header.Format = tar.FormatPAX
	root.Header.Mode, root.Header.Uid, root.Header.Gid = 0o751, 42, 43
	root.Header.ModTime = time.Unix(1700000010, 123456789)
	root.Header.Xattrs = map[string]string{
		"user.root": "root retained",
	}
	parent := DirEntry("inherited")
	parent.Header.Format = tar.FormatPAX
	parent.Header.Mode = 0o2750
	parent.Header.ModTime = time.Unix(1700000020, 234567890)
	parent.Header.Xattrs = map[string]string{
		"user.directory": "inherited retained",
	}
	file := FileEntry("target", "cross-layer content")
	file.Header.Format = tar.FormatPAX
	file.Header.Mode = 0o6740
	file.Header.ModTime = time.Unix(1700000030, 345678901)
	capability := make([]byte, 20)
	binary.LittleEndian.PutUint32(capability, 0x02000001)
	binary.LittleEndian.PutUint32(capability[4:], 1<<10)
	file.Header.Xattrs = map[string]string{
		"user.fixture":        "retained",
		"security.capability": string(capability),
	}
	aclFile := FileEntry("acl", "acl payload")
	acl := make([]byte, 4+5*8)
	binary.LittleEndian.PutUint32(acl, 2)
	for i, entry := range []struct {
		tag  uint16
		perm uint16
		id   uint32
	}{
		{1, 6, 0xffffffff},
		{2, 4, 333},
		{4, 4, 0xffffffff},
		{16, 4, 0xffffffff},
		{32, 0, 0xffffffff},
	} {
		binary.LittleEndian.PutUint16(acl[4+i*8:], entry.tag)
		binary.LittleEndian.PutUint16(acl[6+i*8:], entry.perm)
		binary.LittleEndian.PutUint32(acl[8+i*8:], entry.id)
	}
	aclFile.Header.Xattrs = map[string]string{
		"system.posix_acl_access": string(acl),
	}
	capNS := FileEntry("cap-ns", "namespaced capability")
	capV3 := append(append([]byte(nil), capability...), make([]byte, 4)...)
	binary.LittleEndian.PutUint32(capV3, 0x03000001)
	binary.LittleEndian.PutUint32(capV3[20:], 333)
	capNS.Header.Xattrs = map[string]string{
		"security.capability": string(capV3),
	}
	base := append(BootLayerEntries(), root, parent, file, aclFile, capNS)
	top := []TarEntry{
		HardEntry("alias", "target"),
		FileEntry("inherited/new", "new child"),
	}
	var e *nativeImageFixture
	var m host.ImageManifest
	if materialization == host.LayeredEROFSLayout {
		e, _, m = ImportSharedFixture(t, base, top)
	} else {
		e, _, m = ImportMergedFixture(t, base, top)
	}
	tree := ExtractMergedManifest(t, e, m)
	for name, want := range map[string]string{
		"target":        "cross-layer content",
		"alias":         "cross-layer content",
		"inherited/new": "new child",
	} {
		data, err := os.ReadFile(filepath.Join(tree, name))
		if err != nil || string(data) != want {
			t.Fatalf("%s: %q %v", name, data, err)
		}
	}
	a, err := os.Stat(filepath.Join(tree, "target"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(tree, "alias"))
	if err != nil || !os.SameFile(a, b) {
		t.Fatalf("cross-layer hardlink identity lost: %v", err)
	}
	for name, header := range map[string]tar.Header{
		".":         root.Header,
		"inherited": parent.Header,
		"target":    file.Header,
		"acl":       aclFile.Header,
		"cap-ns":    capNS.Header,
	} {
		st, err := os.Stat(filepath.Join(tree, name))
		if err != nil || st.Mode().Perm() != os.FileMode(header.Mode&0o777) || !st.ModTime().Equal(header.ModTime) {
			t.Fatalf("%s metadata: %+v %v; expected %+v", name, st, err, header)
		}
		dump := dumpMergedPath(t, e, m, "/"+strings.TrimPrefix(name, "."))
		wantOwner := "Uid: 123   Gid: 456"
		if name == "." {
			wantOwner = "Uid: 42   Gid: 43"
		}
		if !strings.Contains(dump, wantOwner) {
			t.Fatalf("%s ownership: %s", name, dump)
		}
		var native unix.Stat_t
		if err := unix.Lstat(filepath.Join(tree, name), &native); err != nil || native.Mode&0o7777 != uint32(header.Mode) {
			t.Fatalf("%s permission bits: %#o %v; expected %#o", name, native.Mode&0o7777, err, header.Mode)
		}
		for key, value := range header.Xattrs {
			buf := make([]byte, 4096)
			n, err := unix.Getxattr(filepath.Join(tree, name), key, buf)
			got := buf[:max(0, n)]
			want := []byte(value)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s xattr %s: %x %v; expected %x", name, key, got, err, want)
			}
		}
	}
	if materialization == host.LayeredEROFSLayout {
		if m.Format != 2 || !m.SharedLayered() || m.Builder != host.ImageBuilder(m.Tools) {
			t.Fatal("shared materialization lacks its new immutable builder identity")
		}
		return
	}
	if m.Format != 3 || m.Layout != host.MergedOCIEROFSLayout || m.Builder != host.MergedOCIBuilder(m.Tools) ||
		m.Builder == host.ImageBuilder(m.Tools) {
		t.Fatal("merged compatibility lacks a distinct immutable identity")
	}
	bindings, err := m.ReadOnlyBindings(filepath.Join(e.Opts.StateDir, "images", m.Digest[7:]))
	if err != nil || len(bindings) != 1 || bindings[0].GuestPath != "/images/root.erofs" ||
		bindings[0].Serial != "sandbox-root" {
		t.Fatalf("merged runtime binding: %+v %v", bindings, err)
	}
	if parameter, err := m.DataKernelParameter(); err != nil || parameter != "" {
		t.Fatalf("merged data kernel parameter: %q %v", parameter, err)
	}
}

func ImportSharedFixture(t *testing.T, layers ...[]TarEntry) (*nativeImageFixture, string, host.ImageManifest) {
	t.Helper()
	RequireEROFS(t)
	layout := t.TempDir()
	var fixtures []OCILayerFixture
	for i, entries := range layers {
		fixtures = append(fixtures, PutOCILayer(t, layout, entries, i%2 == 0))
	}
	image := PutOCIManifest(t, layout, "shared", fixtures)
	e := NewNativeImageFixture(t)
	t.Cleanup(func() {
		e.Close()
	})
	m, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   image,
			MaxBytes: 8 << 20,
			MaxFiles: 1000,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return e, layout, m
}

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
