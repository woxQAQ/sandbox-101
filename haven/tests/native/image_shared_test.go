package native

import (
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/tests/fixtures"

	ocidigest "github.com/opencontainers/go-digest"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/woxQAQ/haven/internal/testfixture"
)

func TestActualSharedOCIEntryOrderingAndForwardLinks(t *testing.T) {
	t.Parallel()
	base := append(
		fixtures.BootLayerEntries(),
		fixtures.FileEntry("target", "old inode"),
		fixtures.FileEntry("duplicate", "lower"),
		fixtures.DirEntry("directory"),
		fixtures.FileEntry("directory/hidden", "hidden"),
		fixtures.FileEntry("file-to-directory", "old file"),
		fixtures.DirEntry("opaque"),
		fixtures.FileEntry("opaque/hidden", "hidden"),
	)
	sym := fixtures.TarEntry{
		Header: tar.Header{
			Name:     "symbolic",
			Typeflag: tar.TypeSymlink,
			Linkname: "target",
			Mode:     0o777,
			Uid:      123,
			Gid:      456,
		},
	}
	payload := strings.Repeat("shared-payload", 32768)
	top := []fixtures.TarEntry{
		fixtures.HardEntry("old-alias", "target"),
		fixtures.FileEntry("target", "new inode"),
		fixtures.HardEntry("forward-a", "forward-b"),
		fixtures.HardEntry("forward-b", "future"),
		fixtures.FileEntry("future", payload),
		fixtures.FileEntry("future", "replacement"),
		fixtures.FileEntry("duplicate", "first"),
		fixtures.HardEntry("duplicate-old", "duplicate"),
		fixtures.FileEntry("duplicate", "last"),
		sym,
		fixtures.HardEntry("symbolic-alias", "symbolic"),
		fixtures.FileEntry("directory", "now a file"),
		fixtures.DirEntry("file-to-directory"),
		fixtures.FileEntry("file-to-directory/child", "child"),
		fixtures.FileEntry("opaque/.wh..wh..opq", ""),
		fixtures.FileEntry("opaque/new", "new"),
		fixtures.FileEntry("whiteout-before", "old"),
		fixtures.FileEntry(".wh.whiteout-before", ""),
		fixtures.FileEntry(".wh.whiteout-after", ""),
		fixtures.FileEntry("whiteout-after", "new"),
		fixtures.FileEntry("implicit/deep/file", "implicit"),
		fixtures.HardEntry("replaced-parent/alias", "late"),
		fixtures.FileEntry("replaced-parent", "replacement"),
		fixtures.FileEntry("late", "late target"),
		fixtures.HardEntry("overwritten-alias", "never-created"),
		fixtures.FileEntry("overwritten-alias", "replacement"),
	}
	e, layout, m := fixtures.ImportSharedFixture(t, base, top)
	root := fixtures.ExtractManifest(t, e, m)
	for n, want := range map[string]string{
		"target":                  "new inode",
		"old-alias":               "old inode",
		"forward-a":               payload,
		"forward-b":               payload,
		"future":                  "replacement",
		"duplicate":               "last",
		"duplicate-old":           "first",
		"directory":               "now a file",
		"file-to-directory/child": "child",
		"opaque/new":              "new",
		"whiteout-before":         "old",
		"whiteout-after":          "new",
		"implicit/deep/file":      "implicit",
		"replaced-parent":         "replacement",
		"late":                    "late target",
		"overwritten-alias":       "replacement",
	} {
		got, err := os.ReadFile(filepath.Join(root, n))
		if err != nil || string(got) != want {
			t.Fatalf("%s: bytes=%d error=%v", n, len(got), err)
		}
	}
	for _, pair := range [][2]string{
		{"forward-a", "forward-b"},
		{"symbolic", "symbolic-alias"},
	} {
		a, err := os.Lstat(filepath.Join(root, pair[0]))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.Lstat(filepath.Join(root, pair[1]))
		if err != nil || !os.SameFile(a, b) {
			t.Fatalf("hardlink inode identity lost: %v %v", pair, err)
		}
	}
	for _, n := range []string{
		"opaque/hidden",
		".wh.whiteout-before",
		".wh.whiteout-after",
	} {
		if _, err := os.Lstat(filepath.Join(root, n)); !os.IsNotExist(err) {
			t.Fatalf("hidden OCI entry remains: %s %v", n, err)
		}
	}
	for _, n := range []string{
		"implicit",
		"implicit/deep",
	} {
		if info, err := os.Stat(filepath.Join(root, n)); err != nil || !info.IsDir() {
			t.Fatalf("implicit directory missing: %s %v", n, err)
		}
	}
	if st, err := os.Stat(filepath.Join(e.Opts.StateDir, m.Artifacts[0].Path)); err != nil || st.Size() >= int64(len(payload))/4 {
		t.Fatalf("fsmeta duplicated regular data: %v %v", st, err)
	}
	// Rebuild this broader view using only authenticated caches, including
	// the first future payload after its original target name was replaced.
	sourceFile, err := host.Blob(layout, m.Digest)
	if err != nil {
		t.Fatal(err)
	}
	var sourceManifest v1.Manifest
	if err = host.ReadBlobJSON(sourceFile, m.Digest, &sourceManifest); err != nil {
		t.Fatal(err)
	}
	var layers []fixtures.OCILayerFixture
	for i, layerDigest := range m.Layers {
		p := filepath.Join(layout, "blobs", "sha256", layerDigest[7:])
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		layers = append(
			layers,
			fixtures.OCILayerFixture{
				Diff:       ocidigest.Digest(m.DiffIDs[i]),
				Descriptor: sourceManifest.Layers[i],
			},
		)
	}
	second := fixtures.PutOCIManifest(t, layout, "cache-only-shared", layers)
	rebuilt, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   second,
			MaxBytes: 8 << 20,
			MaxFiles: 1000,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(m.Artifacts); i++ {
		if m.Artifacts[i] != rebuilt.Artifacts[i] {
			t.Fatal("broader OCI semantics changed per-diff cache identity")
		}
	}
	rebuiltRoot := fixtures.ExtractManifest(t, e, rebuilt)
	if got, err := os.ReadFile(filepath.Join(rebuiltRoot, "forward-a")); err != nil || string(got) != payload {
		t.Fatalf("cache-only forward link lost data: %v", err)
	}
}

func TestActualSharedOCIRootOpaqueAndReplacement(t *testing.T) {
	t.Parallel()
	base := append(fixtures.BootLayerEntries(), fixtures.FileEntry("hidden", "hidden"))
	top := append([]fixtures.TarEntry{
		fixtures.FileEntry(".wh..wh..opq", ""),
	}, fixtures.BootLayerEntries()...)
	top = append(top, fixtures.FileEntry("visible", "visible"))
	e, _, m := fixtures.ImportSharedFixture(t, base, top)
	root := fixtures.ExtractManifest(t, e, m)
	if _, err := os.Stat(filepath.Join(root, "hidden")); !os.IsNotExist(err) {
		t.Fatalf("root opaque did not hide lower child: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "visible")); err != nil || string(got) != "visible" {
		t.Fatalf("root opaque hid same-layer entry: %v", err)
	}
}

func TestActualSharedOCIRejectsUnresolvedLinksAndSymlinkParents(t *testing.T) {
	t.Parallel()
	fixtures.RequireEROFS(t)
	for name, top := range map[string][]fixtures.TarEntry{
		"missing":        {fixtures.HardEntry("alias", "missing")},
		"cycle":          {fixtures.HardEntry("first", "second"), fixtures.HardEntry("second", "first")},
		"directory":      {fixtures.HardEntry("alias", "boot")},
		"self directory": {fixtures.HardEntry("boot", "boot")},
		"symlink parent": {{
			Header: tar.Header{
				Name:     "parent",
				Typeflag: tar.TypeSymlink,
				Linkname: "/outside",
				Mode:     0o777,
			},
		}, fixtures.FileEntry("parent/escaped", "refused")},
	} {
		t.Run(name, func(t *testing.T) {
			layout := t.TempDir()
			layers := []fixtures.OCILayerFixture{
				fixtures.PutOCILayer(t, layout, fixtures.BootLayerEntries(), true),
				fixtures.PutOCILayer(t, layout, top, false),
			}
			image := fixtures.PutOCIManifest(t, layout, name, layers)
			e := fixtures.NewNativeImageFixture(t)
			defer e.Close()
			if _, err := e.ImportOCI(
				context.Background(),
				imageimport.ImageImportOptions{
					Layout:   layout,
					Digest:   image,
					MaxBytes: 4 << 20,
					MaxFiles: 1000,
				},
			); err == nil {
				t.Fatal("invalid namespace was published")
			}
			var count int
			if err := e.DB.QueryRow("SELECT count(*) FROM images WHERE digest=?", image).
				Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed OCI import has catalog entry: %d %v", count, err)
			}
		})
	}
}

//nolint:staticcheck // Binary xattrs must survive deterministic tar cache serialization.
func TestActualSharedOCIDeterministicCachesAndMetadata(t *testing.T) {
	t.Parallel()
	entry := fixtures.FileEntry("payload", strings.Repeat("payload", 4096))
	entry.Header.Xattrs = map[string]string{
		"user.z":      "z",
		"user.binary": "\x00\xff\x80",
		"user.a":      "a",
	}
	entry.Header.Format = tar.FormatPAX
	entries := append(fixtures.BootLayerEntries(), fixtures.DirEntry("."), entry)
	_, _, first := fixtures.ImportSharedFixture(t, entries)
	_, _, second := fixtures.ImportSharedFixture(t, entries)
	if first.Builder != second.Builder || !reflect.DeepEqual(first.Files, second.Files) ||
		!reflect.DeepEqual(first.Artifacts, second.Artifacts) {
		t.Fatal("identical source/config/profile produced different immutable output")
	}
}

func TestActualLegacyNativeLayeredCatalogStillVerifies(t *testing.T) {
	t.Parallel()
	fixtures.RequireEROFS(t)
	ctx := context.Background()
	tools, err := host.ImageTools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	delete(tools, "go-erofs")
	builder := host.ImageBuilder(tools)
	layout := t.TempDir()
	layer := fixtures.PutOCILayer(t, layout, fixtures.BootLayerEntries(), false)
	image := fixtures.PutOCIManifest(t, layout, "legacy-native", []fixtures.OCILayerFixture{
		layer,
	})
	e := fixtures.NewNativeImageFixture(t)
	defer e.Close()
	input := filepath.Join(layout, "blobs", "sha256", layer.Descriptor.Digest.Encoded())
	n, count, err := host.ValidateLayer(host.LayerInput{
		Path:      input,
		MediaType: layer.Descriptor.MediaType,
		DiffID:    layer.Diff.String(),
	}, host.LayerLimits{
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := host.BuildLayerCache(ctx, e.Opts.StateDir, host.LayerCacheRequest{
		Input: host.LayerInput{
			Path:      input,
			MediaType: layer.Descriptor.MediaType,
			DiffID:    layer.Diff.String(),
		},
		Builder: builder,
		Bytes:   n,
		Entries: count,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := cache.Artifact(e.Opts.StateDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(e.Opts.StateDir, "images", image[7:])
	if err = os.MkdirAll(out, 0o700); err != nil {
		t.Fatal(err)
	}
	if err = host.MaterializeFsmeta(ctx, host.FsmetaLayers{
		StateRoot: e.Opts.StateDir,
		Output:    out,
		Artifacts: []host.ImageArtifact{
			artifact,
		},
	}, image); err != nil {
		t.Fatal(err)
	}
	hashes, err := loom.HashTree(out)
	if err != nil {
		t.Fatal(err)
	}
	meta := host.ImageArtifact{
		Role:      "fsmeta",
		Path:      filepath.Join("images", image[7:], "fsmeta.erofs"),
		SHA256:    hashes["fsmeta.erofs"],
		GuestPath: "/sandbox/ro/fsmeta.erofs",
		Serial:    "sandbox-root",
	}
	m := host.ImageManifest{
		Format:       2,
		Layout:       host.LayeredEROFSLayout,
		Digest:       image,
		Architecture: runtime.GOARCH,
		Layers: []string{
			layer.Descriptor.Digest.String(),
		},
		DiffIDs: []string{
			layer.Diff.String(),
		},
		Tools:   tools,
		Files:   hashes,
		Builder: builder,
		Artifacts: []host.ImageArtifact{
			meta,
			artifact,
		},
	}
	if err = loom.WriteJSON(filepath.Join(out, "image.json"), m); err != nil {
		t.Fatal(err)
	}
	if err = loom.SealTree(out); err != nil {
		t.Fatal(err)
	}
	if _, err = e.DB.Exec("INSERT INTO images VALUES(?,?)", image, testfixture.Encode(m)); err != nil {
		t.Fatal(err)
	}
	if err = m.Verify(out, image); err != nil {
		t.Fatal(err)
	}
	reimported, err := e.ImportOCI(ctx, imageimport.ImageImportOptions{
		Layout: layout,
		Digest: image,
	})
	if err != nil || !reflect.DeepEqual(m, reimported) {
		t.Fatalf("old catalog identity changed: %v", err)
	}
	root := fixtures.ExtractManifest(t, e, reimported)
	if got, err := os.ReadFile(filepath.Join(root, "boot/vmlinux")); err != nil || string(got) != "kernel-fixture" {
		t.Fatalf("old catalog lost native contents: %v", err)
	}
}
