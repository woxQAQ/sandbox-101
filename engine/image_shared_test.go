package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"testing/fstest"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func importSharedFixture(
	t *testing.T,
	layers ...[]tarEntry,
) (*Engine, string, ImageManifest) {
	t.Helper()
	requireEROFS(t)
	layout := t.TempDir()
	var fixtures []ociLayerFixture
	for i, entries := range layers {
		fixtures = append(fixtures, putOCILayer(t, layout, entries, i%2 == 0))
	}
	image := putOCIManifest(t, layout, "shared", fixtures)
	e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
	t.Cleanup(func() { e.Close() })
	m, err := e.ImportOCI(
		context.Background(),
		ImportOptions{
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

func TestActualSharedOCIEntryOrderingAndForwardLinks(t *testing.T) {
	base := append(
		bootLayerEntries(),
		fileEntry("target", "old inode"),
		fileEntry("duplicate", "lower"),
		dirEntry("directory"),
		fileEntry("directory/hidden", "hidden"),
		fileEntry("file-to-directory", "old file"),
		dirEntry("opaque"),
		fileEntry("opaque/hidden", "hidden"),
	)
	sym := tarEntry{
		header: tar.Header{
			Name:     "symbolic",
			Typeflag: tar.TypeSymlink,
			Linkname: "target",
			Mode:     0o777,
			Uid:      123,
			Gid:      456,
		},
	}
	payload := strings.Repeat("shared-payload", 32768)
	top := []tarEntry{
		hardEntry("old-alias", "target"),
		fileEntry("target", "new inode"),
		hardEntry(
			"forward-a",
			"forward-b",
		),
		hardEntry("forward-b", "future"),
		fileEntry("future", payload),
		fileEntry("future", "replacement"),
		fileEntry(
			"duplicate",
			"first",
		),
		hardEntry("duplicate-old", "duplicate"),
		fileEntry("duplicate", "last"),
		sym,
		hardEntry("symbolic-alias", "symbolic"),
		fileEntry(
			"directory",
			"now a file",
		),
		dirEntry("file-to-directory"),
		fileEntry("file-to-directory/child", "child"),
		fileEntry("opaque/.wh..wh..opq", ""),
		fileEntry("opaque/new", "new"),
		fileEntry(
			"whiteout-before",
			"old",
		),
		fileEntry(".wh.whiteout-before", ""),
		fileEntry(".wh.whiteout-after", ""),
		fileEntry("whiteout-after", "new"),
		fileEntry("implicit/deep/file", "implicit"),
		hardEntry(
			"replaced-parent/alias",
			"late",
		),
		fileEntry("replaced-parent", "replacement"),
		fileEntry("late", "late target"),
		hardEntry(
			"overwritten-alias",
			"never-created",
		),
		fileEntry("overwritten-alias", "replacement"),
	}
	e, layout, m := importSharedFixture(t, base, top)
	root := extractManifest(t, e, m)
	for n, want := range map[string]string{
		"target": "new inode", "old-alias": "old inode", "forward-a": payload, "forward-b": payload, "future": "replacement",
		"duplicate": "last", "duplicate-old": "first", "directory": "now a file", "file-to-directory/child": "child",
		"opaque/new": "new", "whiteout-before": "old", "whiteout-after": "new", "implicit/deep/file": "implicit",
		"replaced-parent": "replacement", "late": "late target", "overwritten-alias": "replacement",
	} {
		got, err := os.ReadFile(filepath.Join(root, n))
		if err != nil || string(got) != want {
			t.Fatalf("%s: bytes=%d error=%v", n, len(got), err)
		}
	}
	for _, pair := range [][2]string{{"forward-a", "forward-b"}, {"symbolic", "symbolic-alias"}} {
		a, err := os.Lstat(filepath.Join(root, pair[0]))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.Lstat(filepath.Join(root, pair[1]))
		if err != nil || !os.SameFile(a, b) {
			t.Fatalf("hardlink inode identity lost: %v %v", pair, err)
		}
	}
	for _, n := range []string{"opaque/hidden", ".wh.whiteout-before", ".wh.whiteout-after"} {
		if _, err := os.Lstat(filepath.Join(root, n)); !os.IsNotExist(err) {
			t.Fatalf("hidden OCI entry remains: %s %v", n, err)
		}
	}
	for _, n := range []string{"implicit", "implicit/deep"} {
		if info, err := os.Stat(
			filepath.Join(root, n),
		); err != nil ||
			!info.IsDir() {
			t.Fatalf("implicit directory missing: %s %v", n, err)
		}
	}
	if st, err := os.Stat(
		filepath.Join(e.opts.StateDir, m.Artifacts[0].Path),
	); err != nil ||
		st.Size() >= int64(len(payload))/4 {
		t.Fatalf("fsmeta duplicated regular data: %v %v", st, err)
	}
	// Rebuild this broader view using only authenticated caches, including
	// the first future payload after its original target name was replaced.
	sourceFile, err := blob(layout, m.Digest)
	if err != nil {
		t.Fatal(err)
	}
	var sourceManifest v1.Manifest
	if err = readBlobJSON(sourceFile, m.Digest, &sourceManifest); err != nil {
		t.Fatal(err)
	}
	var layers []ociLayerFixture
	for i, layerDigest := range m.Layers {
		p := filepath.Join(layout, "blobs", "sha256", layerDigest[7:])
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		layers = append(
			layers,
			ociLayerFixture{
				diff:       digest.Digest(m.DiffIDs[i]),
				descriptor: sourceManifest.Layers[i],
			},
		)
	}
	second := putOCIManifest(t, layout, "cache-only-shared", layers)
	rebuilt, err := e.ImportOCI(
		context.Background(),
		ImportOptions{
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
	rebuiltRoot := extractManifest(t, e, rebuilt)
	if got, err := os.ReadFile(
		filepath.Join(rebuiltRoot, "forward-a"),
	); err != nil ||
		string(got) != payload {
		t.Fatalf("cache-only forward link lost data: %v", err)
	}
}

func TestActualSharedOCIRootOpaqueAndReplacement(t *testing.T) {
	base := append(bootLayerEntries(), fileEntry("hidden", "hidden"))
	top := append(
		[]tarEntry{fileEntry(".wh..wh..opq", "")},
		bootLayerEntries()...)
	top = append(top, fileEntry("visible", "visible"))
	e, _, m := importSharedFixture(t, base, top)
	root := extractManifest(t, e, m)
	if _, err := os.Stat(filepath.Join(root, "hidden")); !os.IsNotExist(err) {
		t.Fatalf("root opaque did not hide lower child: %v", err)
	}
	if got, err := os.ReadFile(
		filepath.Join(root, "visible"),
	); err != nil ||
		string(got) != "visible" {
		t.Fatalf("root opaque hid same-layer entry: %v", err)
	}
}

func TestActualSharedOCIRejectsUnresolvedLinksAndSymlinkParents(t *testing.T) {
	requireEROFS(t)
	for name, top := range map[string][]tarEntry{
		"missing":        {hardEntry("alias", "missing")},
		"cycle":          {hardEntry("first", "second"), hardEntry("second", "first")},
		"directory":      {hardEntry("alias", "boot")},
		"self directory": {hardEntry("boot", "boot")},
		"symlink parent": {{header: tar.Header{Name: "parent", Typeflag: tar.TypeSymlink, Linkname: "/outside", Mode: 0o777}}, fileEntry("parent/escaped", "refused")},
	} {
		t.Run(name, func(t *testing.T) {
			layout := t.TempDir()
			layers := []ociLayerFixture{
				putOCILayer(t, layout, bootLayerEntries(), true),
				putOCILayer(t, layout, top, false),
			}
			image := putOCIManifest(t, layout, name, layers)
			e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
			defer e.Close()
			if _, err := e.ImportOCI(
				context.Background(),
				ImportOptions{
					Layout:   layout,
					Digest:   image,
					MaxBytes: 4 << 20,
					MaxFiles: 1000,
				},
			); err == nil {
				t.Fatal("invalid namespace was published")
			}
			var count int
			if err := e.db.QueryRow("SELECT count(*) FROM meta WHERE key=?", "image:"+image).
				Scan(&count); err != nil ||
				count != 0 {
				t.Fatalf(
					"failed OCI import has catalog entry: %d %v",
					count,
					err,
				)
			}
		})
	}
}

//nolint:staticcheck // Binary xattrs must survive deterministic tar cache serialization.
func TestActualSharedOCIDeterministicCachesAndMetadata(t *testing.T) {
	entry := fileEntry("payload", strings.Repeat("payload", 4096))
	entry.header.Xattrs = map[string]string{
		"user.z":      "z",
		"user.binary": "\x00\xff\x80",
		"user.a":      "a",
	}
	entry.header.Format = tar.FormatPAX
	entries := append(bootLayerEntries(), dirEntry("."), entry)
	_, _, first := importSharedFixture(t, entries)
	_, _, second := importSharedFixture(t, entries)
	if first.Builder != second.Builder ||
		!reflect.DeepEqual(first.Files, second.Files) ||
		!reflect.DeepEqual(first.Artifacts, second.Artifacts) {
		t.Fatal(
			"identical source/config/profile produced different immutable output",
		)
	}
}

func TestActualLegacyNativeLayeredCatalogStillVerifies(t *testing.T) {
	requireEROFS(t)
	ctx := context.Background()
	tools, err := imageTools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	delete(tools, "go-erofs")
	builder := imageBuilder(tools)
	layout := t.TempDir()
	layer := putOCILayer(t, layout, bootLayerEntries(), false)
	image := putOCIManifest(
		t,
		layout,
		"legacy-native",
		[]ociLayerFixture{layer},
	)
	e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
	defer e.Close()
	input := filepath.Join(
		layout,
		"blobs",
		"sha256",
		layer.descriptor.Digest.Encoded(),
	)
	n, count, err := validateLayer(
		input,
		layer.descriptor.MediaType,
		layer.diff.String(),
		4<<20,
		1000,
	)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := buildLayerCache(
		ctx,
		e.opts.StateDir,
		input,
		layer.descriptor.MediaType,
		builder,
		layer.diff.String(),
		n,
		count,
	)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := cache.artifact(e.opts.StateDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(e.opts.StateDir, "images", image[7:])
	if err = os.MkdirAll(out, 0o700); err != nil {
		t.Fatal(err)
	}
	if err = materializeFsmeta(
		ctx,
		e.opts.StateDir,
		out,
		[]ImageArtifact{artifact},
		image,
	); err != nil {
		t.Fatal(err)
	}
	hashes, err := hashTree(out)
	if err != nil {
		t.Fatal(err)
	}
	meta := ImageArtifact{
		Role:      "fsmeta",
		Path:      filepath.Join("images", image[7:], "fsmeta.erofs"),
		SHA256:    hashes["fsmeta.erofs"],
		GuestPath: "/sandbox/ro/fsmeta.erofs",
		Serial:    "sandbox-root",
	}
	m := ImageManifest{
		Format:       2,
		Layout:       LayeredEROFSLayout,
		Digest:       image,
		Architecture: runtime.GOARCH,
		Layers:       []string{layer.descriptor.Digest.String()},
		DiffIDs:      []string{layer.diff.String()},
		Tools:        tools,
		Files:        hashes,
		Builder:      builder,
		Artifacts:    []ImageArtifact{meta, artifact},
	}
	if err = writeJSON(filepath.Join(out, "image.json"), m); err != nil {
		t.Fatal(err)
	}
	if err = sealTree(out); err != nil {
		t.Fatal(err)
	}
	if _, err = e.db.Exec(
		"INSERT INTO meta VALUES(?,?)",
		"image:"+image,
		string(encode(m)),
	); err != nil {
		t.Fatal(err)
	}
	if err = m.Verify(out, image); err != nil {
		t.Fatal(err)
	}
	reimported, err := e.ImportOCI(
		ctx,
		ImportOptions{Layout: layout, Digest: image},
	)
	if err != nil || !reflect.DeepEqual(m, reimported) {
		t.Fatalf("old catalog identity changed: %v", err)
	}
	root := extractManifest(t, e, reimported)
	if got, err := os.ReadFile(
		filepath.Join(root, "boot/vmlinux"),
	); err != nil ||
		string(got) != "kernel-fixture" {
		t.Fatalf("old catalog lost native contents: %v", err)
	}
}

//nolint:staticcheck // Reject mixed deprecated xattr fields in the persisted cache format.
func TestSharedCachedIndexBoundsAndValidation(t *testing.T) {
	valid := cacheHeader(fileEntry("safe", "data").header)
	for name, mutate := range map[string]func(*cachedHeader){
		"unsafe path":        func(h *cachedHeader) { h.Header.Name = "../escape" },
		"special file":       func(h *cachedHeader) { h.Header.Typeflag = tar.TypeChar },
		"invalid attributes": func(h *cachedHeader) { h.Xattrs = []headerAttribute{{"user.z", "z"}, {"user.a", "a"}} },
		"mixed attributes":   func(h *cachedHeader) { h.Header.Xattrs = map[string]string{"user.x": "x"} },
	} {
		t.Run(name, func(t *testing.T) {
			h := valid
			mutate(&h)
			var data bytes.Buffer
			if err := gob.NewEncoder(&data).
				Encode([]cachedHeader{h}); err != nil {
				t.Fatal(err)
			}
			if _, err := sharedHeaders(
				fstest.MapFS{
					"headers.gob": &fstest.MapFile{Data: data.Bytes()},
				},
				layerCache{Bytes: 4096, Entries: 1},
			); err == nil {
				t.Fatal("invalid cached operations accepted")
			}
		})
	}
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := enc.Encode([]cachedHeader{valid}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode("extra object"); err != nil {
		t.Fatal(err)
	}
	if _, err := sharedHeaders(
		fstest.MapFS{"headers.gob": &fstest.MapFile{Data: data.Bytes()}},
		layerCache{Bytes: 4096, Entries: 1},
	); err == nil {
		t.Fatal("trailing cached index accepted")
	}
	if _, err := sharedHeaders(
		fstest.MapFS{"headers.gob": &fstest.MapFile{Data: make([]byte, 65537)}},
		layerCache{},
	); err == nil {
		t.Fatal("unbounded cached index accepted")
	}
}
