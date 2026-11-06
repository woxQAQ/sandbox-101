package engine

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

func requireEROFS(t *testing.T) {
	t.Helper()
	fail := func(err error) {
		t.Helper()
		if os.Getenv("SANDBOX_REQUIRE_EROFS_TESTS") == "1" {
			t.Fatal(err)
		}
		t.Skipf("actual EROFS tool test unavailable: %v", err)
	}
	if _, err := imageTools(context.Background()); err != nil {
		fail(err)
	}
	if _, err := exec.LookPath("dump.erofs"); err != nil {
		fail(err)
	}
	if err := runImageTool(context.Background(), t.TempDir(), nil, "mkfs.erofs", "--version"); err != nil {
		fail(err)
	}
}

type tarEntry struct {
	header tar.Header
	data   string
}

func dirEntry(name string) tarEntry {
	return tarEntry{header: tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: 0750, Uid: 123, Gid: 456, ModTime: time.Unix(1700000000, 0)}}
}
func fileEntry(name, data string) tarEntry {
	return tarEntry{header: tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0640, Uid: 123, Gid: 456, ModTime: time.Unix(1700000000, 0)}, data: data}
}
func hardEntry(name, target string) tarEntry {
	e := fileEntry(name, "")
	e.header.Typeflag, e.header.Linkname = tar.TypeLink, target
	return e
}
func tarEntries(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	for _, entry := range entries {
		h := entry.header
		h.Size = int64(len(entry.data))
		if err := w.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(entry.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func putOCIBlob(t *testing.T, layout string, b []byte, media string) v1.Descriptor {
	t.Helper()
	d := digest.FromBytes(b)
	p := filepath.Join(layout, "blobs", "sha256", d.Encoded())
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	return v1.Descriptor{MediaType: media, Digest: d, Size: int64(len(b))}
}

type ociLayerFixture struct {
	descriptor v1.Descriptor
	diff       digest.Digest
}

func putOCILayer(t *testing.T, layout string, entries []tarEntry, compressed bool) ociLayerFixture {
	t.Helper()
	raw := tarEntries(t, entries)
	data, media := raw, v1.MediaTypeImageLayer
	if compressed {
		var b bytes.Buffer
		g := gzip.NewWriter(&b)
		if _, err := g.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		data, media = b.Bytes(), v1.MediaTypeImageLayerGzip
	}
	return ociLayerFixture{putOCIBlob(t, layout, data, media), digest.FromBytes(raw)}
}
func putOCIManifest(t *testing.T, layout, variant string, layers []ociLayerFixture) string {
	t.Helper()
	cfg := v1.Image{Platform: v1.Platform{Architecture: runtime.GOARCH, OS: "linux"}, Config: v1.ImageConfig{Labels: map[string]string{"fixture": variant}}, RootFS: v1.RootFS{Type: "layers"}}
	manifest := v1.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageManifest}
	for _, layer := range layers {
		cfg.RootFS.DiffIDs = append(cfg.RootFS.DiffIDs, layer.diff)
		manifest.Layers = append(manifest.Layers, layer.descriptor)
	}
	b, _ := json.Marshal(cfg)
	manifest.Config = putOCIBlob(t, layout, b, v1.MediaTypeImageConfig)
	b, _ = json.Marshal(manifest)
	desc := putOCIBlob(t, layout, b, v1.MediaTypeImageManifest)
	if err := writeJSON(filepath.Join(layout, "oci-layout"), v1.ImageLayout{Version: "1.0.0"}); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(layout, "index.json"), v1.Index{Versioned: specs.Versioned{SchemaVersion: 2}, Manifests: []v1.Descriptor{desc}}); err != nil {
		t.Fatal(err)
	}
	return desc.Digest.String()
}

func bootLayerEntries() []tarEntry {
	return []tarEntry{
		dirEntry("boot"), fileEntry("boot/vmlinux", "kernel-fixture"), fileEntry("boot/initramfs", "initramfs-fixture"),
		dirEntry("usr"), dirEntry("usr/lib"), dirEntry("usr/lib/systemd"), fileEntry("usr/lib/systemd/systemd", "systemd-fixture"),
		dirEntry("usr/local"), dirEntry("usr/local/libexec"), fileEntry("usr/local/libexec/sandbox-agent", "agent-fixture"),
		dirEntry("etc"), dirEntry("etc/systemd"), dirEntry("etc/systemd/system"), dirEntry("etc/systemd/system/sysinit.target.wants"), fileEntry("etc/systemd/system/sysinit.target.wants/sandbox-agent.service", "service-fixture"),
	}
}

func importFixture(t *testing.T) (*Engine, string, []ociLayerFixture, ImageManifest) {
	t.Helper()
	requireEROFS(t)
	layout := t.TempDir()
	base := append(bootLayerEntries(), fileEntry("override", "base"), fileEntry("removed", "remove me"), dirEntry("opaque"), fileEntry("opaque/old", "hide me"), fileEntry("shared.bin", strings.Repeat("shared-content", 8192)), hardEntry("hard.bin", "shared.bin"))
	base[len(base)-2].header.Xattrs = map[string]string{"user.fixture": "retained"}
	base[len(base)-2].header.Format = tar.FormatPAX
	base[len(base)-2].header.ModTime = time.Unix(1700000000, 123456789)
	top := []tarEntry{fileEntry("override", "top"), fileEntry(".wh.removed", ""), dirEntry("opaque"), fileEntry("opaque/.wh..wh..opq", ""), fileEntry("opaque/new", "new child")}
	layers := []ociLayerFixture{putOCILayer(t, layout, base, true), putOCILayer(t, layout, top, false)}
	image := putOCIManifest(t, layout, "first", layers)
	e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
	t.Cleanup(func() { e.Close() })
	m, err := e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: image, MaxBytes: 4 << 20, MaxFiles: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return e, layout, layers, m
}

func extractManifest(t *testing.T, e *Engine, m ImageManifest) string {
	t.Helper()
	dir := filepath.Join(e.opts.StateDir, "images", m.Digest[7:])
	bindings, err := m.ReadOnlyBindings(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	var reads []imageMount
	args := []string{"--extract=/work/tree", "--xattrs", "--no-preserve-owner", "--preserve-perms"}
	for i, b := range bindings {
		reads = append(reads, imageMount{b.HostPath, b.GuestPath})
		if i != 0 {
			args = append(args, "--device="+b.GuestPath)
		}
	}
	args = append(args, bindings[0].GuestPath)
	if err = runImageTool(context.Background(), out, reads, "fsck.erofs", args...); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(out, "tree")
}

func TestActualLayeredEROFSImportAndExtraction(t *testing.T) {
	e, _, _, m := importFixture(t)
	root := extractManifest(t, e, m)
	for name, want := range map[string]string{"override": "top", "opaque/new": "new child", "boot/vmlinux": "kernel-fixture"} {
		got, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s: %q %v", name, got, err)
		}
	}
	for _, name := range []string{"removed", "opaque/old", ".wh.removed", "opaque/.wh..wh..opq"} {
		if _, err := os.Lstat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("whiteout/opaque not applied: %s %v", name, err)
		}
	}
	a, _ := os.Stat(filepath.Join(root, "shared.bin"))
	b, _ := os.Stat(filepath.Join(root, "hard.bin"))
	if a == nil || b == nil || !os.SameFile(a, b) || a.Mode().Perm() != 0640 || a.ModTime().Unix() != 1700000000 || a.ModTime().Nanosecond() != 123456789 {
		t.Fatalf("same-layer hardlink/metadata not retained: %v %v", a, b)
	}
	buf := make([]byte, 1024)
	n, err := unix.Getxattr(filepath.Join(root, "shared.bin"), "user.fixture", buf)
	if err != nil || string(buf[:n]) != "retained" {
		t.Fatalf("xattr lost: %q %v", buf[:max(n, 0)], err)
	}
	meta := filepath.Join(e.opts.StateDir, m.Artifacts[0].Path)
	info, err := exec.Command("dump.erofs", "--path=/shared.bin", meta).CombinedOutput()
	if err != nil || !strings.Contains(string(info), "Uid: 123   Gid: 456") || !strings.Contains(string(info), "Layout: 4") {
		t.Fatalf("external chunk metadata/ownership: %s %v", info, err)
	}
	st, _ := os.Stat(meta)
	if st.Size() >= a.Size() {
		t.Fatal("fsmeta unexpectedly duplicates layer file data")
	}
	if serials, err := m.DataSerials(); err != nil || serials != "sandbox-data-0000,sandbox-data-0001" {
		t.Fatalf("serial order: %q %v", serials, err)
	}
	if arg, err := m.DataKernelParameter(); err != nil || arg != "sandbox.data_serials=sandbox-data-0000,sandbox-data-0001" {
		t.Fatalf("kernel parameter: %q %v", arg, err)
	}
	for name, want := range map[string]string{"kernel": "kernel-fixture", "initramfs": "initramfs-fixture"} {
		got, err := os.ReadFile(filepath.Join(filepath.Dir(meta), name))
		if err != nil || string(got) != want {
			t.Fatalf("extracted %s: %q %v", name, got, err)
		}
	}
}

func TestActualLayerCacheReuseRebuildAndCorruption(t *testing.T) {
	e, layout, layers, first := importFixture(t)
	cache := filepath.Join(e.opts.StateDir, first.Artifacts[1].Path)
	before, _ := os.Stat(cache)
	for _, layer := range layers {
		if err := os.Remove(filepath.Join(layout, "blobs", "sha256", layer.descriptor.Digest.Encoded())); err != nil {
			t.Fatal(err)
		}
	}
	// A new manifest rebuilds fsmeta from already authenticated EROFS layers.
	secondDigest := putOCIManifest(t, layout, "second", layers)
	second, err := e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: secondDigest, MaxBytes: 4 << 20, MaxFiles: 1000})
	if err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(cache)
	if !os.SameFile(before, after) || first.Artifacts[1] != second.Artifacts[1] {
		t.Fatal("layer bytes/inode not shared across manifests")
	}
	root := extractManifest(t, e, second)
	if got, _ := os.ReadFile(filepath.Join(root, "override")); string(got) != "top" {
		t.Fatal("cache-only rebuild lost layer order")
	}
	if _, err = e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: putOCIManifest(t, layout, "bounded", layers), MaxBytes: 1024, MaxFiles: 1000}); err == nil {
		t.Fatal("cache bypassed import bounds")
	}
	if err = os.Chmod(cache, 0644); err != nil {
		t.Fatal(err)
	}
	if f, x := os.OpenFile(cache, os.O_WRONLY|os.O_APPEND, 0); x != nil {
		t.Fatal(x)
	} else {
		_, err = f.Write([]byte("corruption"))
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	os.Chmod(cache, 0444)
	if err = first.Verify(filepath.Join(e.opts.StateDir, "images", first.Digest[7:]), first.Digest); err == nil {
		t.Fatal("cache corruption accepted by existing image")
	}
	if _, err = e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: putOCIManifest(t, layout, "corrupt", layers), MaxBytes: 4 << 20, MaxFiles: 1000}); err == nil {
		t.Fatal("corrupt cache silently rebuilt")
	}
}

func cloneImageManifest(t *testing.T, m ImageManifest) ImageManifest {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var cloned ImageManifest
	if err = json.Unmarshal(b, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func TestActualLayeredManifestVerification(t *testing.T) {
	e, _, _, original := importFixture(t)
	dir := filepath.Join(e.opts.StateDir, "images", original.Digest[7:])
	for name, mutate := range map[string]func(*ImageManifest){
		"format":           func(m *ImageManifest) { m.Format = 1 },
		"layout":           func(m *ImageManifest) { m.Layout = "merged-erofs-v1" },
		"builder":          func(m *ImageManifest) { m.Builder = "sha256:" + strings.Repeat("0", 64) },
		"missing artifact": func(m *ImageManifest) { m.Artifacts = m.Artifacts[:2] },
		"wrong order":      func(m *ImageManifest) { m.Artifacts[1], m.Artifacts[2] = m.Artifacts[2], m.Artifacts[1] },
		"wrong serial":     func(m *ImageManifest) { m.Artifacts[1].Serial = "sandbox-data-0001" },
		"wrong hash":       func(m *ImageManifest) { m.Artifacts[1].SHA256 = "sha256:" + strings.Repeat("0", 64) },
		"extra file":       func(m *ImageManifest) { m.Files["ignored"] = m.Files["kernel"] },
		"wrong receipt":    func(m *ImageManifest) { m.Artifacts[1].CacheSHA256 = m.Files["kernel"] },
	} {
		t.Run(name, func(t *testing.T) {
			m := cloneImageManifest(t, original)
			mutate(&m)
			if err := m.Verify(dir, original.Digest); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
	m := cloneImageManifest(t, original)
	m.DiffIDs[0], m.DiffIDs[1] = m.DiffIDs[1], m.DiffIDs[0]
	m.Layers[0], m.Layers[1] = m.Layers[1], m.Layers[0]
	for i := 0; i < 2; i++ {
		a := original.Artifacts[2-i]
		m.Artifacts[i+1] = dataArtifact(m.Builder, m.DiffIDs[i], a.SHA256, a.CacheSHA256, i)
	}
	if err := m.Verify(dir, original.Digest); err == nil {
		t.Fatal("manifest reordered without rebuilding device_table accepted")
	}
	meta := filepath.Join(dir, "fsmeta.erofs")
	if err := os.Chmod(meta, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(meta, make([]byte, 4096), 0444); err != nil {
		t.Fatal(err)
	}
	os.Chmod(meta, 0444)
	m = cloneImageManifest(t, original)
	m.Files["fsmeta.erofs"], _ = digestFile(meta)
	m.Artifacts[0].SHA256 = m.Files["fsmeta.erofs"]
	if err := m.Verify(dir, original.Digest); err == nil {
		t.Fatal("meaningless fsmeta accepted with a matching content hash")
	}
}

func TestActualLayerOrderAndBackendLimit(t *testing.T) {
	e, layout, layers, first := importFixture(t)
	reversed := []ociLayerFixture{layers[1], layers[0]}
	image := putOCIManifest(t, layout, "reversed", reversed)
	m, err := e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: image, MaxBytes: 4 << 20, MaxFiles: 1000})
	if err != nil {
		t.Fatal(err)
	}
	root := extractManifest(t, e, m)
	if got, _ := os.ReadFile(filepath.Join(root, "override")); string(got) != "base" {
		t.Fatal("reversing layers did not reverse override precedence")
	}
	if got, _ := os.ReadFile(filepath.Join(root, "removed")); string(got) != "remove me" {
		t.Fatal("lower whiteout masked a newer upper file")
	}
	if m.Artifacts[1].Path != first.Artifacts[2].Path || m.Artifacts[2].Path != first.Artifacts[1].Path {
		t.Fatal("device_table order does not track OCI order")
	}
	many := make([]ociLayerFixture, MaxImageDataDevices)
	for i := range many {
		many[i] = layers[0]
	}
	image = putOCIManifest(t, layout, "sixteen", many)
	m, err = e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: image, MaxBytes: 8 << 20, MaxFiles: 1000})
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := m.ReadOnlyBindings(filepath.Join(e.opts.StateDir, "images", image[7:]))
	if err != nil || len(bindings) != 17 || bindings[16].Serial != "sandbox-data-0015" {
		t.Fatalf("16-backend boundary: %d %v", len(bindings), err)
	}
	image = putOCIManifest(t, layout, "seventeen", append(many, layers[0]))
	if _, err = e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: image, MaxBytes: 8 << 20, MaxFiles: 1000}); err == nil {
		t.Fatal("more than 16 data backends accepted")
	}
}

func TestActualMissingAndUnsafeArtifacts(t *testing.T) {
	e, layout, layers, m := importFixture(t)
	dir := filepath.Join(e.opts.StateDir, "images", m.Digest[7:])
	for _, rel := range []string{m.Artifacts[0].Path, m.Artifacts[1].Path, filepath.Join(filepath.Dir(m.Artifacts[1].Path), "layer.json")} {
		file := filepath.Join(e.opts.StateDir, rel)
		moved := file + ".saved"
		if err := os.Rename(file, moved); err != nil {
			t.Fatal(err)
		}
		if err := m.Verify(dir, m.Digest); err == nil {
			t.Fatalf("missing artifact accepted: %s", rel)
		}
		if err := os.Symlink(moved, file); err != nil {
			t.Fatal(err)
		}
		if err := m.Verify(dir, m.Digest); err == nil {
			t.Fatalf("symlink artifact accepted: %s", rel)
		}
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(moved, file); err != nil {
			t.Fatal(err)
		}
	}
	cacheDir := filepath.Dir(filepath.Join(e.opts.StateDir, m.Artifacts[1].Path))
	if err := os.Rename(cacheDir, cacheDir+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(cacheDir+".saved", cacheDir); err != nil {
		t.Fatal(err)
	}
	if err := m.Verify(dir, m.Digest); err == nil {
		t.Fatal("cache directory symlink accepted")
	}
	os.Remove(cacheDir)
	if err := os.Rename(cacheDir+".saved", cacheDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(cacheDir, "layer.json")); err != nil {
		t.Fatal(err)
	}
	image := putOCIManifest(t, layout, "missing receipt", layers)
	if _, err := e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: image, MaxBytes: 4 << 20, MaxFiles: 1000}); err == nil {
		t.Fatal("existing cache with missing receipt treated as a cache miss")
	}
}

func TestLegacyNativeLayeredSubsetRemainsFrozen(t *testing.T) {
	cases := map[string][]tarEntry{
		"cross layer hardlink":        {hardEntry("alias", "lower")},
		"forward hardlink":            {hardEntry("alias", "target"), fileEntry("target", "data")},
		"root opaque":                 {fileEntry(".wh..wh..opq", "")},
		"root metadata":               {dirEntry(".")},
		"implicit parent metadata":    {fileEntry("parent/file", "data")},
		"duplicate path":              {fileEntry("file", "a"), fileEntry("file", "b")},
		"same layer whiteout":         {fileEntry("file", "a"), fileEntry(".wh.file", "")},
		"reverse same layer whiteout": {fileEntry(".wh.file", ""), fileEntry("file", "a")},
		"symlink hardlink":            {{header: tar.Header{Name: "sym", Typeflag: tar.TypeSymlink, Linkname: "target", Mode: 0777}}, hardEntry("hard", "sym")},
		"overlay xattr":               {{header: tar.Header{Name: "file", Typeflag: tar.TypeReg, Mode: 0644, Xattrs: map[string]string{"trusted.overlay.opaque": "y"}}}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			recorded, whiteouts := map[string]byte{}, map[string]bool{}
			for _, entry := range entries {
				if err := validateLayerEntry(&entry.header, recorded, whiteouts); err != nil {
					return
				}
			}
			t.Fatal("legacy native subset changed without a new profile")
		})
	}
}
