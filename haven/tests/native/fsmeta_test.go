package native

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/tests/fixtures"
	"golang.org/x/sys/unix"
)

func TestActualLayeredEROFSImportAndExtraction(t *testing.T) {
	t.Parallel()
	e, _, _, m := fixtures.ImportFixture(t)
	root := fixtures.ExtractManifest(t, e, m)
	for name, want := range map[string]string{
		"override":     "top",
		"opaque/new":   "new child",
		"boot/vmlinux": "kernel-fixture",
	} {
		got, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s: %q %v", name, got, err)
		}
	}
	for _, name := range []string{
		"removed",
		"opaque/old",
		".wh.removed",
		"opaque/.wh..wh..opq",
	} {
		if _, err := os.Lstat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("whiteout/opaque not applied: %s %v", name, err)
		}
	}
	a, _ := os.Stat(filepath.Join(root, "shared.bin"))
	b, _ := os.Stat(filepath.Join(root, "hard.bin"))
	if a == nil || b == nil || !os.SameFile(a, b) || a.Mode().Perm() != 0o640 || a.ModTime().Unix() != 1700000000 ||
		a.ModTime().Nanosecond() != 123456789 {
		t.Fatalf("same-layer hardlink/metadata not retained: %v %v", a, b)
	}
	buf := make([]byte, 1024)
	n, err := unix.Getxattr(filepath.Join(root, "shared.bin"), "user.fixture", buf)
	if err != nil || string(buf[:n]) != "retained" {
		t.Fatalf("xattr lost: %q %v", buf[:max(n, 0)], err)
	}
	meta := filepath.Join(e.Opts.StateDir, m.Artifacts[0].Path)
	info, err := exec.Command("dump.erofs", "--path=/shared.bin", meta).CombinedOutput()
	if err != nil || !strings.Contains(string(info), "Uid: 123   Gid: 456") ||
		!strings.Contains(string(info), "Layout: 4") {
		t.Fatalf("external chunk metadata/ownership: %s %v", info, err)
	}
	st, _ := os.Stat(meta)
	if st.Size() >= a.Size() {
		t.Fatal("fsmeta unexpectedly duplicates layer file data")
	}
	if serials, err := m.DataSerials(); err != nil || serials != "sandbox-data-0000,sandbox-data-0001" {
		t.Fatalf("serial order: %q %v", serials, err)
	}
	if arg, err := m.DataKernelParameter(); err != nil ||
		arg != "sandbox.data_serials=sandbox-data-0000,sandbox-data-0001" {
		t.Fatalf("kernel parameter: %q %v", arg, err)
	}
	for name, want := range map[string]string{
		"kernel":    "kernel-fixture",
		"initramfs": "initramfs-fixture",
	} {
		got, err := os.ReadFile(filepath.Join(filepath.Dir(meta), name))
		if err != nil || string(got) != want {
			t.Fatalf("extracted %s: %q %v", name, got, err)
		}
	}
}

func TestActualLayerCacheReuseRebuildAndCorruption(t *testing.T) {
	t.Parallel()
	e, layout, layers, first := fixtures.ImportFixture(t)
	cache := filepath.Join(e.Opts.StateDir, first.Artifacts[1].Path)
	before, _ := os.Stat(cache)
	for _, layer := range layers {
		if err := os.Remove(filepath.Join(layout, "blobs", "sha256", layer.Descriptor.Digest.Encoded())); err != nil {
			t.Fatal(err)
		}
	}
	// A new manifest rebuilds fsmeta from already authenticated EROFS layers.
	secondDigest := fixtures.PutOCIManifest(t, layout, "second", layers)
	second, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   secondDigest,
			MaxBytes: 4 << 20,
			MaxFiles: 1000,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(cache)
	if !os.SameFile(before, after) || first.Artifacts[1] != second.Artifacts[1] {
		t.Fatal("layer bytes/inode not shared across manifests")
	}
	root := fixtures.ExtractManifest(t, e, second)
	if got, _ := os.ReadFile(filepath.Join(root, "override")); string(got) != "top" {
		t.Fatal("cache-only rebuild lost layer order")
	}
	if _, err = e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   fixtures.PutOCIManifest(t, layout, "bounded", layers),
			MaxBytes: 1024,
			MaxFiles: 1000,
		},
	); err == nil {
		t.Fatal("cache bypassed import bounds")
	}
	if err = os.Chmod(cache, 0o644); err != nil {
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
	os.Chmod(cache, 0o444)
	if err = first.Verify(filepath.Join(e.Opts.StateDir, "images", first.Digest[7:]), first.Digest); err == nil {
		t.Fatal("cache corruption accepted by existing image")
	}
	if _, err = e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   fixtures.PutOCIManifest(t, layout, "corrupt", layers),
			MaxBytes: 4 << 20,
			MaxFiles: 1000,
		},
	); err == nil {
		t.Fatal("corrupt cache silently rebuilt")
	}
}

func TestActualLayeredManifestVerification(t *testing.T) {
	t.Parallel()
	e, _, _, original := fixtures.ImportFixture(t)
	dir := filepath.Join(e.Opts.StateDir, "images", original.Digest[7:])
	for name, mutate := range map[string]func(*host.ImageManifest){
		"format": func(m *host.ImageManifest) {
			m.Format = 1
		},
		"layout": func(m *host.ImageManifest) {
			m.Layout = "merged-erofs-v1"
		},
		"builder": func(m *host.ImageManifest) {
			m.Builder = "sha256:" + strings.Repeat("0", 64)
		},
		"missing artifact": func(m *host.ImageManifest) {
			m.Artifacts = m.Artifacts[:2]
		},
		"wrong order": func(m *host.ImageManifest) {
			m.Artifacts[1], m.Artifacts[2] = m.Artifacts[2], m.Artifacts[1]
		},
		"wrong serial": func(m *host.ImageManifest) {
			m.Artifacts[1].Serial = "sandbox-data-0001"
		},
		"wrong hash": func(m *host.ImageManifest) {
			m.Artifacts[1].SHA256 = "sha256:" + strings.Repeat("0", 64)
		},
		"extra file": func(m *host.ImageManifest) {
			m.Files["ignored"] = m.Files["kernel"]
		},
		"wrong receipt": func(m *host.ImageManifest) {
			m.Artifacts[1].CacheSHA256 = m.Files["kernel"]
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := fixtures.CloneImageManifest(t, original)
			mutate(&m)
			if err := m.Verify(dir, original.Digest); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
	m := fixtures.CloneImageManifest(t, original)
	m.DiffIDs[0], m.DiffIDs[1] = m.DiffIDs[1], m.DiffIDs[0]
	m.Layers[0], m.Layers[1] = m.Layers[1], m.Layers[0]
	for i := range 2 {
		a := original.Artifacts[2-i]
		m.Artifacts[i+1] = host.DataArtifact(host.LayerArtifactIdentity{
			Builder:     m.Builder,
			DiffID:      m.DiffIDs[i],
			SHA256:      a.SHA256,
			CacheSHA256: a.CacheSHA256,
		}, i)
	}
	if err := m.Verify(dir, original.Digest); err == nil {
		t.Fatal("manifest reordered without rebuilding device_table accepted")
	}
	meta := filepath.Join(dir, "fsmeta.erofs")
	if err := os.Chmod(meta, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(meta, make([]byte, 4096), 0o444); err != nil {
		t.Fatal(err)
	}
	os.Chmod(meta, 0o444)
	m = fixtures.CloneImageManifest(t, original)
	m.Files["fsmeta.erofs"], _ = loom.DigestFile(meta)
	m.Artifacts[0].SHA256 = m.Files["fsmeta.erofs"]
	if err := m.Verify(dir, original.Digest); err == nil {
		t.Fatal("meaningless fsmeta accepted with a matching content hash")
	}
}

func TestActualLayerOrderAndBackendLimit(t *testing.T) {
	t.Parallel()
	e, layout, layers, first := fixtures.ImportFixture(t)
	reversed := []fixtures.OCILayerFixture{
		layers[1],
		layers[0],
	}
	image := fixtures.PutOCIManifest(t, layout, "reversed", reversed)
	m, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   image,
			MaxBytes: 4 << 20,
			MaxFiles: 1000,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	root := fixtures.ExtractManifest(t, e, m)
	if got, _ := os.ReadFile(filepath.Join(root, "override")); string(got) != "base" {
		t.Fatal("reversing layers did not reverse override precedence")
	}
	if got, _ := os.ReadFile(filepath.Join(root, "removed")); string(got) != "remove me" {
		t.Fatal("lower whiteout masked a newer upper file")
	}
	if m.Artifacts[1].Path != first.Artifacts[2].Path || m.Artifacts[2].Path != first.Artifacts[1].Path {
		t.Fatal("device_table order does not track OCI order")
	}
	many := make([]fixtures.OCILayerFixture, host.MaxImageDataDevices)
	for i := range many {
		many[i] = layers[0]
	}
	image = fixtures.PutOCIManifest(t, layout, "sixteen", many)
	m, err = e.ImportOCI(
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
	bindings, err := m.ReadOnlyBindings(filepath.Join(e.Opts.StateDir, "images", image[7:]))
	if err != nil || len(bindings) != 17 || bindings[16].Serial != "sandbox-data-0015" {
		t.Fatalf("16-backend boundary: %d %v", len(bindings), err)
	}
	image = fixtures.PutOCIManifest(t, layout, "seventeen", append(many, layers[0]))
	if _, err = e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   image,
			MaxBytes: 8 << 20,
			MaxFiles: 1000,
		},
	); err == nil {
		t.Fatal("more than 16 data backends accepted")
	}
}

func TestActualMissingAndUnsafeArtifacts(t *testing.T) {
	t.Parallel()
	e, layout, layers, m := fixtures.ImportFixture(t)
	dir := filepath.Join(e.Opts.StateDir, "images", m.Digest[7:])
	for _, rel := range []string{
		m.Artifacts[0].Path,
		m.Artifacts[1].Path,
		filepath.Join(filepath.Dir(m.Artifacts[1].Path), "layer.json"),
	} {
		file := filepath.Join(e.Opts.StateDir, rel)
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
	cacheDir := filepath.Dir(filepath.Join(e.Opts.StateDir, m.Artifacts[1].Path))
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
	image := fixtures.PutOCIManifest(t, layout, "missing receipt", layers)
	if _, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   image,
			MaxBytes: 4 << 20,
			MaxFiles: 1000,
		},
	); err == nil {
		t.Fatal("existing cache with missing receipt treated as a cache miss")
	}
}
