package testfixture

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

	ocidigest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

func RequireEROFS(t *testing.T) {
	t.Helper()
	fail := func(err error) {
		t.Helper()
		if os.Getenv("SANDBOX_REQUIRE_EROFS_TESTS") == "1" {
			t.Fatal(err)
		}
		t.Skipf("actual EROFS tool test unavailable: %v", err)
	}
	if _, err := host.ImageTools(context.Background()); err != nil {
		fail(err)
	}
	if _, err := exec.LookPath("dump.erofs"); err != nil {
		fail(err)
	}
	if err := host.RunImageTool(context.Background(), t.TempDir(), "mkfs.erofs", "--version"); err != nil {
		fail(err)
	}
}

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

type TarEntry struct {
	Header tar.Header
	data   string
}

func DirEntry(name string) TarEntry {
	return TarEntry{
		Header: tar.Header{
			Name:     name,
			Typeflag: tar.TypeDir,
			Mode:     0o750,
			Uid:      123,
			Gid:      456,
			ModTime:  time.Unix(1700000000, 0),
		},
	}
}

func FileEntry(name, data string) TarEntry {
	return TarEntry{
		Header: tar.Header{
			Name:     name,
			Typeflag: tar.TypeReg,
			Mode:     0o640,
			Uid:      123,
			Gid:      456,
			ModTime:  time.Unix(1700000000, 0),
		},
		data: data,
	}
}

func HardEntry(name, target string) TarEntry {
	e := FileEntry(name, "")
	e.Header.Typeflag, e.Header.Linkname = tar.TypeLink, target
	return e
}

func tarEntries(t *testing.T, entries []TarEntry) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	for _, entry := range entries {
		h := entry.Header
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

func PutOCIBlob(t *testing.T, layout string, b []byte, media string) v1.Descriptor {
	t.Helper()
	d := ocidigest.FromBytes(b)
	p := filepath.Join(layout, "blobs", "sha256", d.Encoded())
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return v1.Descriptor{
		MediaType: media,
		Digest:    d,
		Size:      int64(len(b)),
	}
}

type OCILayerFixture struct {
	Descriptor v1.Descriptor
	Diff       ocidigest.Digest
}

func PutOCILayer(t *testing.T, layout string, entries []TarEntry, compressed bool) OCILayerFixture {
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
	return OCILayerFixture{
		PutOCIBlob(t, layout, data, media),
		ocidigest.FromBytes(raw),
	}
}

func PutOCIManifest(t *testing.T, layout, variant string, layers []OCILayerFixture) string {
	t.Helper()
	cfg := v1.Image{
		Platform: v1.Platform{
			Architecture: runtime.GOARCH,
			OS:           "linux",
		},
		Config: v1.ImageConfig{
			Labels: map[string]string{
				"fixture": variant,
			},
		},
		RootFS: v1.RootFS{
			Type: "layers",
		},
	}
	manifest := v1.Manifest{
		Versioned: specs.Versioned{
			SchemaVersion: 2,
		},
		MediaType: v1.MediaTypeImageManifest,
	}
	for _, layer := range layers {
		cfg.RootFS.DiffIDs = append(cfg.RootFS.DiffIDs, layer.Diff)
		manifest.Layers = append(manifest.Layers, layer.Descriptor)
	}
	b, _ := json.Marshal(cfg)
	manifest.Config = PutOCIBlob(t, layout, b, v1.MediaTypeImageConfig)
	b, _ = json.Marshal(manifest)
	desc := PutOCIBlob(t, layout, b, v1.MediaTypeImageManifest)
	if err := loom.WriteJSON(filepath.Join(layout, "oci-layout"), v1.ImageLayout{
		Version: "1.0.0",
	}); err != nil {
		t.Fatal(err)
	}
	if err := loom.WriteJSON(
		filepath.Join(layout, "index.json"),
		v1.Index{
			Versioned: specs.Versioned{
				SchemaVersion: 2,
			},
			Manifests: []v1.Descriptor{
				desc,
			},
		},
	); err != nil {
		t.Fatal(err)
	}
	return desc.Digest.String()
}

func BootLayerEntries() []TarEntry {
	return []TarEntry{
		DirEntry("boot"),
		FileEntry("boot/vmlinux", "kernel-fixture"),
		FileEntry("boot/initramfs", "initramfs-fixture"),
		DirEntry("usr"),
		DirEntry("usr/lib"),
		DirEntry("usr/lib/systemd"),
		FileEntry("usr/lib/systemd/systemd", "systemd-fixture"),
		DirEntry("usr/local"),
		DirEntry("usr/local/libexec"),
		FileEntry("usr/local/libexec/sandbox-agent", "agent-fixture"),
		DirEntry("etc"),
		DirEntry("etc/systemd"),
		DirEntry("etc/systemd/system"),
		DirEntry("etc/systemd/system/sysinit.target.wants"),
		FileEntry("etc/systemd/system/sysinit.target.wants/sandbox-agent.service", "service-fixture"),
	}
}

//nolint:staticcheck // Exercise archive/tar's binary xattr encoding used by the OCI materializers.
func LayeredOCILayout(t *testing.T) (string, []OCILayerFixture, string) {
	t.Helper()
	layout := t.TempDir()
	base := append(
		BootLayerEntries(),
		FileEntry("override", "base"),
		FileEntry("removed", "remove me"),
		DirEntry("opaque"),
		FileEntry("opaque/old", "hide me"),
		FileEntry("shared.bin", strings.Repeat("shared-content", 8192)),
		HardEntry("hard.bin", "shared.bin"),
	)
	base[len(base)-2].Header.PAXRecords = map[string]string{
		"user.fixture": "retained",
	}
	base[len(base)-2].Header.Format = tar.FormatPAX
	base[len(base)-2].Header.ModTime = time.Unix(1700000000, 123456789)
	top := []TarEntry{
		FileEntry("override", "top"),
		FileEntry(".wh.removed", ""),
		DirEntry("opaque"),
		FileEntry("opaque/.wh..wh..opq", ""),
		FileEntry("opaque/new", "new child"),
	}
	layers := []OCILayerFixture{
		PutOCILayer(t, layout, base, true),
		PutOCILayer(t, layout, top, false),
	}
	image := PutOCIManifest(t, layout, "first", layers)
	return layout, layers, image
}

func ExtractManifest(t *testing.T, stateDir string, m host.ImageManifest) string {
	t.Helper()
	dir := filepath.Join(stateDir, "images", m.Digest[7:])
	bindings, err := m.ReadOnlyBindings(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	args := []string{
		"--extract=" + filepath.Join(out, "tree"),
		"--xattrs",
		"--no-preserve-owner",
		"--preserve-perms",
	}
	for i, b := range bindings {
		if i != 0 {
			args = append(args, "--device="+b.HostPath)
		}
	}
	args = append(args, bindings[0].HostPath)
	if err = host.RunImageTool(context.Background(), out, "fsck.erofs", args...); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(out, "tree")
}
