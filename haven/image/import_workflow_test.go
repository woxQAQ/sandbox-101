package image_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	ocidigest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/tests/fixtures"
	"github.com/woxQAQ/haven/types"
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

func TestActualOCIUsesPrivateWorkDirectory(t *testing.T) {
	fixtures.RequireMergedOCI(t)
	layout := filepath.Join(t.TempDir(), "layout with spaces")
	if err := os.Mkdir(layout, 0o700); err != nil {
		t.Fatal(err)
	}
	layer := fixtures.PutOCILayer(t, layout, fixtures.BootLayerEntries(), false)
	image := fixtures.PutOCIManifest(t, layout, "host-tools", []fixtures.OCILayerFixture{
		layer,
	})
	for _, materialization := range []string{
		host.LayeredEROFSLayout,
		host.MergedOCIEROFSLayout,
	} {
		t.Run(materialization, func(t *testing.T) {
			e := fixtures.NativeImageFixtureAt(t, filepath.Join(t.TempDir(), "state with spaces"))
			manifest, err := e.ImportOCI(context.Background(), imageimport.ImageImportOptions{
				Layout:          layout,
				Digest:          image,
				Materialization: materialization,
				MaxBytes:        4 << 20,
				MaxFiles:        1000,
			})
			if err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(e.Opts.StateDir, "images", image[7:])
			if err := manifest.Verify(directory, image); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".unpack-") {
					t.Fatal("published image retains private unpack work")
				}
			}
		})
	}
}

func TestActualMergedOCIHardlinksAndMetadata(t *testing.T) {
	t.Parallel()
	checkOCIHardlinksAndMetadata(t, host.MergedOCIEROFSLayout)
}

func TestActualSharedOCIHardlinksAndMetadata(t *testing.T) {
	t.Parallel()
	checkOCIHardlinksAndMetadata(t, host.LayeredEROFSLayout)
}

func TestActualMergedOCIWhiteoutOpaqueAndReplacement(t *testing.T) {
	t.Parallel()
	base := append(
		fixtures.BootLayerEntries(),
		fixtures.DirEntry("opaque"),
		fixtures.FileEntry("opaque/old", "hidden"),
		fixtures.FileEntry("removed", "hidden"),
		fixtures.FileEntry("replace", "old"),
	)
	top := []fixtures.TarEntry{
		fixtures.FileEntry("removed-again", "same layer"),
		fixtures.FileEntry(".wh.removed", ""),
		fixtures.FileEntry(".wh.replace", ""),
		fixtures.FileEntry("replace", "new"),
		fixtures.FileEntry("opaque/.wh..wh..opq", ""),
		fixtures.FileEntry("opaque/new", "new child"),
	}
	e, _, m := importMergedFixture(t, base, top)
	tree := fixtures.ExtractMergedManifest(t, e, m)
	for name, want := range map[string]string{
		"replace":       "new",
		"opaque/new":    "new child",
		"removed-again": "same layer",
	} {
		got, err := os.ReadFile(filepath.Join(tree, name))
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
		if _, err := os.Lstat(filepath.Join(tree, name)); !os.IsNotExist(err) {
			t.Fatalf("whiteout/opaque not applied: %s %v", name, err)
		}
	}
	rootTop := append(fixtures.BootLayerEntries(), fixtures.FileEntry(".wh..wh..opq", ""), fixtures.FileEntry("survivor", "top"))
	e2, _, m2 := importMergedFixture(t, base, rootTop)
	tree2 := fixtures.ExtractMergedManifest(t, e2, m2)
	if _, err := os.Stat(filepath.Join(tree2, "replace")); !os.IsNotExist(err) {
		t.Fatalf("root opaque retained lower file: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(tree2, "survivor")); err != nil || string(data) != "top" {
		t.Fatalf("root opaque lost same-layer file: %q %v", data, err)
	}
}

func TestActualMergedOCIOrderedEntries(t *testing.T) {
	t.Parallel()
	for name, entries := range map[string][]fixtures.TarEntry{
		"duplicate path":              {fixtures.FileEntry("target", "first"), fixtures.FileEntry("target", "last"), fixtures.HardEntry("alias", "target")},
		"replacement before whiteout": {fixtures.FileEntry("target", "last"), fixtures.FileEntry(".wh.target", ""), fixtures.HardEntry("alias", "target")},
	} {
		t.Run(name, func(t *testing.T) {
			base := append(fixtures.BootLayerEntries(), fixtures.FileEntry("target", "base"))
			e, _, m := importMergedFixture(t, base, entries)
			tree := fixtures.ExtractMergedManifest(t, e, m)
			for _, name := range []string{
				"target",
				"alias",
			} {
				data, err := os.ReadFile(filepath.Join(tree, name))
				if err != nil || string(data) != "last" {
					t.Fatalf("%s: %q %v", name, data, err)
				}
			}
			a, _ := os.Stat(filepath.Join(tree, "target"))
			b, _ := os.Stat(filepath.Join(tree, "alias"))
			if a == nil || b == nil || !os.SameFile(a, b) {
				t.Fatal("hardlink identity lost")
			}
		})
	}
}

func TestActualMergedOCIForwardHardlinkRefusal(t *testing.T) {
	t.Parallel()
	fixtures.RequireMergedOCI(t)
	layout := t.TempDir()
	entries := append(fixtures.BootLayerEntries(), fixtures.HardEntry("alias", "target"), fixtures.FileEntry("target", "data"))
	image := fixtures.PutOCIManifest(t, layout, "forward", []fixtures.OCILayerFixture{
		fixtures.PutOCILayer(t, layout, entries, false),
	})
	e := fixtures.NewNativeImageFixture(t)
	defer e.Close()
	_, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:          layout,
			Digest:          image,
			Materialization: host.MergedOCIEROFSLayout,
			MaxBytes:        4 << 20,
			MaxFiles:        1000,
		},
	)
	if err == nil || !strings.Contains(err.Error(), "link: no such file or directory") {
		t.Fatalf("expected explicit native umoci forward-hardlink failure: %v", err)
	}
	var count int
	if err := e.DB.QueryRow("SELECT count(*) FROM images WHERE digest=?", image).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed unpack published an image: %d %v", count, err)
	}
}

func TestActualMergedOCISymlinkHardlinkAndConfinement(t *testing.T) {
	t.Parallel()
	base := append(
		fixtures.BootLayerEntries(),
		fixtures.TarEntry{
			Header: tar.Header{
				Name:     "sym",
				Typeflag: tar.TypeSymlink,
				Linkname: "/guest/target",
				Mode:     0o777,
			},
		},
		fixtures.FileEntry("victim", "unchanged"),
	)
	top := []fixtures.TarEntry{
		fixtures.HardEntry("alias", "sym"),
		{
			Header: tar.Header{
				Name:     "escape",
				Typeflag: tar.TypeSymlink,
				Linkname: "/work",
				Mode:     0o777,
			},
		},
		fixtures.FileEntry("escape/victim", "inside guest"),
	}
	e, _, m := importMergedFixture(t, base, top)
	tree := fixtures.ExtractMergedManifest(t, e, m)
	for _, name := range []string{
		"sym",
		"alias",
	} {
		link, err := os.Readlink(filepath.Join(tree, name))
		if err != nil || link != "/guest/target" {
			t.Fatalf("%s symlink: %q %v", name, link, err)
		}
	}
	a, _ := os.Lstat(filepath.Join(tree, "sym"))
	b, _ := os.Lstat(filepath.Join(tree, "alias"))
	if a == nil || b == nil || !os.SameFile(a, b) {
		t.Fatal("symlink hardlink identity lost")
	}
	if data, err := os.ReadFile(filepath.Join(tree, "victim")); err != nil || string(data) != "unchanged" {
		t.Fatalf("symlink parent escaped root: %q %v", data, err)
	}
	if data, err := os.ReadFile(
		filepath.Join(e.ImportWork(t, m.Digest), "output", "victim"),
	); !os.IsNotExist(err) {
		t.Fatalf("symlink parent wrote builder workspace: %q %v", data, err)
	}
}

func TestActualMergedOCIExplicitSelectionAndVerification(t *testing.T) {
	t.Parallel()
	fixtures.RequireMergedOCI(t)
	layout := t.TempDir()
	entries := append(fixtures.BootLayerEntries(), fixtures.DirEntry("."))
	layers := []fixtures.OCILayerFixture{
		fixtures.PutOCILayer(t, layout, entries, true),
	}
	image := fixtures.PutOCIManifest(t, layout, "explicit", layers)
	e := fixtures.NewNativeImageFixture(t)
	defer e.Close()
	opts := imageimport.ImageImportOptions{
		Layout:   layout,
		Digest:   image,
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	}
	shared := fixtures.NewNativeImageFixture(t)
	defer shared.Close()
	defaultManifest, err := shared.ImportOCI(context.Background(), opts)
	if err != nil || defaultManifest.Layout != host.LayeredEROFSLayout || !defaultManifest.SharedLayered() {
		t.Fatalf("default must preserve root metadata in shared layers: %+v %v", defaultManifest, err)
	}
	opts.Materialization = host.MergedOCIEROFSLayout
	original, err := e.ImportOCI(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(e.Opts.StateDir, "images", image[7:])
	for name, mutate := range map[string]func(*host.ImageManifest){
		"format": func(m *host.ImageManifest) {
			m.Format = 1
		},
		"legacy identity": func(m *host.ImageManifest) {
			m.Layout = "merged-erofs-v1"
		},
		"builder": func(m *host.ImageManifest) {
			m.Builder = host.ImageBuilder(m.Tools)
		},
		"umoci version": func(m *host.ImageManifest) {
			m.Tools["umoci"] = "umoci version 0.5.0"
		},
		"erofs version": func(m *host.ImageManifest) {
			m.Tools["mkfs.erofs"] = "mkfs.erofs 1.9.5"
		},
		"missing file": func(m *host.ImageManifest) {
			delete(m.Files, "root.erofs")
		},
		"extra file": func(m *host.ImageManifest) {
			m.Files["ignored"] = m.Files["kernel"]
		},
		"wrong hash": func(m *host.ImageManifest) {
			m.Files["root.erofs"] = m.Files["kernel"]
		},
		"extra device": func(m *host.ImageManifest) {
			m.Artifacts = []host.ImageArtifact{
				{
					Role: "layer",
				},
			}
		},
		"invalid diff": func(m *host.ImageManifest) {
			m.DiffIDs[0] = "sha256:bad"
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := cloneImageManifest(t, original)
			mutate(&m)
			if err := m.Verify(dir, image); err == nil {
				t.Fatal("invalid merged manifest accepted")
			}
		})
	}
	root := filepath.Join(dir, "root.erofs")
	if err := os.Rename(root, root+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := original.Verify(dir, image); err == nil {
		t.Fatal("missing root accepted")
	}
	if err := os.Symlink(root+".saved", root); err != nil {
		t.Fatal(err)
	}
	if err := original.Verify(dir, image); err == nil {
		t.Fatal("symlink root accepted")
	}
	os.Remove(root)
	if err := os.Rename(root+".saved", root); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := original.Verify(dir, image); err == nil {
		t.Fatal("writable root accepted")
	}
	if err := os.WriteFile(root, make([]byte, 4096), 0o444); err != nil {
		t.Fatal(err)
	}
	os.Chmod(root, 0o444)
	m := cloneImageManifest(t, original)
	m.Files["root.erofs"], _ = loom.DigestFile(root)
	if err := m.Verify(dir, image); err == nil {
		t.Fatal("non-EROFS bytes accepted with matching checksum")
	}
}

func TestActualMergedOCIUnpublishedDestinationGuard(t *testing.T) {
	t.Parallel()
	fixtures.RequireMergedOCI(t)
	for _, kind := range []string{
		"other layout",
		"dangling symlink",
	} {
		t.Run(kind, func(t *testing.T) {
			layout := t.TempDir()
			image := fixtures.PutOCIManifest(
				t,
				layout,
				kind,
				[]fixtures.OCILayerFixture{
					fixtures.PutOCILayer(t, layout, fixtures.BootLayerEntries(), false),
				},
			)
			e := fixtures.NewNativeImageFixture(t)
			defer e.Close()
			opts := imageimport.ImageImportOptions{
				Layout:   layout,
				Digest:   image,
				MaxBytes: 4 << 20,
				MaxFiles: 1000,
			}
			dest := filepath.Join(e.Opts.StateDir, "images", image[7:])
			if kind == "other layout" {
				if _, err := e.ImportOCI(context.Background(), opts); err != nil {
					t.Fatal(err)
				}
				if _, err := e.DB.Exec("DELETE FROM images WHERE digest=?", image); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Symlink(filepath.Join(t.TempDir(), "absent"), dest); err != nil {
				t.Fatal(err)
			}
			before, err := os.Lstat(dest)
			if err != nil {
				t.Fatal(err)
			}
			opts.Materialization = host.MergedOCIEROFSLayout
			if _, err := e.ImportOCI(context.Background(), opts); err == nil {
				t.Fatal("existing unpublished destination replaced or layout changed")
			}
			after, err := os.Lstat(dest)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("existing destination changed: %v", err)
			}
			var count int
			if err := e.DB.QueryRow("SELECT count(*) FROM images WHERE digest=?", image).
				Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed import published image: %d %v", count, err)
			}
		})
	}
}

func TestActualMergedOCIInvalidInputsDoNotPublish(t *testing.T) {
	t.Parallel()
	fixtures.RequireMergedOCI(t)
	for _, kind := range []string{
		"diff_id",
		"layer checksum",
		"layer size",
		"config size",
		"resource bound",
		"symlink boot",
		"invalid owner",
	} {
		t.Run(kind, func(t *testing.T) {
			layout := t.TempDir()
			entries := fixtures.BootLayerEntries()
			if kind == "symlink boot" {
				entries = append(
					entries,
					fixtures.TarEntry{
						Header: tar.Header{
							Name:     "boot/vmlinux",
							Typeflag: tar.TypeSymlink,
							Linkname: "initramfs",
							Mode:     0o777,
						},
					},
				)
			}
			if kind == "invalid owner" {
				entry := fixtures.FileEntry("owner", "data")
				entry.Header.Uid = 1<<32 - 1
				entries = append(entries, entry)
			}
			layer := fixtures.PutOCILayer(t, layout, entries, true)
			if kind == "diff_id" {
				layer.Diff = ocidigest.Digest("sha256:" + strings.Repeat("0", 64))
			}
			if kind == "layer size" {
				layer.Descriptor.Size++
			}
			image := fixtures.PutOCIManifest(t, layout, kind, []fixtures.OCILayerFixture{
				layer,
			})
			if kind == "config size" {
				file, err := host.Blob(layout, image)
				if err != nil {
					t.Fatal(err)
				}
				var manifest v1.Manifest
				if err := host.ReadBlobJSON(file, image, &manifest); err != nil {
					t.Fatal(err)
				}
				manifest.Config.Size++
				data, _ := json.Marshal(manifest)
				image = fixtures.PutOCIBlob(t, layout, data, v1.MediaTypeImageManifest).Digest.String()
			}
			if kind == "layer checksum" {
				if err := os.WriteFile(
					filepath.Join(layout, "blobs", "sha256", layer.Descriptor.Digest.Encoded()),
					[]byte("corrupt"),
					0o600,
				); err != nil {
					t.Fatal(err)
				}
			}
			e := fixtures.NewNativeImageFixture(t)
			defer e.Close()
			opts := imageimport.ImageImportOptions{
				Layout:          layout,
				Digest:          image,
				Materialization: host.MergedOCIEROFSLayout,
				MaxBytes:        4 << 20,
				MaxFiles:        1000,
			}
			if kind == "resource bound" {
				opts.MaxFiles = 1
			}
			if _, err := e.ImportOCI(context.Background(), opts); err == nil {
				t.Fatal("invalid/unsupported image accepted")
			}
			var count int
			if err := e.DB.QueryRow("SELECT count(*) FROM images WHERE digest=?", image).
				Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed import published image: %d %v", count, err)
			}
		})
	}
}

func TestHavenActualOCIImportRecoveryUsesFrozenOutput(t *testing.T) {
	for _, boundary := range []string{
		"image_import_materialized",
		"image_import_renamed",
		"image_import_publication_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			options := fixtures.PrivateOCIImport(t)
			service, runtime, _, _ := testapp.SandboxFixtureWithFault(t, func(point string) error {
				if point == boundary {
					return errs.ErrInterrupted
				}
				return nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			operation, err := service.Images.AdmitImport(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "import", options)
			if err != nil {
				t.Fatal(err)
			}
			fixtures.WaitImportInterruption(t, service, operation.ID)
			before, err := observeImageImport(ctx, service.State.Database(), operation.ID)
			if err != nil || before.State.Proof == nil {
				t.Fatal(before, err)
			}
			if err = os.RemoveAll(options.Layout); err != nil {
				t.Fatal(err)
			}
			service = testapp.ReopenCloneService(t, service, runtime)
			result, err := service.Operations.WaitManagement(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, operation.ID)
			if err != nil || result.Result.ImageDigest != options.Digest {
				t.Fatal(result, err)
			}
			after, err := observeImageImport(ctx, service.State.Database(), operation.ID)
			if err != nil || !reflect.DeepEqual(before.State.Proof, after.State.Proof) || !reflect.DeepEqual(before.State.Boundary, after.State.Boundary) {
				t.Fatal("recovery replaced original output proof", before, after, err)
			}
		})
	}
}

func TestHavenActualOCIImportRejectsReplacedOutput(t *testing.T) {
	options := fixtures.PrivateOCIImport(t)
	service, runtime, _, _ := testapp.SandboxFixtureWithFault(t, func(point string) error {
		if point == "image_import_materialized" {
			return errs.ErrInterrupted
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	operation, err := service.Images.AdmitImport(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "import", options)
	if err != nil {
		t.Fatal(err)
	}
	fixtures.WaitImportInterruption(t, service, operation.ID)
	step, err := observeImageImport(ctx, service.State.Database(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(step.Input.Work, "output")
	original := filepath.Join(step.Input.Work, "original-output")
	if err = os.Rename(output, original); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatal("unexpected output directory")
		}
		if err = loom.CopyFile(filepath.Join(original, entry.Name()), filepath.Join(output, entry.Name()), 0o400); err != nil {
			t.Fatal(err)
		}
	}
	service = testapp.ReopenCloneService(t, service, runtime)
	result, err := service.Operations.WaitManagement(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, operation.ID)
	if err == nil || result.Status != "failed" {
		t.Fatal(result, err)
	}
	var count int
	if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM images WHERE digest=?", options.Digest).Scan(&count); err != nil || count != 0 {
		t.Fatal("replacement was published", count, err)
	}
	if _, err = os.Stat(original); err != nil {
		t.Fatal("original output was removed", err)
	}
}

func TestHavenActualOCIImportPublishesSingleCatalog(t *testing.T) {
	for _, layout := range []string{
		host.LayeredEROFSLayout,
		host.MergedOCIEROFSLayout,
	} {
		t.Run(layout, func(t *testing.T) {
			options := fixtures.PrivateOCIImport(t)
			options.Materialization = layout
			service, _, _, _ := testapp.SandboxFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			operation, err := service.Images.AdmitImport(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "import", options)
			if err != nil {
				t.Fatal(err)
			}
			result, err := service.Operations.WaitManagement(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, operation.ID)
			if err != nil {
				last := service.Workers.State(operation.ID).Err
				t.Fatal(result, err, last)
			}
			manifest, err := image.LoadManifest(ctx, service.State.Database(), options.Digest)
			if err != nil || manifest.Layout != layout || result.Result.ImageDigest != options.Digest {
				t.Fatal(manifest, result, err)
			}
			if err = image.VerifyFiles(manifest, filepath.Join(service.State.Directory(), "images", options.Digest[7:])); err != nil {
				t.Fatal(err)
			}
			for {
				active := service.Workers.State(operation.ID).Active
				if !active {
					break
				}
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(time.Millisecond)
			}
			report, err := service.GC.Collect(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "work-gc", types.GCOptions{
				Budget: 20 * time.Second,
			})
			if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "image_work" {
				for _, item := range report.Deferred {
					if item.OperationID != "" {
						last := service.Workers.State(item.OperationID).Err
						t.Log(item.Kind, item.OperationID, last)
					}
				}
				t.Fatal(report, err)
			}
			if err = image.VerifyFiles(manifest, filepath.Join(service.State.Directory(), "images", options.Digest[7:])); err != nil {
				t.Fatal("work cleanup affected publication", err)
			}
			replay, err := service.Images.AdmitImport(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "import", options)
			if err != nil || replay.ID != operation.ID {
				t.Fatal(replay, err)
			}
			conflict := options
			if layout == host.LayeredEROFSLayout {
				conflict.Materialization = host.MergedOCIEROFSLayout
			} else {
				conflict.Materialization = host.LayeredEROFSLayout
			}
			if _, err = service.Images.AdmitImport(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "conflict", conflict); err == nil {
				t.Fatal("digest changed materialization")
			}
		})
	}
}

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
	e, layout, m := importSharedFixture(t, base, top)
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
	e, _, m := importSharedFixture(t, base, top)
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
	_, _, first := importSharedFixture(t, entries)
	_, _, second := importSharedFixture(t, entries)
	if first.Builder != second.Builder || !reflect.DeepEqual(first.Files, second.Files) ||
		!reflect.DeepEqual(first.Artifacts, second.Artifacts) {
		t.Fatal("identical source/config/profile produced different immutable output")
	}
}

// The fixture observes only the frozen path and output witnesses under test.
// Workflow validation remains the same public entry used by store recovery.
type imageImportEvidence struct {
	Input struct {
		Work string `json:"work"`
	}
	State struct {
		Boundary *imageimport.ImageImportBoundary `json:"boundary,omitempty"`
		Proof    *imageimport.ImageImportProof    `json:"proof,omitempty"`
	}
}

func observeImageImport(ctx context.Context, database store.DBTX, id string) (imageImportEvidence, error) {
	var evidence imageImportEvidence
	if err := image.ValidateImageImport(ctx, database, id); err != nil {
		return evidence, err
	}
	step, err := testfixture.ObserveStep(ctx, database, id, "image_import")
	if err != nil {
		return evidence, err
	}
	if err = json.Unmarshal(step.Input, &evidence.Input); err != nil {
		return evidence, err
	}
	err = json.Unmarshal(step.Body, &evidence.State)
	return evidence, err
}

func importMergedFixture(t *testing.T, layers ...[]fixtures.TarEntry) (*fixtures.NativeImageFixture, string, host.ImageManifest) {
	t.Helper()
	fixtures.RequireMergedOCI(t)
	layout := t.TempDir()
	var ociLayers []fixtures.OCILayerFixture
	for i, entries := range layers {
		ociLayers = append(ociLayers, fixtures.PutOCILayer(t, layout, entries, i%2 == 0))
	}
	image := fixtures.PutOCIManifest(t, layout, "compat", ociLayers)
	e := fixtures.NewNativeImageFixture(t)
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

func dumpMergedPath(t *testing.T, e *fixtures.NativeImageFixture, m host.ImageManifest, path string) string {
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
func checkOCIHardlinksAndMetadata(t *testing.T, materialization string) {
	root := fixtures.DirEntry(".")
	root.Header.Format = tar.FormatPAX
	root.Header.Mode, root.Header.Uid, root.Header.Gid = 0o751, 42, 43
	root.Header.ModTime = time.Unix(1700000010, 123456789)
	root.Header.Xattrs = map[string]string{
		"user.root": "root retained",
	}
	parent := fixtures.DirEntry("inherited")
	parent.Header.Format = tar.FormatPAX
	parent.Header.Mode = 0o2750
	parent.Header.ModTime = time.Unix(1700000020, 234567890)
	parent.Header.Xattrs = map[string]string{
		"user.directory": "inherited retained",
	}
	file := fixtures.FileEntry("target", "cross-layer content")
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
	aclFile := fixtures.FileEntry("acl", "acl payload")
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
	capNS := fixtures.FileEntry("cap-ns", "namespaced capability")
	capV3 := append(append([]byte(nil), capability...), make([]byte, 4)...)
	binary.LittleEndian.PutUint32(capV3, 0x03000001)
	binary.LittleEndian.PutUint32(capV3[20:], 333)
	capNS.Header.Xattrs = map[string]string{
		"security.capability": string(capV3),
	}
	base := append(fixtures.BootLayerEntries(), root, parent, file, aclFile, capNS)
	top := []fixtures.TarEntry{
		fixtures.HardEntry("alias", "target"),
		fixtures.FileEntry("inherited/new", "new child"),
	}
	var e *fixtures.NativeImageFixture
	var m host.ImageManifest
	if materialization == host.LayeredEROFSLayout {
		e, _, m = importSharedFixture(t, base, top)
	} else {
		e, _, m = importMergedFixture(t, base, top)
	}
	tree := fixtures.ExtractMergedManifest(t, e, m)
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

func importSharedFixture(t *testing.T, layers ...[]fixtures.TarEntry) (*fixtures.NativeImageFixture, string, host.ImageManifest) {
	t.Helper()
	fixtures.RequireEROFS(t)
	layout := t.TempDir()
	var ociLayers []fixtures.OCILayerFixture
	for i, entries := range layers {
		ociLayers = append(ociLayers, fixtures.PutOCILayer(t, layout, entries, i%2 == 0))
	}
	image := fixtures.PutOCIManifest(t, layout, "shared", ociLayers)
	e := fixtures.NewNativeImageFixture(t)
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
