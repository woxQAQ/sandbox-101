package native

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/tests/fixtures"

	ocidigest "github.com/opencontainers/go-digest"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/woxQAQ/haven/internal/errs"
)

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
	fixtures.CheckOCIHardlinksAndMetadata(t, host.MergedOCIEROFSLayout)
}

func TestActualSharedOCIHardlinksAndMetadata(t *testing.T) {
	t.Parallel()
	fixtures.CheckOCIHardlinksAndMetadata(t, host.LayeredEROFSLayout)
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
	e, _, m := fixtures.ImportMergedFixture(t, base, top)
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
	e2, _, m2 := fixtures.ImportMergedFixture(t, base, rootTop)
	tree2 := fixtures.ExtractMergedManifest(t, e2, m2)
	if _, err := os.Stat(filepath.Join(tree2, "replace")); !os.IsNotExist(err) {
		t.Fatalf("root opaque retained lower file: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(tree2, "survivor")); err != nil || string(data) != "top" {
		t.Fatalf("root opaque lost same-layer file: %q %v", data, err)
	}
}

func TestActualMergedOCIMaterializationConflict(t *testing.T) {
	t.Parallel()
	e, layout, m := fixtures.ImportMergedFixture(t, fixtures.BootLayerEntries())
	_, err := e.ImportOCI(context.Background(), imageimport.ImageImportOptions{
		Layout: layout,
		Digest: m.Digest,
	})
	if !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("digest layout change must conflict: %v", err)
	}
	if again, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:          layout,
			Digest:          m.Digest,
			Materialization: host.MergedOCIEROFSLayout,
		},
	); err != nil || again.Builder != m.Builder {
		t.Fatalf("same layout reimport: %v", err)
	}
	layer := fixtures.PutOCILayer(t, layout, fixtures.BootLayerEntries(), false)
	image := fixtures.PutOCIManifest(t, layout, "layered-first", []fixtures.OCILayerFixture{
		layer,
	})
	opts := imageimport.ImageImportOptions{
		Layout:   layout,
		Digest:   image,
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	}
	if _, err := e.ImportOCI(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	opts.Materialization = host.MergedOCIEROFSLayout
	_, err = e.ImportOCI(context.Background(), opts)
	if !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("layered-first materialization change must conflict: %v", err)
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
			e, _, m := fixtures.ImportMergedFixture(t, base, entries)
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
	e, _, m := fixtures.ImportMergedFixture(t, base, top)
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
			m := fixtures.CloneImageManifest(t, original)
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
	m := fixtures.CloneImageManifest(t, original)
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
