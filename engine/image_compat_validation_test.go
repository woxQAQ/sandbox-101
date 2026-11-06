package engine

import (
	"archive/tar"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestMergedOCIPreflight(t *testing.T) {
	for name, header := range map[string]*tar.Header{
		"traversal":         {Name: "../escape", Typeflag: tar.TypeReg},
		"absolute":          {Name: "/absolute", Typeflag: tar.TypeReg},
		"empty component":   {Name: "bad//path", Typeflag: tar.TypeReg},
		"device":            {Name: "device", Typeflag: tar.TypeChar},
		"fifo":              {Name: "pipe", Typeflag: tar.TypeFifo},
		"hardlink escape":   {Name: "alias", Typeflag: tar.TypeLink, Linkname: "../outside"},
		"absolute hardlink": {Name: "alias", Typeflag: tar.TypeLink, Linkname: "/outside"},
		"whiteout data":     {Name: ".wh.victim", Typeflag: tar.TypeReg, Size: 1},
		"whiteout parent":   {Name: ".wh...", Typeflag: tar.TypeReg},
		"nondirectory root": {Name: ".", Typeflag: tar.TypeReg},
		"overlay xattr":     {Name: "file", Typeflag: tar.TypeReg, Xattrs: map[string]string{"user.overlay.opaque": "y"}},
		"unknown pax":       {Name: "file", Typeflag: tar.TypeReg, PAXRecords: map[string]string{"vendor.ignored": "x"}},
	} {
		t.Run(name, func(t *testing.T) {
			file, diff := layerFixture(t, header)
			if _, _, err := validateLayerFor(
				file,
				v1.MediaTypeImageLayer,
				diff,
				1<<20,
				100,
				true,
			); err == nil {
				t.Fatal("compatibility bypassed security preflight")
			}
		})
	}
	file, diff := layerFixture(
		t,
		&tar.Header{Name: "file", Typeflag: tar.TypeReg, Size: 1024},
	)
	for name, limits := range map[string][2]int64{"bytes": {100, 100}, "files": {1 << 20, 0}} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := validateLayerFor(
				file,
				v1.MediaTypeImageLayer,
				diff,
				limits[0],
				limits[1],
				true,
			); err == nil {
				t.Fatal("compatibility bypassed resource bounds")
			}
		})
	}
	if _, _, err := validateLayerFor(
		file,
		v1.MediaTypeImageLayer,
		"sha256:"+strings.Repeat("0", 64),
		1<<20,
		100,
		true,
	); err == nil {
		t.Fatal("compatibility bypassed diff_id validation")
	}
}

func TestActualMergedOCIExplicitSelectionAndVerification(t *testing.T) {
	requireMergedOCI(t)
	layout := t.TempDir()
	entries := append(bootLayerEntries(), dirEntry("."))
	layers := []ociLayerFixture{putOCILayer(t, layout, entries, true)}
	image := putOCIManifest(t, layout, "explicit", layers)
	e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
	defer e.Close()
	opts := ImportOptions{
		Layout:   layout,
		Digest:   image,
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	}
	shared := openTest(t, t.TempDir(), newTestBackend(), true, nil)
	defer shared.Close()
	defaultManifest, err := shared.ImportOCI(context.Background(), opts)
	if err != nil || defaultManifest.Layout != LayeredEROFSLayout ||
		!defaultManifest.sharedLayered() {
		t.Fatalf(
			"default must preserve root metadata in shared layers: %+v %v",
			defaultManifest,
			err,
		)
	}
	opts.Materialization = MergedOCIEROFSLayout
	original, err := e.ImportOCI(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(e.opts.StateDir, "images", image[7:])
	for name, mutate := range map[string]func(*ImageManifest){
		"format":          func(m *ImageManifest) { m.Format = 1 },
		"legacy identity": func(m *ImageManifest) { m.Layout = "merged-erofs-v1" },
		"builder":         func(m *ImageManifest) { m.Builder = imageBuilder(m.Tools) },
		"umoci version":   func(m *ImageManifest) { m.Tools["umoci"] = "umoci version 0.5.0" },
		"erofs version":   func(m *ImageManifest) { m.Tools["mkfs.erofs"] = "mkfs.erofs 1.9.5" },
		"missing file":    func(m *ImageManifest) { delete(m.Files, "root.erofs") },
		"extra file":      func(m *ImageManifest) { m.Files["ignored"] = m.Files["kernel"] },
		"wrong hash":      func(m *ImageManifest) { m.Files["root.erofs"] = m.Files["kernel"] },
		"extra device":    func(m *ImageManifest) { m.Artifacts = []ImageArtifact{{Role: "layer"}} },
		"invalid diff":    func(m *ImageManifest) { m.DiffIDs[0] = "sha256:bad" },
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
	m.Files["root.erofs"], _ = digestFile(root)
	if err := m.Verify(dir, image); err == nil {
		t.Fatal("non-EROFS bytes accepted with matching checksum")
	}
}

func TestActualMergedOCIUnpublishedDestinationGuard(t *testing.T) {
	requireMergedOCI(t)
	for _, kind := range []string{"other layout", "dangling symlink"} {
		t.Run(kind, func(t *testing.T) {
			layout := t.TempDir()
			image := putOCIManifest(
				t,
				layout,
				kind,
				[]ociLayerFixture{
					putOCILayer(t, layout, bootLayerEntries(), false),
				},
			)
			e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
			defer e.Close()
			opts := ImportOptions{
				Layout:   layout,
				Digest:   image,
				MaxBytes: 4 << 20,
				MaxFiles: 1000,
			}
			dest := filepath.Join(e.opts.StateDir, "images", image[7:])
			if kind == "other layout" {
				if _, err := e.ImportOCI(
					context.Background(),
					opts,
				); err != nil {
					t.Fatal(err)
				}
				if _, err := e.db.Exec(
					"DELETE FROM meta WHERE key=?",
					"image:"+image,
				); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Symlink(filepath.Join(t.TempDir(), "absent"), dest); err != nil {
				t.Fatal(err)
			}
			before, err := os.Lstat(dest)
			if err != nil {
				t.Fatal(err)
			}
			opts.Materialization = MergedOCIEROFSLayout
			if _, err := e.ImportOCI(context.Background(), opts); err == nil {
				t.Fatal(
					"existing unpublished destination replaced or layout changed",
				)
			}
			after, err := os.Lstat(dest)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("existing destination changed: %v", err)
			}
			var count int
			if err := e.db.QueryRow("SELECT count(*) FROM meta WHERE key=?", "image:"+image).
				Scan(&count); err != nil ||
				count != 0 {
				t.Fatalf("failed import published image: %d %v", count, err)
			}
		})
	}
}

func TestActualMergedOCIInvalidInputsDoNotPublish(t *testing.T) {
	requireMergedOCI(t)
	for _, kind := range []string{"diff_id", "layer checksum", "layer size", "config size", "resource bound", "symlink boot", "unmapped owner"} {
		t.Run(kind, func(t *testing.T) {
			layout := t.TempDir()
			entries := bootLayerEntries()
			if kind == "symlink boot" {
				entries = append(
					entries,
					tarEntry{
						header: tar.Header{
							Name:     "boot/vmlinux",
							Typeflag: tar.TypeSymlink,
							Linkname: "initramfs",
							Mode:     0o777,
						},
					},
				)
			}
			if kind == "unmapped owner" {
				if os.Geteuid() == 0 {
					t.Skip("root can identity-map all owners")
				}
				entry := fileEntry("owner", "data")
				entry.header.Uid = 1 << 31
				entries = append(entries, entry)
			}
			layer := putOCILayer(t, layout, entries, true)
			if kind == "diff_id" {
				layer.diff = digest.Digest("sha256:" + strings.Repeat("0", 64))
			}
			if kind == "layer size" {
				layer.descriptor.Size++
			}
			image := putOCIManifest(t, layout, kind, []ociLayerFixture{layer})
			if kind == "config size" {
				file, err := blob(layout, image)
				if err != nil {
					t.Fatal(err)
				}
				var manifest v1.Manifest
				if err := readBlobJSON(file, image, &manifest); err != nil {
					t.Fatal(err)
				}
				manifest.Config.Size++
				data, _ := json.Marshal(manifest)
				image = putOCIBlob(
					t,
					layout,
					data,
					v1.MediaTypeImageManifest,
				).Digest.String()
			}
			if kind == "layer checksum" {
				if err := os.WriteFile(
					filepath.Join(
						layout,
						"blobs",
						"sha256",
						layer.descriptor.Digest.Encoded(),
					),
					[]byte("corrupt"),
					0o600,
				); err != nil {
					t.Fatal(err)
				}
			}
			e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
			defer e.Close()
			opts := ImportOptions{
				Layout:          layout,
				Digest:          image,
				Materialization: MergedOCIEROFSLayout,
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
			if err := e.db.QueryRow("SELECT count(*) FROM meta WHERE key=?", "image:"+image).
				Scan(&count); err != nil ||
				count != 0 {
				t.Fatalf("failed import published image: %d %v", count, err)
			}
		})
	}
}
