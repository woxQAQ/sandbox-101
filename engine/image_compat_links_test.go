package engine

import (
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestActualMergedOCIOrderedEntries(t *testing.T) {
	for name, entries := range map[string][]tarEntry{
		"duplicate path":              {fileEntry("target", "first"), fileEntry("target", "last"), hardEntry("alias", "target")},
		"replacement before whiteout": {fileEntry("target", "last"), fileEntry(".wh.target", ""), hardEntry("alias", "target")},
	} {
		t.Run(name, func(t *testing.T) {
			base := append(bootLayerEntries(), fileEntry("target", "base"))
			e, _, m := importMergedFixture(t, base, entries)
			tree := extractMergedManifest(t, e, m)
			for _, name := range []string{"target", "alias"} {
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
	requireMergedOCI(t)
	layout := t.TempDir()
	entries := append(bootLayerEntries(), hardEntry("alias", "target"), fileEntry("target", "data"))
	image := putOCIManifest(t, layout, "forward", []ociLayerFixture{putOCILayer(t, layout, entries, false)})
	e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
	defer e.Close()
	_, err := e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: image, Materialization: MergedOCIEROFSLayout, MaxBytes: 4 << 20, MaxFiles: 1000})
	if err == nil || !strings.Contains(err.Error(), "link: no such file or directory") {
		t.Fatalf("expected explicit native umoci forward-hardlink failure: %v", err)
	}
	var count int
	if err := e.db.QueryRow("SELECT count(*) FROM meta WHERE key=?", "image:"+image).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed unpack published an image: %d %v", count, err)
	}
}

func TestActualMergedOCISymlinkHardlinkAndConfinement(t *testing.T) {
	base := append(bootLayerEntries(), tarEntry{header: tar.Header{Name: "sym", Typeflag: tar.TypeSymlink, Linkname: "/guest/target", Mode: 0777}}, fileEntry("victim", "unchanged"))
	top := []tarEntry{hardEntry("alias", "sym"), {header: tar.Header{Name: "escape", Typeflag: tar.TypeSymlink, Linkname: "/work", Mode: 0777}}, fileEntry("escape/victim", "inside guest")}
	e, _, m := importMergedFixture(t, base, top)
	tree := extractMergedManifest(t, e, m)
	for _, name := range []string{"sym", "alias"} {
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
	if data, err := os.ReadFile(filepath.Join(e.opts.StateDir, "imports", m.Digest[7:], "output", "victim")); !os.IsNotExist(err) {
		t.Fatalf("symlink parent wrote builder workspace: %q %v", data, err)
	}
}
