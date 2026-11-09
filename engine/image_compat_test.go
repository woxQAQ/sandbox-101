package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"

	"github.com/woxQAQ/sandbox-engine/api"
)

func requireMergedOCI(t *testing.T) {
	t.Helper()
	requireEROFS(t)
	tools, err := imageTools(context.Background())
	if err == nil {
		_, err = mergedOCITools(context.Background(), tools)
	}
	if err == nil {
		binary, x := exec.LookPath("umoci")
		if x == nil {
			binary, x = filepath.EvalSymlinks(binary)
		}
		if x == nil {
			var args []string
			args, err = imageToolSandbox(
				t.TempDir(),
				[]imageMount{{binary, "/tools/umoci"}},
				true,
			)
			if err == nil {
				err = runMappedImageTool(
					context.Background(),
					append(args, "--", "/tools/umoci", "--version"),
				)
			}
		} else {
			err = x
		}
	}
	if err != nil {
		if os.Getenv("SANDBOX_REQUIRE_EROFS_TESTS") == "1" {
			t.Fatal(err)
		}
		t.Skipf(
			"actual OCI compatibility tool/namespace test unavailable: %v",
			err,
		)
	}
}

func importMergedFixture(
	t *testing.T,
	layers ...[]tarEntry,
) (*Engine, string, ImageManifest) {
	t.Helper()
	requireMergedOCI(t)
	layout := t.TempDir()
	var fixtures []ociLayerFixture
	for i, entries := range layers {
		fixtures = append(fixtures, putOCILayer(t, layout, entries, i%2 == 0))
	}
	image := putOCIManifest(t, layout, "compat", fixtures)
	e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
	t.Cleanup(func() { e.Close() })
	m, err := e.ImportOCI(
		context.Background(),
		ImportOptions{
			Layout:          layout,
			Digest:          image,
			Materialization: MergedOCIEROFSLayout,
			MaxBytes:        4 << 20,
			MaxFiles:        1000,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return e, layout, m
}

func extractMergedManifest(t *testing.T, e *Engine, m ImageManifest) string {
	t.Helper()
	bindings, err := m.ReadOnlyBindings(
		filepath.Join(e.opts.StateDir, "images", m.Digest[7:]),
	)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	binary, err := exec.LookPath("fsck.erofs")
	if err != nil {
		t.Fatal(err)
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		t.Fatal(err)
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Fatal(err)
	}
	sh, err = filepath.EvalSymlinks(sh)
	if err != nil {
		t.Fatal(err)
	}
	reads := []imageMount{{binary, "/tools/fsck"}, {sh, "/tools/sh"}}
	fsckArgs := []string{
		"--extract=/work/tree",
		"--xattrs",
		"--no-preserve-owner",
		"--preserve-perms",
	}
	for i, binding := range bindings {
		reads = append(reads, imageMount{binding.HostPath, binding.GuestPath})
		if i != 0 {
			fsckArgs = append(fsckArgs, "--device="+binding.GuestPath)
		}
	}
	fsckArgs = append(fsckArgs, bindings[0].GuestPath)
	args, err := imageToolSandbox(out, reads, true)
	if err != nil {
		t.Fatal(err)
	}
	args = append(args, "--", "/tools/sh", "-euc", `/tools/fsck "$@"
while read -r inner outer count; do printf '%s %s %s\n' "$inner" "$outer" "$count"; done < /proc/self/uid_map > /work/uid_map
while read -r inner outer count; do printf '%s %s %s\n' "$inner" "$outer" "$count"; done < /proc/self/gid_map > /work/gid_map`, "extractor")
	args = append(args, fsckArgs...)
	if err = runMappedImageTool(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(out, "tree")
}

func dumpMergedPath(
	t *testing.T,
	e *Engine,
	m ImageManifest,
	path string,
) string {
	t.Helper()
	image := filepath.Join(
		e.opts.StateDir,
		"images",
		m.Digest[7:],
		"root.erofs",
	)
	if len(m.Artifacts) != 0 {
		image = filepath.Join(e.opts.StateDir, m.Artifacts[0].Path)
	}
	out, err := exec.Command("dump.erofs", "--path="+path, image).
		CombinedOutput()
	if err != nil {
		t.Fatalf("native inode inspection: %v: %s", err, out)
	}
	return string(out)
}

func mappedHostID(t *testing.T, mapping string, id uint32) uint32 {
	t.Helper()
	data, err := os.ReadFile(mapping)
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			t.Fatalf("invalid ID mapping %q", line)
		}
		inner, _ := strconv.ParseUint(fields[0], 10, 32)
		outer, _ := strconv.ParseUint(fields[1], 10, 32)
		count, _ := strconv.ParseUint(fields[2], 10, 32)
		if uint64(id) >= inner && uint64(id) < inner+count {
			return uint32(outer + uint64(id) - inner)
		}
	}
	t.Fatalf("ID %d absent from mapping %s", id, data)
	return 0
}

func TestActualOCIRejectsInvalidToolRoots(t *testing.T) {
	requireMergedOCI(t)
	layout := t.TempDir()
	layer := putOCILayer(t, layout, bootLayerEntries(), false)
	image := putOCIManifest(
		t,
		layout,
		"invalid-tool-environment",
		[]ociLayerFixture{layer},
	)
	for _, materialization := range []string{LayeredEROFSLayout, MergedOCIEROFSLayout} {
		t.Run(materialization, func(t *testing.T) {
			for _, roots := range []string{"relative/path", "/", "/proc", "/tmp/alternate-tools", "/missing-acceptance-tool-root"} {
				t.Run(roots, func(t *testing.T) {
					t.Setenv("SANDBOX_IMAGE_TOOL_ROOTS", roots)
					e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
					t.Cleanup(func() { e.Close() })
					_, err := e.ImportOCI(context.Background(), ImportOptions{
						Layout:          layout,
						Digest:          image,
						Materialization: materialization,
						MaxBytes:        4 << 20,
						MaxFiles:        1000,
					})
					if err == nil {
						t.Fatal(
							"OCI import accepted invalid tool dependency roots",
						)
					}
					if !strings.Contains(err.Error(), "tool root") &&
						!strings.Contains(
							err.Error(),
							"SANDBOX_IMAGE_TOOL_ROOTS",
						) {
						t.Fatalf("unexpected OCI import failure: %v", err)
					}
					published := filepath.Join(
						e.opts.StateDir,
						"images",
						image[7:],
					)
					if _, err := os.Stat(published); !os.IsNotExist(err) {
						t.Fatalf(
							"invalid tool environment published an image: %v",
							err,
						)
					}
				})
			}
		})
	}
}

func TestActualMergedOCIHardlinksAndMetadata(t *testing.T) {
	testOCIHardlinksAndMetadata(t, MergedOCIEROFSLayout)
}

func TestActualSharedOCIHardlinksAndMetadata(t *testing.T) {
	testOCIHardlinksAndMetadata(t, LayeredEROFSLayout)
}

//nolint:staticcheck // Verify binary capability and ACL xattrs across the legacy tar representation and both materializers.
func testOCIHardlinksAndMetadata(t *testing.T, materialization string) {
	root := dirEntry(".")
	root.header.Format = tar.FormatPAX
	root.header.Mode, root.header.Uid, root.header.Gid = 0o751, 42, 43
	root.header.ModTime = time.Unix(1700000010, 123456789)
	root.header.Xattrs = map[string]string{"user.root": "root retained"}
	parent := dirEntry("inherited")
	parent.header.Format = tar.FormatPAX
	parent.header.Mode = 0o2750
	parent.header.ModTime = time.Unix(1700000020, 234567890)
	parent.header.Xattrs = map[string]string{
		"user.directory": "inherited retained",
	}
	file := fileEntry("target", "cross-layer content")
	file.header.Format = tar.FormatPAX
	file.header.Mode = 0o6740
	file.header.ModTime = time.Unix(1700000030, 345678901)
	capability := make([]byte, 20)
	binary.LittleEndian.PutUint32(capability, 0x02000001)
	binary.LittleEndian.PutUint32(capability[4:], 1<<10)
	file.header.Xattrs = map[string]string{
		"user.fixture":        "retained",
		"security.capability": string(capability),
	}
	aclFile := fileEntry("acl", "acl payload")
	acl := make([]byte, 4+5*8)
	binary.LittleEndian.PutUint32(acl, 2)
	for i, entry := range []struct {
		tag, perm uint16
		id        uint32
	}{{1, 6, 0xffffffff}, {2, 4, 333}, {4, 4, 0xffffffff}, {16, 4, 0xffffffff}, {32, 0, 0xffffffff}} {
		binary.LittleEndian.PutUint16(acl[4+i*8:], entry.tag)
		binary.LittleEndian.PutUint16(acl[6+i*8:], entry.perm)
		binary.LittleEndian.PutUint32(acl[8+i*8:], entry.id)
	}
	aclFile.header.Xattrs = map[string]string{
		"system.posix_acl_access": string(acl),
	}
	capNS := fileEntry("cap-ns", "namespaced capability")
	capV3 := append(append([]byte(nil), capability...), make([]byte, 4)...)
	binary.LittleEndian.PutUint32(capV3, 0x03000001)
	binary.LittleEndian.PutUint32(capV3[20:], 333)
	capNS.header.Xattrs = map[string]string{
		"security.capability": string(capV3),
	}
	base := append(bootLayerEntries(), root, parent, file, aclFile, capNS)
	top := []tarEntry{
		hardEntry("alias", "target"),
		fileEntry("inherited/new", "new child"),
	}
	var e *Engine
	var m ImageManifest
	if materialization == LayeredEROFSLayout {
		e, _, m = importSharedFixture(t, base, top)
	} else {
		e, _, m = importMergedFixture(t, base, top)
	}
	tree := extractMergedManifest(t, e, m)
	for name, want := range map[string]string{"target": "cross-layer content", "alias": "cross-layer content", "inherited/new": "new child"} {
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
	for name, header := range map[string]tar.Header{".": root.header, "inherited": parent.header, "target": file.header, "acl": aclFile.header, "cap-ns": capNS.header} {
		st, err := os.Stat(filepath.Join(tree, name))
		if err != nil || st.Mode().Perm() != os.FileMode(header.Mode&0o777) ||
			!st.ModTime().Equal(header.ModTime) {
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
		if err := unix.Lstat(
			filepath.Join(tree, name),
			&native,
		); err != nil ||
			native.Mode&0o7777 != uint32(header.Mode) {
			t.Fatalf(
				"%s permission bits: %#o %v; expected %#o",
				name,
				native.Mode&0o7777,
				err,
				header.Mode,
			)
		}
		for key, value := range header.Xattrs {
			buf := make([]byte, 4096)
			n, err := unix.Getxattr(filepath.Join(tree, name), key, buf)
			got := buf[:max(0, n)]
			// Extracting a v2 capability in a user namespace writes a v3
			// capability owned by that namespace's host root ID. Verify both
			// payload and root mapping before comparing its original v2 value.
			if key == "security.capability" && n == 24 &&
				binary.LittleEndian.Uint32(got) == 0x03000001 &&
				binary.LittleEndian.Uint32(got[20:]) == uint32(os.Geteuid()) {
				got = append([]byte(nil), got[:20]...)
				binary.LittleEndian.PutUint32(got, 0x02000001)
			}
			want := []byte(value)
			if key == "security.capability" && len(want) == 24 {
				want = append([]byte(nil), want...)
				id := binary.LittleEndian.Uint32(want[20:])
				binary.LittleEndian.PutUint32(
					want[20:],
					mappedHostID(
						t,
						filepath.Join(filepath.Dir(tree), "uid_map"),
						id,
					),
				)
			}
			if key == "system.posix_acl_access" {
				want = append([]byte(nil), want...)
				for offset := 4; offset+8 <= len(want); offset += 8 {
					tag := binary.LittleEndian.Uint16(want[offset:])
					if tag == 2 || tag == 8 {
						kind := "uid_map"
						if tag == 8 {
							kind = "gid_map"
						}
						id := binary.LittleEndian.Uint32(want[offset+4:])
						binary.LittleEndian.PutUint32(
							want[offset+4:],
							mappedHostID(
								t,
								filepath.Join(filepath.Dir(tree), kind),
								id,
							),
						)
					}
				}
			}
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf(
					"%s xattr %s: %x %v; expected %x",
					name,
					key,
					got,
					err,
					want,
				)
			}
		}
	}
	if materialization == LayeredEROFSLayout {
		if m.Format != 2 || !m.sharedLayered() ||
			m.Builder != imageBuilder(m.Tools) {
			t.Fatal(
				"shared materialization lacks its new immutable builder identity",
			)
		}
		return
	}
	if m.Format != 3 || m.Layout != MergedOCIEROFSLayout ||
		m.Builder != mergedOCIBuilder(m.Tools) ||
		m.Builder == imageBuilder(m.Tools) {
		t.Fatal("merged compatibility lacks a distinct immutable identity")
	}
	bindings, err := m.ReadOnlyBindings(
		filepath.Join(e.opts.StateDir, "images", m.Digest[7:]),
	)
	if err != nil || len(bindings) != 1 ||
		bindings[0].GuestPath != "/images/root.erofs" ||
		bindings[0].Serial != "sandbox-root" {
		t.Fatalf("merged runtime binding: %+v %v", bindings, err)
	}
	if parameter, err := m.DataKernelParameter(); err != nil ||
		parameter != "" {
		t.Fatalf("merged data kernel parameter: %q %v", parameter, err)
	}
}

func TestActualMergedOCIWhiteoutOpaqueAndReplacement(t *testing.T) {
	base := append(
		bootLayerEntries(),
		dirEntry("opaque"),
		fileEntry("opaque/old", "hidden"),
		fileEntry("removed", "hidden"),
		fileEntry("replace", "old"),
	)
	top := []tarEntry{
		fileEntry("removed-again", "same layer"),
		fileEntry(".wh.removed", ""),
		fileEntry(".wh.replace", ""),
		fileEntry("replace", "new"),
		fileEntry("opaque/.wh..wh..opq", ""),
		fileEntry("opaque/new", "new child"),
	}
	e, _, m := importMergedFixture(t, base, top)
	tree := extractMergedManifest(t, e, m)
	for name, want := range map[string]string{"replace": "new", "opaque/new": "new child", "removed-again": "same layer"} {
		got, err := os.ReadFile(filepath.Join(tree, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s: %q %v", name, got, err)
		}
	}
	for _, name := range []string{"removed", "opaque/old", ".wh.removed", "opaque/.wh..wh..opq"} {
		if _, err := os.Lstat(filepath.Join(tree, name)); !os.IsNotExist(err) {
			t.Fatalf("whiteout/opaque not applied: %s %v", name, err)
		}
	}
	rootTop := append(
		bootLayerEntries(),
		fileEntry(".wh..wh..opq", ""),
		fileEntry("survivor", "top"),
	)
	e2, _, m2 := importMergedFixture(t, base, rootTop)
	tree2 := extractMergedManifest(t, e2, m2)
	if _, err := os.Stat(filepath.Join(tree2, "replace")); !os.IsNotExist(err) {
		t.Fatalf("root opaque retained lower file: %v", err)
	}
	if data, err := os.ReadFile(
		filepath.Join(tree2, "survivor"),
	); err != nil ||
		string(data) != "top" {
		t.Fatalf("root opaque lost same-layer file: %q %v", data, err)
	}
}

func TestActualMergedOCIMaterializationConflict(t *testing.T) {
	e, layout, m := importMergedFixture(t, bootLayerEntries())
	_, err := e.ImportOCI(
		context.Background(),
		ImportOptions{Layout: layout, Digest: m.Digest},
	)
	var apiErr *api.Error
	if !errors.As(err, &apiErr) || apiErr.Code != api.Conflict {
		t.Fatalf("digest layout change must conflict: %v", err)
	}
	if again, err := e.ImportOCI(
		context.Background(),
		ImportOptions{
			Layout:          layout,
			Digest:          m.Digest,
			Materialization: MergedOCIEROFSLayout,
		},
	); err != nil ||
		again.Builder != m.Builder {
		t.Fatalf("same layout reimport: %v", err)
	}
	layer := putOCILayer(t, layout, bootLayerEntries(), false)
	image := putOCIManifest(
		t,
		layout,
		"layered-first",
		[]ociLayerFixture{layer},
	)
	opts := ImportOptions{
		Layout:   layout,
		Digest:   image,
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	}
	if _, err := e.ImportOCI(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	opts.Materialization = MergedOCIEROFSLayout
	_, err = e.ImportOCI(context.Background(), opts)
	if !errors.As(err, &apiErr) || apiErr.Code != api.Conflict {
		t.Fatalf("layered-first materialization change must conflict: %v", err)
	}
}

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
	entries := append(
		bootLayerEntries(),
		hardEntry("alias", "target"),
		fileEntry("target", "data"),
	)
	image := putOCIManifest(
		t,
		layout,
		"forward",
		[]ociLayerFixture{putOCILayer(t, layout, entries, false)},
	)
	e := openTest(t, t.TempDir(), newTestBackend(), true, nil)
	defer e.Close()
	_, err := e.ImportOCI(
		context.Background(),
		ImportOptions{
			Layout:          layout,
			Digest:          image,
			Materialization: MergedOCIEROFSLayout,
			MaxBytes:        4 << 20,
			MaxFiles:        1000,
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "link: no such file or directory") {
		t.Fatalf(
			"expected explicit native umoci forward-hardlink failure: %v",
			err,
		)
	}
	var count int
	if err := e.db.QueryRow("SELECT count(*) FROM meta WHERE key=?", "image:"+image).
		Scan(&count); err != nil ||
		count != 0 {
		t.Fatalf("failed unpack published an image: %d %v", count, err)
	}
}

func TestActualMergedOCISymlinkHardlinkAndConfinement(t *testing.T) {
	base := append(
		bootLayerEntries(),
		tarEntry{
			header: tar.Header{
				Name:     "sym",
				Typeflag: tar.TypeSymlink,
				Linkname: "/guest/target",
				Mode:     0o777,
			},
		},
		fileEntry("victim", "unchanged"),
	)
	top := []tarEntry{
		hardEntry("alias", "sym"),
		{
			header: tar.Header{
				Name:     "escape",
				Typeflag: tar.TypeSymlink,
				Linkname: "/work",
				Mode:     0o777,
			},
		},
		fileEntry("escape/victim", "inside guest"),
	}
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
	if data, err := os.ReadFile(
		filepath.Join(tree, "victim"),
	); err != nil ||
		string(data) != "unchanged" {
		t.Fatalf("symlink parent escaped root: %q %v", data, err)
	}
	if data, err := os.ReadFile(
		filepath.Join(
			e.opts.StateDir,
			"imports",
			m.Digest[7:],
			"output",
			"victim",
		),
	); !os.IsNotExist(
		err,
	) {
		t.Fatalf("symlink parent wrote builder workspace: %q %v", data, err)
	}
}

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
