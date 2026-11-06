package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"golang.org/x/sys/unix"
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
			args := imageToolSandbox(t.TempDir(), []imageMount{{binary, "/tools/umoci"}}, true)
			err = runMappedImageTool(context.Background(), append(args, "--", "/tools/umoci", "--version"))
		} else {
			err = x
		}
	}
	if err != nil {
		if os.Getenv("SANDBOX_REQUIRE_EROFS_TESTS") == "1" {
			t.Fatal(err)
		}
		t.Skipf("actual OCI compatibility tool/namespace test unavailable: %v", err)
	}
}

func importMergedFixture(t *testing.T, layers ...[]tarEntry) (*Engine, string, ImageManifest) {
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
	m, err := e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: image, Materialization: MergedOCIEROFSLayout, MaxBytes: 4 << 20, MaxFiles: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return e, layout, m
}

func extractMergedManifest(t *testing.T, e *Engine, m ImageManifest) string {
	t.Helper()
	bindings, err := m.ReadOnlyBindings(filepath.Join(e.opts.StateDir, "images", m.Digest[7:]))
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
	fsckArgs := []string{"--extract=/work/tree", "--xattrs", "--no-preserve-owner", "--preserve-perms"}
	for i, binding := range bindings {
		reads = append(reads, imageMount{binding.HostPath, binding.GuestPath})
		if i != 0 {
			fsckArgs = append(fsckArgs, "--device="+binding.GuestPath)
		}
	}
	fsckArgs = append(fsckArgs, bindings[0].GuestPath)
	args := imageToolSandbox(out, reads, true)
	args = append(args, "--", "/tools/sh", "-euc", `/tools/fsck "$@"
while read -r inner outer count; do printf '%s %s %s\n' "$inner" "$outer" "$count"; done < /proc/self/uid_map > /work/uid_map
while read -r inner outer count; do printf '%s %s %s\n' "$inner" "$outer" "$count"; done < /proc/self/gid_map > /work/gid_map`, "extractor")
	args = append(args, fsckArgs...)
	if err = runMappedImageTool(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(out, "tree")
}

func dumpMergedPath(t *testing.T, e *Engine, m ImageManifest, path string) string {
	t.Helper()
	image := filepath.Join(e.opts.StateDir, "images", m.Digest[7:], "root.erofs")
	if len(m.Artifacts) != 0 {
		image = filepath.Join(e.opts.StateDir, m.Artifacts[0].Path)
	}
	out, err := exec.Command("dump.erofs", "--path="+path, image).CombinedOutput()
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
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
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

func TestActualMergedOCIHardlinksAndMetadata(t *testing.T) {
	testOCIHardlinksAndMetadata(t, MergedOCIEROFSLayout)
}

func TestActualSharedOCIHardlinksAndMetadata(t *testing.T) {
	testOCIHardlinksAndMetadata(t, LayeredEROFSLayout)
}

func testOCIHardlinksAndMetadata(t *testing.T, materialization string) {
	root := dirEntry(".")
	root.header.Format = tar.FormatPAX
	root.header.Mode, root.header.Uid, root.header.Gid = 0751, 42, 43
	root.header.ModTime = time.Unix(1700000010, 123456789)
	root.header.Xattrs = map[string]string{"user.root": "root retained"}
	parent := dirEntry("inherited")
	parent.header.Format = tar.FormatPAX
	parent.header.Mode = 02750
	parent.header.ModTime = time.Unix(1700000020, 234567890)
	parent.header.Xattrs = map[string]string{"user.directory": "inherited retained"}
	file := fileEntry("target", "cross-layer content")
	file.header.Format = tar.FormatPAX
	file.header.Mode = 06740
	file.header.ModTime = time.Unix(1700000030, 345678901)
	capability := make([]byte, 20)
	binary.LittleEndian.PutUint32(capability, 0x02000001)
	binary.LittleEndian.PutUint32(capability[4:], 1<<10)
	file.header.Xattrs = map[string]string{"user.fixture": "retained", "security.capability": string(capability)}
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
	aclFile.header.Xattrs = map[string]string{"system.posix_acl_access": string(acl)}
	capNS := fileEntry("cap-ns", "namespaced capability")
	capV3 := append(append([]byte(nil), capability...), make([]byte, 4)...)
	binary.LittleEndian.PutUint32(capV3, 0x03000001)
	binary.LittleEndian.PutUint32(capV3[20:], 333)
	capNS.header.Xattrs = map[string]string{"security.capability": string(capV3)}
	base := append(bootLayerEntries(), root, parent, file, aclFile, capNS)
	top := []tarEntry{hardEntry("alias", "target"), fileEntry("inherited/new", "new child")}
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
		if err != nil || st.Mode().Perm() != os.FileMode(header.Mode&0777) || !st.ModTime().Equal(header.ModTime) {
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
		if err := unix.Lstat(filepath.Join(tree, name), &native); err != nil || native.Mode&07777 != uint32(header.Mode) {
			t.Fatalf("%s permission bits: %#o %v; expected %#o", name, native.Mode&07777, err, header.Mode)
		}
		for key, value := range header.Xattrs {
			buf := make([]byte, 4096)
			n, err := unix.Getxattr(filepath.Join(tree, name), key, buf)
			got := buf[:max(0, n)]
			// Extracting a v2 capability in a user namespace writes a v3
			// capability owned by that namespace's host root ID. Verify both
			// payload and root mapping before comparing its original v2 value.
			if key == "security.capability" && n == 24 && binary.LittleEndian.Uint32(got) == 0x03000001 && binary.LittleEndian.Uint32(got[20:]) == uint32(os.Geteuid()) {
				got = append([]byte(nil), got[:20]...)
				binary.LittleEndian.PutUint32(got, 0x02000001)
			}
			want := []byte(value)
			if key == "security.capability" && len(want) == 24 {
				want = append([]byte(nil), want...)
				id := binary.LittleEndian.Uint32(want[20:])
				binary.LittleEndian.PutUint32(want[20:], mappedHostID(t, filepath.Join(filepath.Dir(tree), "uid_map"), id))
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
						binary.LittleEndian.PutUint32(want[offset+4:], mappedHostID(t, filepath.Join(filepath.Dir(tree), kind), id))
					}
				}
			}
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s xattr %s: %x %v; expected %x", name, key, got, err, want)
			}
		}
	}
	if materialization == LayeredEROFSLayout {
		if m.Format != 2 || !m.sharedLayered() || m.Builder != imageBuilder(m.Tools) {
			t.Fatal("shared materialization lacks its new immutable builder identity")
		}
		return
	}
	if m.Format != 3 || m.Layout != MergedOCIEROFSLayout || m.Builder != mergedOCIBuilder(m.Tools) || m.Builder == imageBuilder(m.Tools) {
		t.Fatal("merged compatibility lacks a distinct immutable identity")
	}
	bindings, err := m.ReadOnlyBindings(filepath.Join(e.opts.StateDir, "images", m.Digest[7:]))
	if err != nil || len(bindings) != 1 || bindings[0].GuestPath != "/images/root.erofs" || bindings[0].Serial != "sandbox-root" {
		t.Fatalf("merged runtime binding: %+v %v", bindings, err)
	}
	if parameter, err := m.DataKernelParameter(); err != nil || parameter != "" {
		t.Fatalf("merged data kernel parameter: %q %v", parameter, err)
	}
}

func TestActualMergedOCIWhiteoutOpaqueAndReplacement(t *testing.T) {
	base := append(bootLayerEntries(), dirEntry("opaque"), fileEntry("opaque/old", "hidden"), fileEntry("removed", "hidden"), fileEntry("replace", "old"))
	top := []tarEntry{fileEntry("removed-again", "same layer"), fileEntry(".wh.removed", ""), fileEntry(".wh.replace", ""), fileEntry("replace", "new"), fileEntry("opaque/.wh..wh..opq", ""), fileEntry("opaque/new", "new child")}
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
	rootTop := append(bootLayerEntries(), fileEntry(".wh..wh..opq", ""), fileEntry("survivor", "top"))
	e2, _, m2 := importMergedFixture(t, base, rootTop)
	tree2 := extractMergedManifest(t, e2, m2)
	if _, err := os.Stat(filepath.Join(tree2, "replace")); !os.IsNotExist(err) {
		t.Fatalf("root opaque retained lower file: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(tree2, "survivor")); err != nil || string(data) != "top" {
		t.Fatalf("root opaque lost same-layer file: %q %v", data, err)
	}
}

func TestActualMergedOCIMaterializationConflict(t *testing.T) {
	e, layout, m := importMergedFixture(t, bootLayerEntries())
	_, err := e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: m.Digest})
	var apiErr *api.Error
	if !errors.As(err, &apiErr) || apiErr.Code != api.Conflict {
		t.Fatalf("digest layout change must conflict: %v", err)
	}
	if again, err := e.ImportOCI(context.Background(), ImportOptions{Layout: layout, Digest: m.Digest, Materialization: MergedOCIEROFSLayout}); err != nil || again.Builder != m.Builder {
		t.Fatalf("same layout reimport: %v", err)
	}
	layer := putOCILayer(t, layout, bootLayerEntries(), false)
	image := putOCIManifest(t, layout, "layered-first", []ociLayerFixture{layer})
	opts := ImportOptions{Layout: layout, Digest: image, MaxBytes: 4 << 20, MaxFiles: 1000}
	if _, err := e.ImportOCI(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	opts.Materialization = MergedOCIEROFSLayout
	_, err = e.ImportOCI(context.Background(), opts)
	if !errors.As(err, &apiErr) || apiErr.Code != api.Conflict {
		t.Fatalf("layered-first materialization change must conflict: %v", err)
	}
}
