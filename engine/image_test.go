package engine

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func layerFixture(t *testing.T, headers ...*tar.Header) (string, string) {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, h := range headers {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Size > 0 {
			if _, err := tw.Write(make([]byte, h.Size)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "layer.tar")
	if err := os.WriteFile(file, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(buf.Bytes())
	return file, "sha256:" + hex.EncodeToString(h[:])
}
func TestLayerValidationRejectsTraversalSpecialFilesAndBounds(t *testing.T) {
	for _, h := range []*tar.Header{{Name: "../../escape", Typeflag: tar.TypeReg}, {Name: "/absolute", Typeflag: tar.TypeReg}, {Name: "bad//path", Typeflag: tar.TypeReg}, {Name: "dev/evil", Typeflag: tar.TypeChar}, {Name: "pipe", Typeflag: tar.TypeFifo}, {Name: "link", Typeflag: tar.TypeLink, Linkname: "../outside"}, {Name: ".wh.hidden", Typeflag: tar.TypeReg, Size: 1}} {
		file, diff := layerFixture(t, h)
		if _, _, err := validateLayer(file, v1.MediaTypeImageLayer, diff, 1<<20, 100); err == nil {
			t.Fatalf("unsafe header accepted %+v", h)
		}
	}
	file, diff := layerFixture(t, &tar.Header{Name: "ok", Typeflag: tar.TypeReg, Size: 1024})
	if _, _, err := validateLayer(file, v1.MediaTypeImageLayer, diff, 100, 100); err == nil {
		t.Fatal("expanded-byte bound ignored")
	}
	if _, _, err := validateLayer(file, v1.MediaTypeImageLayer, diff, 1<<20, 0); err == nil {
		t.Fatal("file count bound ignored")
	}
	if _, _, err := validateLayer(file, v1.MediaTypeImageLayer, "sha256:"+string(bytes.Repeat([]byte("0"), 64)), 1<<20, 100); err == nil {
		t.Fatal("diff_id mismatch accepted")
	}
}
func TestLayerValidationKeepsSafeLinksAndWhiteouts(t *testing.T) {
	file, diff := layerFixture(t, &tar.Header{Name: "etc/", Typeflag: tar.TypeDir}, &tar.Header{Name: "etc/.wh.removed", Typeflag: tar.TypeReg}, &tar.Header{Name: "etc/.wh..wh..opq", Typeflag: tar.TypeReg}, &tar.Header{Name: "regular", Typeflag: tar.TypeReg}, &tar.Header{Name: "hard", Typeflag: tar.TypeLink, Linkname: "regular"}, &tar.Header{Name: "symbolic", Typeflag: tar.TypeSymlink, Linkname: "/absolute/guest/path", Mode: 0777})
	if _, n, err := validateLayer(file, v1.MediaTypeImageLayer, diff, 1<<20, 100); err != nil || n != 6 {
		t.Fatalf("valid layer rejected n=%d %v", n, err)
	}
}
func TestLayerValidationPreservesLinuxBackslashNames(t *testing.T) {
	for _, merged := range []bool{false, true} {
		file, diff := layerFixture(t,
			&tar.Header{Name: `system-systemd\x2dcryptsetup.slice`, Typeflag: tar.TypeReg, Mode: 0644},
			&tar.Header{Name: "alias", Typeflag: tar.TypeLink, Linkname: `system-systemd\x2dcryptsetup.slice`, Mode: 0644},
		)
		if _, n, err := validateLayerFor(file, v1.MediaTypeImageLayer, diff, 1<<20, 10, merged); err != nil || n != 2 {
			t.Fatalf("valid Linux name rejected (merged=%v): entries=%d %v", merged, n, err)
		}
		for _, name := range []string{`../system\x2dslice`, `safe/../system\x2dslice`, `/system\x2dslice`} {
			file, diff := layerFixture(t, &tar.Header{Name: name, Typeflag: tar.TypeReg})
			if _, _, err := validateLayerFor(file, v1.MediaTypeImageLayer, diff, 1<<20, 10, merged); err == nil {
				t.Fatalf("traversal accepted: %q", name)
			}
		}
	}
}

func TestWritableCloneIsIndependent(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source")
	dst := filepath.Join(dir, "clone")
	if err := os.WriteFile(src, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(src, dst, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte("modified"), 0600); err != nil {
		t.Fatal(err)
	}
	a, _ := os.Stat(src)
	b, _ := os.Stat(dst)
	data, err := os.ReadFile(dst)
	if err != nil || string(data) != "original" || os.SameFile(a, b) {
		t.Fatal("writable disk copy shared mutable inode")
	}
}
