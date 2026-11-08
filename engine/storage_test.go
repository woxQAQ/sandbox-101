package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritableCloneIsIndependent(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source")
	dst := filepath.Join(dir, "clone")
	if err := os.WriteFile(src, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(src, dst, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte("modified"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, _ := os.Stat(src)
	b, _ := os.Stat(dst)
	data, err := os.ReadFile(dst)
	if err != nil || string(data) != "original" || os.SameFile(a, b) {
		t.Fatal("writable disk copy shared mutable inode")
	}
}
