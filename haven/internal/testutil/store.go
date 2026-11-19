package testutil

import (
	"crypto/sha256"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

type storeFileSnapshot struct {
	Mode  fs.FileMode
	Inode uint64
	Hash  [32]byte
	Link  string
}

func StoreSnapshot(t *testing.T, directory string) map[string]storeFileSnapshot {
	t.Helper()
	files := map[string]storeFileSnapshot{}
	if err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		value := storeFileSnapshot{
			Mode:  info.Mode(),
			Inode: info.Sys().(*syscall.Stat_t).Ino,
		}
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			value.Hash = sha256.Sum256(data)
		} else if info.Mode()&os.ModeSymlink != 0 {
			value.Link, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		relative, _ := filepath.Rel(directory, path)
		files[relative] = value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files
}
