package fixtures

import (
	"crypto/sha256"
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/woxQAQ/haven/internal/recovery"
	"github.com/woxQAQ/haven/store"
)

func OpenStore(directory string, initialize bool) (*store.State, error) {
	return store.Open(directory, initialize, recovery.NewRecovery().Validation())
}

func StoreTestSQL(t *testing.T, path, statement string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.Exec(statement); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

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
