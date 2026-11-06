package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	return errors.Join(f.Sync(), f.Close())
}

func writeJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return atomicWrite(path, b, 0o600)
}

func atomicWrite(path string, b []byte, mode os.FileMode) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".pending-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer func() { _ = os.Remove(name) }()
	if e = f.Chmod(mode); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	return syncDir(filepath.Dir(path))
}

func readJSON(path string, v any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}

func digestFile(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func regular(path string) error {
	s, e := os.Lstat(path)
	if e != nil {
		return e
	}
	if !s.Mode().IsRegular() {
		return fmt.Errorf("not a regular file: %s", path)
	}
	return nil
}

// copyFile first attempts a filesystem clone, otherwise copies sparse extents.
// Its caller must keep the source stable until it returns. It deliberately does
// not fsync: snapshot capture must resume the source before publication syncing.
func copyFile(src, dst string, mode os.FileMode) error {
	if e := regular(src); e != nil {
		return e
	}
	in, e := os.Open(src)
	if e != nil {
		return e
	}
	defer func() { _ = in.Close() }()
	st, e := in.Stat()
	if e != nil {
		return e
	}
	out, e := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_RDWR, mode)
	if e != nil {
		return e
	}
	ok := false
	defer func() {
		_ = out.Close()
		if !ok {
			_ = os.Remove(dst)
		}
	}()
	if e = unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); e != nil {
		if e = out.Truncate(st.Size()); e != nil {
			return e
		}
		for pos := int64(0); pos < st.Size(); {
			data, err := unix.Seek(int(in.Fd()), pos, unix.SEEK_DATA)
			if errors.Is(err, syscall.ENXIO) {
				break
			}
			if errors.Is(err, syscall.EINVAL) ||
				errors.Is(err, syscall.ENOTSUP) {
				data = pos
			} else if err != nil {
				return err
			}
			end, err := unix.Seek(int(in.Fd()), data, unix.SEEK_HOLE)
			if err != nil {
				end = st.Size()
			}
			if end > st.Size() {
				end = st.Size()
			}
			if _, e = in.Seek(data, io.SeekStart); e != nil {
				return e
			}
			if _, e = out.Seek(data, io.SeekStart); e != nil {
				return e
			}
			if _, e = io.CopyN(out, in, end-data); e != nil {
				return e
			}
			pos = end
		}
	}
	if e = out.Chmod(mode); e != nil {
		return e
	}
	if e = out.Close(); e != nil {
		return e
	}
	ok = true
	return nil
}

func syncTree(root string) error {
	var dirs []string
	e := filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			dirs = append(dirs, p)
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("nonregular artifact %s", p)
		}
		f, e := os.Open(p)
		if e != nil {
			return e
		}
		e = f.Sync()
		return errors.Join(e, f.Close())
	})
	if e != nil {
		return e
	}
	for _, dir := range slices.Backward(dirs) {
		if e = syncDir(dir); e != nil {
			return e
		}
	}
	return nil
}

func sealTree(root string) error {
	return filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return os.Chmod(p, 0o700)
		}
		if e = os.Chown(p, 0, 0); e != nil && os.Geteuid() == 0 {
			return e
		}
		return os.Chmod(p, 0o444)
	})
}

func hashTree(root string) (map[string]string, error) {
	m := map[string]string{}
	e := filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("nonregular snapshot artifact")
		}
		rel, _ := filepath.Rel(root, p)
		if rel == "manifest.json" {
			return nil
		}
		h, e := digestFile(p)
		if e == nil {
			m[rel] = h
		}
		return e
	})
	return m, e
}

func verifyFiles(root string, files map[string]string) error {
	for rel, want := range files {
		if filepath.IsAbs(rel) || rel == "." || strings.Contains(rel, "\\") ||
			filepath.Clean(rel) != rel ||
			strings.HasPrefix(rel, "../") {
			return fmt.Errorf("invalid artifact path")
		}
		path := filepath.Join(root, rel)
		if e := regular(path); e != nil {
			return e
		}
		got, e := digestFile(path)
		if e != nil {
			return e
		}
		if got != want {
			return fmt.Errorf("checksum mismatch: %s", rel)
		}
	}
	return nil
}

func freeBytes(path string) (int64, error) {
	var s unix.Statfs_t
	e := unix.Statfs(path, &s)
	return int64(s.Bavail) * int64(s.Bsize), e
}
