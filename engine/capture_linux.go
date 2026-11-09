package engine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// captureBoundary pins the trusted operation parent and the private publication
// directory. Only its ch child is ever delegated to the VMM identity.
type captureBoundary struct {
	Path        string     `json:"path"`
	Parent      gcIdentity `json:"parent"`
	Publication gcIdentity `json:"publication"`
}

// openCaptureDir resolves every component without following links, including
// ancestors. Fds, rather than a prior pathname check, authorize later effects.
func openCaptureDir(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("invalid capture directory")
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for component := range strings.SplitSeq(strings.TrimPrefix(path, "/"), "/") {
		next, x := unix.Openat(
			fd,
			component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
			0,
		)
		_ = unix.Close(fd)
		if x != nil {
			return nil, x
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), path), nil
}

func captureChild(parent *os.File, name string, mode uint32) (*os.File, error) {
	if err := unix.Mkdirat(
		int(parent.Fd()),
		name,
		mode,
	); err != nil &&
		!errors.Is(err, unix.EEXIST) {
		return nil, err
	}
	fd, err := unix.Openat(
		int(parent.Fd()),
		name,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0,
	)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func privateCaptureDir(f *os.File, mode os.FileMode) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) ||
		st.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("incompatible capture parent ownership: %s", f.Name())
	}
	return f.Chmod(mode)
}

func prepareCapture(r Resource, operation string) (*captureBoundary, error) {
	if !gcID(operation, "op-") {
		return nil, fmt.Errorf("invalid capture operation")
	}
	root, err := openCaptureDir(filepath.Join(r.Dir, "capture"))
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	if err = privateCaptureDir(root, 0o711); err != nil {
		return nil, err
	}
	parent, err := captureChild(root, operation, 0o711)
	if err != nil {
		return nil, err
	}
	defer func() { _ = parent.Close() }()
	if err = privateCaptureDir(parent, 0o711); err != nil {
		return nil, err
	}
	ch, err := captureChild(parent, "ch", 0o700)
	if err != nil {
		return nil, err
	}
	defer func() { _ = ch.Close() }()
	stage, err := captureChild(parent, "publication", 0o700)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stage.Close() }()
	if err = privateCaptureDir(stage, 0o700); err != nil {
		return nil, err
	}
	for _, f := range []*os.File{ch, stage, parent, root} {
		if err = f.Sync(); err != nil {
			return nil, err
		}
	}
	p, err := parent.Stat()
	if err != nil {
		return nil, err
	}
	s, err := stage.Stat()
	if err != nil {
		return nil, err
	}
	return &captureBoundary{
		filepath.Join(r.Dir, "capture", operation),
		gcNode(p),
		gcNode(s),
	}, nil
}

func (c *captureBoundary) open() (*os.File, error) {
	f, err := openCaptureDir(c.Path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err == nil && gcNode(st) != c.Parent {
		err = fmt.Errorf("capture parent identity changed")
	}
	if err == nil {
		err = privateCaptureDir(f, 0o711)
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func delegateCapture(r Resource, dst string, uid, gid int) error {
	rel, err := filepath.Rel(filepath.Join(r.Dir, "capture"), dst)
	parts := strings.Split(rel, string(filepath.Separator))
	if err != nil || len(parts) != 2 || !gcID(parts[0], "op-") ||
		parts[1] != "ch" {
		return fmt.Errorf("invalid capture destination")
	}
	parent, err := openCaptureDir(filepath.Dir(dst))
	if err != nil {
		return err
	}
	defer func() { _ = parent.Close() }()
	if err = privateCaptureDir(parent, 0o711); err != nil {
		return err
	}
	ch, err := captureChild(parent, "ch", 0o700)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	if err = ch.Chown(uid, gid); err != nil {
		return err
	}
	if err = ch.Chmod(0o700); err != nil {
		return err
	}
	return ch.Sync()
}

// materializeCapture reads only the fixed CH topology and bounded regular file
// descriptors. Each destination is a fresh inode; an old writer fd cannot reach it.
func materializeCapture(c *captureBoundary, memoryMiB int64) error {
	parent, err := c.open()
	if err != nil {
		return err
	}
	defer func() { _ = parent.Close() }()
	stage, err := captureChild(parent, "publication", 0o700)
	if err != nil {
		return err
	}
	defer func() { _ = stage.Close() }()
	st, err := stage.Stat()
	if err != nil {
		return err
	}
	if gcNode(st) != c.Publication {
		return fmt.Errorf("publication identity changed")
	}
	source, err := captureChild(parent, "ch", 0o700)
	if err != nil {
		return err
	}
	defer func() { _ = source.Close() }()
	names, err := source.Readdirnames(4)
	if err != nil && err != io.EOF {
		return err
	}
	if len(names) != 3 {
		return fmt.Errorf("unexpected capture artifact count")
	}
	limits := map[string]int64{
		"config.json":   16 << 20,
		"state.json":    64 << 20,
		"memory-ranges": memoryMiB << 20,
	}
	dest, err := captureChild(stage, "ch", 0o700)
	if err != nil {
		return err
	}
	defer func() { _ = dest.Close() }()
	remaining := (memoryMiB << 20) + (80 << 20)
	for _, name := range names {
		limit, ok := limits[name]
		if !ok {
			return fmt.Errorf("unexpected capture artifact %q", name)
		}
		fd, x := unix.Openat(
			int(source.Fd()),
			name,
			unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC,
			0,
		)
		if x != nil {
			return x
		}
		in := os.NewFile(uintptr(fd), name)
		st, x := in.Stat()
		if x == nil &&
			(!st.Mode().IsRegular() || st.Size() < 0 || st.Size() > limit || st.Size() > remaining) {
			x = fmt.Errorf("nonregular or oversized capture artifact %s", name)
		}
		if x != nil {
			_ = in.Close()
			return x
		}
		// A replay may replace only our private, unpublished destination entries.
		if x = unix.Unlinkat(
			int(dest.Fd()),
			name,
			0,
		); x != nil &&
			!errors.Is(x, unix.ENOENT) {
			_ = in.Close()
			return x
		}
		outfd, x := unix.Openat(
			int(dest.Fd()),
			name,
			unix.O_CREAT|unix.O_EXCL|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC,
			0o600,
		)
		if x != nil {
			_ = in.Close()
			return x
		}
		out := os.NewFile(uintptr(outfd), name)
		if x = unix.IoctlFileClone(outfd, fd); x != nil {
			_, x = io.CopyN(out, in, st.Size())
		}
		// Bound even a reflink raced by a writer extending the source inode.
		if x == nil {
			x = out.Truncate(st.Size())
		}
		remaining -= st.Size()
		x = errors.Join(x, out.Close(), in.Close())
		if x != nil {
			return x
		}
	}
	return nil
}
