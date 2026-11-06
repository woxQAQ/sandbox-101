package egress

import (
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func (b *binding) listenUnix(path string) (net.Listener, error) {
	info, err := os.Lstat(path)
	if err == nil {
		owner := b.manager.opts.RecoverStaleSocket
		if info.Mode()&os.ModeSocket == 0 || owner == nil ||
			!owner(b.info, path, info) {
			return nil, errors.New("egress: proxy path already exists")
		}
		c, dialErr := net.DialTimeout("unix", path, 100*time.Millisecond)
		if c != nil {
			_ = c.Close()
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) &&
			!errors.Is(dialErr, os.ErrNotExist) {
			return nil, errors.New(
				"egress: proxy socket is active or inaccessible",
			)
		}
		current, err := os.Lstat(path)
		if err == nil {
			if !os.SameFile(info, current) {
				return nil, errors.New("egress: proxy socket ownership changed")
			}
			if err := os.Remove(path); err != nil {
				return nil, errors.New("egress: cannot recover owned socket")
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("egress: proxy socket inaccessible")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("egress: proxy path inaccessible")
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, errors.New("egress: cannot bind proxy socket")
	}
	ln.SetUnlinkOnClose(false)
	info, err = os.Lstat(path)
	if err != nil {
		_ = ln.Close()
		return nil, errors.New("egress: cannot identify proxy socket")
	}
	owned := &ownedListener{Listener: ln, path: path, info: info}
	fd, err := unix.Open(path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = owned.Close()
		return nil, errors.New("egress: cannot open proxy socket identity")
	}
	anchor := os.NewFile(uintptr(fd), "proxy-socket")
	defer func() { _ = anchor.Close() }()
	current, err := anchor.Stat()
	if err != nil || current.Mode()&os.ModeSocket == 0 ||
		!os.SameFile(info, current) {
		_ = owned.Close()
		return nil, errors.New("egress: proxy socket identity changed")
	}
	// proc's descriptor link targets this exact inode even if the directory entry
	// changes. O_NOFOLLOW above prevents chmod of a replacement symlink target.
	if err = os.Chmod("/proc/self/fd/"+strconv.Itoa(fd), 0o600); err != nil {
		_ = owned.Close()
		return nil, errors.New("egress: cannot secure proxy socket")
	}
	if prepare := b.manager.opts.PrepareSocket; prepare != nil {
		if err = prepare(b.info, path, current); err != nil {
			_ = owned.Close()
			return nil, errors.New("egress: cannot grant VMM proxy access")
		}
	}
	if current, err = os.Lstat(
		path,
	); err != nil ||
		!os.SameFile(info, current) {
		_ = owned.Close()
		return nil, errors.New("egress: proxy socket identity changed")
	}
	return owned, nil
}

// Runtime directories must be exclusively owned by the control plane. Checking
// the inode also avoids removing a replacement socket during normal teardown.
type ownedListener struct {
	net.Listener

	path string
	info os.FileInfo
	once sync.Once
	err  error
}

func (l *ownedListener) Close() error {
	l.once.Do(func() {
		l.err = l.Listener.Close()
		if current, err := os.Lstat(
			l.path,
		); err == nil &&
			os.SameFile(l.info, current) {
			if err := os.Remove(l.path); err != nil {
				l.err = errors.Join(l.err, err)
			}
		}
	})
	return l.err
}
