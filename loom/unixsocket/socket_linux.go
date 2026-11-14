// Package unixsocket anchors Linux Unix socket operations at an opened parent
// directory, so physical runtime paths do not consume sockaddr_un's path budget.
package unixsocket

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func anchor(path string) (*os.File, string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Base(path) == "/" {
		return nil, "", fmt.Errorf("unix socket path must be absolute and clean")
	}
	descriptor, err := unix.Open(filepath.Dir(path), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, "", err
	}
	directory := os.NewFile(uintptr(descriptor), "socket-parent")
	return directory, fmt.Sprintf("/proc/self/fd/%d/%s", descriptor, filepath.Base(path)), nil
}

func Dial(ctx context.Context, path string) (net.Conn, error) {
	directory, short, err := anchor(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = directory.Close()
	}()
	return (&net.Dialer{}).DialContext(ctx, "unix", short)
}

// Listen never unlinks on Close: the owner must remove the canonical entry only
// after checking its original inode. The temporary descriptor path cannot own it.
func Listen(path string) (*net.UnixListener, error) {
	directory, short, err := anchor(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = directory.Close()
	}()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{
		Name: short,
		Net:  "unix",
	})
	if err != nil {
		return nil, err
	}
	listener.SetUnlinkOnClose(false)
	return listener, nil
}
