// Package localuds owns the private local administration transport.
package localuds

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type peerKey struct{}

// PeerContext authenticates the kernel-supplied UID, independent of HTTP headers.
func PeerContext(ctx context.Context, conn net.Conn) context.Context {
	socket, ok := conn.(syscall.Conn)
	if !ok {
		return ctx
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return ctx
	}
	var peer *unix.Ucred
	var peerErr error
	if err = raw.Control(func(fd uintptr) {
		peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil || peerErr != nil || peer == nil {
		return ctx
	}
	if peer.Uid != 0 && peer.Uid != uint32(os.Geteuid()) {
		return ctx
	}
	return context.WithValue(ctx, peerKey{}, true)
}

func Authorized(ctx context.Context) bool {
	value, _ := ctx.Value(peerKey{}).(bool)
	return value
}

type Listener struct {
	*net.UnixListener

	path     string
	identity os.FileInfo
}

func Open(path string) (*Listener, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("management socket must be absolute and clean")
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, err
	}
	directory, err := os.Lstat(parent)
	if err != nil {
		return nil, err
	}
	owner, ok := directory.Sys().(*syscall.Stat_t)
	if !ok || !directory.IsDir() || directory.Mode()&os.ModeSymlink != 0 || directory.Mode().Perm()&0o022 != 0 || owner.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("management directory is not privately controlled")
	}
	if previous, err := os.Lstat(path); err == nil {
		stat, ok := previous.Sys().(*syscall.Stat_t)
		if !ok || previous.Mode()&os.ModeSocket == 0 || previous.Mode().Perm() != 0o600 || stat.Uid != uint32(os.Geteuid()) {
			return nil, fmt.Errorf("management socket path has an unknown owner or type")
		}
		connection, dialErr := net.DialTimeout("unix", path, 250*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			return nil, fmt.Errorf("management socket already serves requests")
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
			return nil, dialErr
		}
		current, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if !os.SameFile(previous, current) {
			return nil, fmt.Errorf("management socket identity changed")
		}
		if err = os.Remove(path); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	socket, err := net.ListenUnix("unix", &net.UnixAddr{
		Name: path,
		Net:  "unix",
	})
	if err != nil {
		return nil, err
	}
	socket.SetUnlinkOnClose(false)
	if err = os.Chmod(path, 0o600); err != nil {
		_ = socket.Close()
		return nil, err
	}
	identity, err := os.Lstat(path)
	if err != nil {
		_ = socket.Close()
		return nil, err
	}
	return &Listener{
		UnixListener: socket,
		path:         path,
		identity:     identity,
	}, nil
}

func (listener *Listener) Close() error {
	closeErr := listener.UnixListener.Close()
	current, err := os.Lstat(listener.path)
	if os.IsNotExist(err) {
		return closeErr
	}
	if err != nil {
		return errors.Join(closeErr, err)
	}
	if !os.SameFile(listener.identity, current) {
		return closeErr
	}
	return errors.Join(closeErr, os.Remove(listener.path))
}
