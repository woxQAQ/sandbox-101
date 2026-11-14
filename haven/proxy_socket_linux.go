package haven

import (
	"context"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
)

func (service *Service) proxyResource(ctx context.Context, binding egress.Binding) (loom.Resource, error) {
	sandbox, err := service.Get(ctx, Principal{
		Root: true,
	}, binding.SandboxID)
	if err != nil {
		return loom.Resource{}, err
	}
	if sandbox.TenantID != binding.TenantID || sandbox.VMID == "" {
		return loom.Resource{}, fmt.Errorf("%w: proxy binding ownership changed", ErrConflict)
	}
	source, err := service.dataResource(ctx, sandbox)
	if err != nil {
		return loom.Resource{}, err
	}
	resource := source.Input.Resource
	if guestSocket(resource) != binding.VSockPath {
		return resource, fmt.Errorf("%w: proxy runtime path changed", ErrConflict)
	}
	observation, err := service.opts.Backend.Inspect(ctx, resource)
	if err != nil {
		return resource, err
	}
	if !observation.Alive || observation.Paused || !observation.Booted {
		return resource, fmt.Errorf("%w: proxy instance is not running", ErrConflict)
	}
	return resource, nil
}

// PrepareProxySocket transfers access to the exact verified VMM socket owner.
// Both identities are anchored before changing the new portal inode.
func (service *Service) PrepareProxySocket(ctx context.Context, binding egress.Binding, path string, info os.FileInfo) error {
	resource, err := service.proxyResource(ctx, binding)
	if err != nil {
		return err
	}
	if path != binding.VSockPath+"_2049" && path != binding.VSockPath+"_2050" {
		return fmt.Errorf("%w: proxy socket is outside the VM binding", ErrInvalid)
	}
	peer, err := unix.Open(binding.VSockPath, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() {
		_ = unix.Close(peer)
	}()
	var identity unix.Stat_t
	if err = unix.Fstat(peer, &identity); err != nil {
		return err
	}
	if identity.Mode&unix.S_IFMT != unix.S_IFSOCK || identity.Uid == 0 || identity.Gid == 0 {
		return fmt.Errorf("%w: VMM socket lacks an unprivileged owner", ErrConflict)
	}
	fd, err := unix.Open(path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	anchor := os.NewFile(uintptr(fd), "proxy-socket")
	defer func() {
		_ = anchor.Close()
	}()
	actual, err := anchor.Stat()
	if err != nil {
		return err
	}
	if actual.Mode()&os.ModeSocket == 0 || !os.SameFile(info, actual) || actual.Mode().Perm() != 0o600 {
		return fmt.Errorf("%w: proxy socket identity or permissions changed", ErrConflict)
	}
	if err = unix.Fchownat(fd, "", int(identity.Uid), int(identity.Gid), unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	current, err := service.proxyResource(ctx, binding)
	if err != nil {
		return err
	}
	if current.Instance != resource.Instance || current.VMID != resource.VMID || current.RunDir != resource.RunDir {
		return fmt.Errorf("%w: VM changed during proxy handoff", ErrConflict)
	}
	return nil
}

// RecoverStaleSocket supplies ownership permission only. Manager independently
// proves the old listener dead and preserves replaced or reachable sockets.
func (service *Service) RecoverStaleSocket(binding egress.Binding, path string, _ os.FileInfo) bool {
	ctx, cancel := context.WithTimeout(service.ctx, 3*time.Second)
	defer cancel()
	if _, err := service.proxyResource(ctx, binding); err != nil {
		return false
	}
	return path == binding.VSockPath+"_2049" || path == binding.VSockPath+"_2050"
}
