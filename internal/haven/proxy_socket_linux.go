package haven

import (
	"context"
	"os"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/haven/internal/egress"
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
		return loom.Resource{}, loom.Fail(loom.Conflict, "proxy binding ownership changed")
	}
	source, err := service.dataResource(ctx, sandbox)
	if err != nil {
		return loom.Resource{}, err
	}
	resource := source.Input.Resource
	if guestSocket(resource) != binding.VSockPath {
		return resource, loom.Fail(loom.Conflict, "proxy runtime path changed")
	}
	observation, err := service.opts.Backend.Inspect(ctx, resource)
	if err != nil {
		return resource, err
	}
	if !observation.Alive || observation.Paused || !observation.Booted {
		return resource, loom.Fail(loom.Conflict, "proxy instance is not running")
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
		return loom.Fail(loom.Invalid, "proxy socket is outside the VM binding")
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
		return loom.Fail(loom.Conflict, "VMM socket lacks an unprivileged owner")
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
		return loom.Fail(loom.Conflict, "proxy socket identity or permissions changed")
	}
	if err = unix.Fchownat(fd, "", int(identity.Uid), int(identity.Gid), unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	current, err := service.proxyResource(ctx, binding)
	if err != nil {
		return err
	}
	if current.Instance != resource.Instance || current.VMID != resource.VMID || current.RunDir != resource.RunDir {
		return loom.Fail(loom.Conflict, "VM changed during proxy handoff")
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
