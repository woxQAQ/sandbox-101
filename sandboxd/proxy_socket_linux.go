package sandboxd

import (
	"context"
	"os"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
	"golang.org/x/sys/unix"
)

// PrepareProxySocket grants the VMM access without making a host portal public.
// The engine has already verified the instance; its own vsock inode supplies the
// dropped identity. All ownership changes target an opened socket inode.
func (s *Service) PrepareProxySocket(ctx context.Context, binding egress.Binding, path string, info os.FileInfo) error {
	b, err := s.Get(ctx, Principal{Root: true}, binding.SandboxID)
	if err != nil {
		return err
	}
	if b.TenantID != binding.TenantID || b.VMID == "" {
		return api.Fail(api.Conflict, "proxy binding ownership changed")
	}
	vm, err := s.opts.Engine.GetVM(ctx, b.VMID)
	if err != nil {
		return err
	}
	if !vm.Verified || !vm.AgentReady || vm.Phase != "running" || vm.Instance == "" || vm.VSockPath != binding.VSockPath || (b.Instance != "" && b.Instance != vm.Instance) {
		return api.Fail(api.Conflict, "proxy VM identity is not verified")
	}
	if path != vm.VSockPath+"_2049" && path != vm.VSockPath+"_2050" {
		return api.Fail(api.Invalid, "proxy socket is outside the VM binding")
	}
	peer, err := unix.Open(vm.VSockPath, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(peer)
	var identity unix.Stat_t
	if err = unix.Fstat(peer, &identity); err != nil {
		return err
	}
	if identity.Mode&unix.S_IFMT != unix.S_IFSOCK || identity.Uid == 0 || identity.Gid == 0 {
		return api.Fail(api.Conflict, "VMM socket does not have an unprivileged owner")
	}
	fd, err := unix.Open(path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	anchor := os.NewFile(uintptr(fd), "proxy-socket")
	defer anchor.Close()
	actual, err := anchor.Stat()
	if err != nil {
		return err
	}
	if actual.Mode()&os.ModeSocket == 0 || !os.SameFile(info, actual) || actual.Mode().Perm() != 0600 {
		return api.Fail(api.Conflict, "proxy socket identity or permissions changed")
	}
	if err = unix.Fchownat(fd, "", int(identity.Uid), int(identity.Gid), unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	current, err := s.opts.Engine.GetVM(ctx, b.VMID)
	if err != nil {
		return err
	}
	if !current.Verified || current.Instance != vm.Instance || current.VSockPath != vm.VSockPath {
		return api.Fail(api.Conflict, "VM changed while granting proxy access")
	}
	return nil
}
