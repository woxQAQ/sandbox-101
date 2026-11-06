package engine

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/sandbox-engine/api"
)

// InternalDeviceCheck runs only as the already dropped VMM identity. Opening the
// actual devices catches mode, group and ACL restrictions before VM allocation.
func InternalDeviceCheck() error {
	if os.Geteuid() == 0 || os.Getegid() == 0 {
		return fmt.Errorf(
			"device preflight requires the unprivileged VMM identity",
		)
	}
	for _, path := range []string{"/dev/kvm", "/dev/net/tun"} {
		fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC, 0)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		if path == "/dev/kvm" {
			version, ioctlErr := unix.IoctlRetInt(fd, 0xae00)
			if ioctlErr != nil || version != 12 {
				_ = unix.Close(fd)
				return fmt.Errorf(
					"KVM API unavailable: version %d, %v",
					version,
					ioctlErr,
				)
			}
		}
		if err := unix.Close(fd); err != nil {
			return fmt.Errorf("close %s: %w", path, err)
		}
	}
	return nil
}

func (b *CloudHypervisor) checkDeviceIdentity(ctx context.Context) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "--internal-device-check")
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    b.opts.UID,
			Gid:    b.opts.GID,
			Groups: []uint32{},
		},
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return api.Fail(
			api.Incompatible,
			"VMM UID %d GID %d cannot use required devices: %v %.1024s",
			b.opts.UID,
			b.opts.GID,
			err,
			output,
		)
	}
	return nil
}
