package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type launchSpec struct {
	Resource Resource `json:"resource"`
	Binary   string   `json:"binary"`
	Root     string   `json:"root"`
	UID      uint32   `json:"uid"`
	GID      uint32   `json:"gid"`
}

// InternalLaunch is only called by engined's internal child entry. The kernel
// has already created its mount/PID/IPC/UTS namespaces and placed it directly in
// its cgroup. It execs CH in this same process: no resident supervisor exists.
func InternalLaunch(specPath string) error {
	runtime.LockOSThread()
	if os.Geteuid() != 0 {
		return fmt.Errorf("internal launcher requires root")
	}
	var s launchSpec
	if err := readJSON(specPath, &s); err != nil {
		return err
	}
	if s.UID == 0 || s.GID == 0 {
		return fmt.Errorf("refusing privileged VMM identity")
	}
	// /proc still refers to the host mount here, giving the host PID even though
	// getpid() is 1 in our new PID namespace.
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return err
	}
	pid, err := strconv.Atoi(strings.Fields(string(stat))[0])
	if err != nil {
		return err
	}
	start, _, err := procStart(pid)
	if err != nil {
		return err
	}
	if err = writeJSON(
		filepath.Join(s.Resource.RunDir, "process.json"),
		processReceipt{
			Instance:  s.Resource.Instance,
			PID:       pid,
			StartTime: start,
			BootID:    bootID(),
		},
	); err != nil {
		return err
	}
	ns, _, _, _, _, _ := netNames(s.Resource)
	netfd, err := unix.Open("/run/netns/"+ns, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	if err = unix.Setns(netfd, unix.CLONE_NEWNET); err != nil {
		_ = unix.Close(netfd)
		return err
	}
	if err = unix.Close(netfd); err != nil {
		return err
	}
	if err = unix.Mount(
		"",
		"/",
		"",
		unix.MS_REC|unix.MS_PRIVATE,
		"",
	); err != nil {
		return err
	}
	// tmpfs prevents access through the mountpoint's old directory entries.
	if err = unix.Mount(
		"tmpfs",
		s.Root,
		"tmpfs",
		unix.MS_NOSUID|unix.MS_NODEV,
		"mode=0755,size=16m",
	); err != nil {
		return err
	}
	for _, d := range []string{"boot", "images", "sandbox/ro", "disks", "run", "capture", "restore", "dev", "dev/net", "proc", "tmp", "bin"} {
		if err = os.MkdirAll(filepath.Join(s.Root, d), 0o755); err != nil {
			return err
		}
	}
	if err = os.Chmod(filepath.Join(s.Root, "tmp"), 0o1777); err != nil {
		return err
	}
	bind := func(src, rel string, ro bool) error {
		dst := filepath.Join(s.Root, rel)
		st, err := os.Stat(src)
		if err != nil {
			return err
		}
		if st.IsDir() {
			if err = os.MkdirAll(dst, 0o755); err != nil {
				return err
			}
		} else {
			f, e := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, 0o600)
			if e != nil {
				return e
			}
			if e = f.Close(); e != nil {
				return e
			}
		}
		if err = unix.Mount(src, dst, "", unix.MS_BIND, ""); err != nil {
			return err
		}
		flags := uintptr(unix.MS_BIND | unix.MS_REMOUNT | unix.MS_NOSUID)
		if ro {
			flags |= unix.MS_RDONLY
		}
		return unix.Mount("", dst, "", flags, "")
	}
	for _, item := range []struct {
		src, dst string
		ro       bool
	}{
		{s.Binary, "bin/cloud-hypervisor", true},
		{filepath.Join(s.Resource.ImageDir, "kernel"), "boot/kernel", true},
		{filepath.Join(s.Resource.ImageDir, "initramfs"), "boot/initramfs", true},
		{s.Resource.Disk, "disks/upper.raw", false},
		{filepath.Join(s.Resource.RunDir, "io"), "run", false},
		{filepath.Join(s.Resource.Dir, "capture"), "capture", false},
		{"/dev/kvm", "dev/kvm", false},
		{"/dev/net/tun", "dev/net/tun", false},
		{"/dev/null", "dev/null", false},
		{"/dev/zero", "dev/zero", false},
		{"/dev/urandom", "dev/urandom", false},
	} {
		if err = bind(item.src, item.dst, item.ro); err != nil {
			return fmt.Errorf("bind %s: %w", item.dst, err)
		}
	}
	_, bindings, err := runtimeImage(s.Resource)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		if err = bind(
			binding.HostPath,
			strings.TrimPrefix(binding.GuestPath, "/"),
			true,
		); err != nil {
			return fmt.Errorf("bind image device %s: %w", binding.Serial, err)
		}
	}
	if s.Resource.RestoreDir != "" {
		for _, name := range []string{"config.json", "state.json", "memory-ranges"} {
			if err = bind(
				filepath.Join(s.Resource.RestoreDir, name),
				"restore/"+name,
				true,
			); err != nil {
				return err
			}
		}
	}
	if err = unix.Mount(
		"proc",
		filepath.Join(s.Root, "proc"),
		"proc",
		unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC,
		"",
	); err != nil {
		return err
	}
	// pivot_root removes the old host root from the process's namespace entirely.
	old := filepath.Join(s.Root, ".oldroot")
	if err = os.Mkdir(old, 0o700); err != nil {
		return err
	}
	if err = unix.Chdir(s.Root); err != nil {
		return err
	}
	if err = unix.PivotRoot(".", ".oldroot"); err != nil {
		return err
	}
	if err = unix.Chdir("/"); err != nil {
		return err
	}
	if err = unix.Unmount("/.oldroot", unix.MNT_DETACH); err != nil {
		return err
	}
	if err = os.Remove("/.oldroot"); err != nil {
		return err
	}
	if err = unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	for cap := 0; cap <= 40; cap++ {
		if err = unix.Prctl(
			unix.PR_CAPBSET_DROP,
			uintptr(cap),
			0,
			0,
			0,
		); err != nil {
			return err
		}
	}
	if err = unix.Setgroups([]int{}); err != nil {
		return err
	}
	if err = unix.Setresgid(int(s.GID), int(s.GID), int(s.GID)); err != nil {
		return err
	}
	if err = unix.Setresuid(int(s.UID), int(s.UID), int(s.UID)); err != nil {
		return err
	}
	// Go runtime descriptors are CLOEXEC. Explicitly close any accidental inherited
	// descriptor as well, preserving only stdin/out/err for serial diagnostics.
	if err = unix.CloseRange(
		3,
		^uint(0),
		unix.CLOSE_RANGE_CLOEXEC,
	); err != nil {
		return err
	}
	return unix.Exec(
		"/bin/cloud-hypervisor",
		[]string{
			"cloud-hypervisor",
			"--api-socket",
			"/run/api.sock",
			"--seccomp",
			"true",
		},
		[]string{"PATH=/bin", "RUST_BACKTRACE=1"},
	)
}

func openTapInNamespace(r Resource) (*os.File, error) {
	// Namespace changes are per-thread. Never return this thread to Go's pool
	// unless the original namespace was successfully restored.
	runtime.LockOSThread()
	original, err := unix.Open(
		"/proc/self/ns/net",
		unix.O_RDONLY|unix.O_CLOEXEC,
		0,
	)
	if err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	ns, _, _, _, _, _ := netNames(r)
	target, err := unix.Open("/run/netns/"+ns, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(original)
		runtime.UnlockOSThread()
		return nil, err
	}
	err = unix.Setns(target, unix.CLONE_NEWNET)
	_ = unix.Close(target)
	if err != nil {
		_ = unix.Close(original)
		runtime.UnlockOSThread()
		return nil, err
	}
	fd, openErr := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if openErr == nil {
		var req *unix.Ifreq
		req, openErr = unix.NewIfreq("tap0")
		if openErr == nil {
			req.SetUint16(unix.IFF_TAP | unix.IFF_NO_PI | unix.IFF_VNET_HDR)
			openErr = unix.IoctlIfreq(fd, unix.TUNSETIFF, req)
		}
	}
	restoreErr := unix.Setns(original, unix.CLONE_NEWNET)
	_ = unix.Close(original)
	if restoreErr != nil {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
		panic(
			fmt.Sprintf(
				"cannot restore engine network namespace: %v",
				restoreErr,
			),
		)
	}
	runtime.UnlockOSThread()
	if openErr != nil {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
		return nil, openErr
	}
	return os.NewFile(uintptr(fd), "tap0"), nil
}

func (b *CloudHypervisor) Agent(
	ctx context.Context,
	r Resource,
	restore bool,
) error {
	return systemAgent(
		ctx,
		filepath.Join(r.RunDir, "io", "vsock"),
		r.Instance,
		restore,
	)
}
