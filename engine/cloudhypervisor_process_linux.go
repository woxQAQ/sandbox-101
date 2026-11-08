package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/sandbox-engine/api"
)

type processReceipt struct {
	Instance  string `json:"instance"`
	PID       int    `json:"pid"`
	StartTime uint64 `json:"start_time"`
	BootID    string `json:"boot_id"`
}

func (b *CloudHypervisor) Events() <-chan struct{} { return b.events }

func (b *CloudHypervisor) Close() error {
	var err error
	b.once.Do(func() {
		close(b.done)
		b.mu.Lock()
		defer b.mu.Unlock()
		for fd := range b.watched {
			err = errors.Join(err, unix.Close(fd))
		}
		if b.epoll >= 0 {
			err = errors.Join(err, unix.Close(b.epoll))
			b.epoll = -1
		}
	})
	return err
}

func (b *CloudHypervisor) watch(pid int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.watched {
		if p == pid {
			return nil
		}
	}
	if b.epoll < 0 {
		fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
		if err != nil {
			return err
		}
		b.epoll = fd
		go b.watchLoop(fd)
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return err
	}
	if err = unix.EpollCtl(
		b.epoll,
		unix.EPOLL_CTL_ADD,
		fd,
		&unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(fd)},
	); err != nil {
		_ = unix.Close(fd)
		return err
	}
	b.watched[fd] = pid
	return nil
}

func (b *CloudHypervisor) watchLoop(ep int) {
	events := make([]unix.EpollEvent, 32)
	for {
		select {
		case <-b.done:
			return
		default:
		}
		n, err := unix.EpollWait(ep, events, 1000)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return
		}
		for _, event := range events[:n] {
			b.mu.Lock()
			fd := int(event.Fd)
			if _, ok := b.watched[fd]; ok {
				_ = unix.EpollCtl(ep, unix.EPOLL_CTL_DEL, fd, nil)
				_ = unix.Close(fd)
				delete(b.watched, fd)
			}
			b.mu.Unlock()
			select {
			case b.events <- struct{}{}:
			default:
			}
		}
	}
}

func procStart(pid int) (uint64, byte, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0, 0, fmt.Errorf("invalid proc stat")
	}
	f := strings.Fields(string(data[end+1:]))
	if len(f) < 20 {
		return 0, 0, fmt.Errorf("short proc stat")
	}
	start, err := strconv.ParseUint(f[19], 10, 64)
	return start, f[0][0], err
}

func bootID() string {
	b, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return strings.TrimSpace(string(b))
}

func (b *CloudHypervisor) scope(r Resource) string {
	return filepath.Join(b.opts.CgroupRoot, r.VMID+"-"+r.Instance)
}

func (b *CloudHypervisor) process(r Resource) (processReceipt, bool, error) {
	var p processReceipt
	if r.Instance == "" {
		return p, false, nil
	}
	err := readJSON(filepath.Join(r.RunDir, "process.json"), &p)
	if errors.Is(err, os.ErrNotExist) {
		procs, x := os.ReadFile(filepath.Join(b.scope(r), "cgroup.procs"))
		if x == nil && len(strings.TrimSpace(string(procs))) > 0 {
			return p, false, api.Fail(
				api.ManualRecovery,
				"process exists in scope without identity receipt",
			)
		}
		if x != nil && !os.IsNotExist(x) {
			return p, false, x
		}
		return p, false, nil
	}
	if err != nil {
		return p, false, err
	}
	if p.Instance != r.Instance {
		return p, false, api.Fail(
			api.ManualRecovery,
			"process receipt belongs to another instance",
		)
	}
	if p.BootID != bootID() {
		return p, false, nil
	}
	start, state, err := procStart(p.PID)
	if os.IsNotExist(err) {
		return p, false, nil
	}
	if err != nil {
		return p, false, err
	}
	if start != p.StartTime {
		return p, false, api.Fail(
			api.ManualRecovery,
			"PID reused: refusing to adopt or signal",
		)
	}
	if state == 'Z' || state == 'X' {
		return p, false, nil
	}
	src, err := os.Stat(b.opts.Binary)
	if err != nil {
		return p, false, err
	}
	actual, err := os.Stat(fmt.Sprintf("/proc/%d/exe", p.PID))
	if err != nil {
		return p, false, err
	}
	if !os.SameFile(src, actual) {
		return p, false, api.Fail(
			api.ManualRecovery,
			"VMM executable identity mismatch or launcher did not finish",
		)
	}
	cgroup, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", p.PID))
	if err != nil {
		return p, false, err
	}
	if !strings.Contains(string(cgroup), "/"+filepath.Base(b.scope(r))+"\n") {
		return p, false, api.Fail(
			api.ManualRecovery,
			"VMM cgroup identity mismatch",
		)
	}
	if err = b.watch(p.PID); err != nil {
		return p, false, err
	}
	return p, true, nil
}

func (b *CloudHypervisor) Inspect(
	ctx context.Context,
	r Resource,
) (Observation, error) {
	entries, x := os.ReadDir(b.opts.CgroupRoot)
	if x != nil && !os.IsNotExist(x) {
		return Observation{}, x
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), r.VMID+"-") &&
			entry.Name() != filepath.Base(b.scope(r)) {
			procs, x := os.ReadFile(
				filepath.Join(b.opts.CgroupRoot, entry.Name(), "cgroup.procs"),
			)
			if x != nil {
				return Observation{}, x
			}
			if strings.TrimSpace(string(procs)) != "" {
				return Observation{}, api.Fail(
					api.ManualRecovery,
					"unrecorded live instance scope %s",
					entry.Name(),
				)
			}
		}
	}
	netUp, err := b.networkInspect(ctx, r)
	if err != nil {
		return Observation{}, err
	}
	p, alive, err := b.process(r)
	if err != nil {
		return Observation{}, err
	}
	obs := Observation{Alive: alive, Network: netUp}
	if !alive {
		return obs, nil
	}
	obs.PID = p.PID
	obs.StartTime = p.StartTime
	data, err := b.api(ctx, r, "GET", "vm.info", nil, nil)
	if err != nil {
		var status *chHTTPError
		if errors.As(err, &status) && status.Status == 404 {
			return obs, nil
		}
		return obs, api.Fail(
			api.ManualRecovery,
			"live VMM API unreachable: %v",
			err,
		)
	}
	obs.Configured = true
	var info struct {
		State string `json:"state"`
	}
	if err = json.Unmarshal(data, &info); err != nil {
		return obs, err
	}
	obs.Paused = info.State == "Paused"
	obs.Booted = info.State == "Paused" || info.State == "Running"
	return obs, nil
}

func (b *CloudHypervisor) setupScope(r Resource, loading bool) error {
	path := b.scope(r)
	if err := os.Mkdir(path, 0o700); err != nil && !os.IsExist(err) {
		return err
	}
	cpu := fmt.Sprintf("%d 100000", r.Config.Shape.VCPUs*100000)
	if loading {
		cpu = "max 100000"
	}
	values := map[string]string{
		"memory.max": strconv.FormatInt(
			(r.Config.Shape.MemoryMiB+128)<<20,
			10,
		),
		"memory.swap.max": "0",
		"pids.max":        "256",
		"cpu.max":         cpu,
		"cpu.max.burst":   "25000",
		"cpu.weight":      "100",
		"io.max":          b.opts.IOMax,
	}
	for name, value := range values {
		if err := os.WriteFile(
			filepath.Join(path, name),
			[]byte(value),
			0o600,
		); err != nil {
			return api.Fail(api.Incompatible, "cgroup %s: %v", name, err)
		}
	}
	return nil
}

func (b *CloudHypervisor) arm(r Resource) error {
	return os.WriteFile(
		filepath.Join(b.scope(r), "cpu.max"),
		[]byte(fmt.Sprintf("%d 100000", r.Config.Shape.VCPUs*100000)),
		0o600,
	)
}

func (b *CloudHypervisor) Launch(
	ctx context.Context,
	r Resource,
	restore bool,
) error {
	if _, err := os.Lstat(filepath.Join(r.RunDir, "process.json")); err == nil {
		return api.Fail(
			api.Unknown,
			"this instance previously launched and exited; submit a new intent after inspection",
		)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := b.Check(ctx, r.Config); err != nil {
		return err
	}
	if err := b.setupScope(r, restore); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(r.RunDir, "io"), 0o700); err != nil {
		return err
	}
	if err := os.Chown(
		filepath.Join(r.RunDir, "io"),
		int(b.opts.UID),
		int(b.opts.GID),
	); err != nil {
		return err
	}
	for _, path := range []string{r.Disk} {
		if err := os.Chown(path, int(b.opts.UID), int(b.opts.GID)); err != nil {
			return err
		}
	}
	root := filepath.Join(r.RunDir, "root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	spec := launchSpec{
		Resource: r,
		Binary:   b.opts.Binary,
		Root:     root,
		UID:      b.opts.UID,
		GID:      b.opts.GID,
	}
	specPath := filepath.Join(r.RunDir, "launch.json")
	if err := writeJSON(specPath, spec); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	scope, err := os.Open(b.scope(r))
	if err != nil {
		return err
	}
	defer func() { _ = scope.Close() }()
	log, err := os.OpenFile(
		filepath.Join(r.RunDir, "console.log"),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		return err
	}
	defer func() { _ = log.Close() }()
	cmd := exec.Command(exe, "--internal-launch", specPath)
	cmd.Env = []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "GOMAXPROCS=1"}
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid:      true,
		Cloneflags:  unix.CLONE_NEWNS | unix.CLONE_NEWPID | unix.CLONE_NEWIPC | unix.CLONE_NEWUTS,
		UseCgroupFD: true,
		CgroupFD:    int(scope.Fd()),
		Pdeathsig:   0,
	}
	if err = cmd.Start(); err != nil {
		return api.Fail(
			api.Incompatible,
			"isolated CLONE_INTO_CGROUP launcher failed: %v",
			err,
		)
	}
	// Wait only reaps this process. It neither forwards signals nor supervises it.
	go func() { _ = cmd.Wait() }()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return api.Fail(
				api.ManualRecovery,
				"launcher did not publish a usable API; preserve scope and console.log",
			)
		case <-tick.C:
			if _, alive, x := b.process(r); x == nil && alive {
				if _, x = b.api(ctx, r, "GET", "vmm.ping", nil, nil); x == nil {
					return nil
				}
			}
		}
	}
}

func (b *CloudHypervisor) Stop(ctx context.Context, r Resource) error {
	p, alive, err := b.process(r)
	if err != nil {
		return err
	}
	if !alive {
		return nil
	}
	fd, err := unix.PidfdOpen(p.PID, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	start, _, err := procStart(p.PID)
	if err != nil {
		return err
	}
	if start != p.StartTime {
		return api.Fail(api.ManualRecovery, "process changed before signal")
	}
	// SIGKILL via pidfd has no PID-reuse race and also works for namespace PID 1.
	if err = unix.PidfdSendSignal(
		fd,
		unix.SIGKILL,
		nil,
		0,
	); err != nil &&
		!errors.Is(err, unix.ESRCH) {
		return err
	}
	poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, x := unix.Poll(poll, 1000)
		if x == unix.EINTR {
			continue
		}
		if x != nil {
			return x
		}
		if n > 0 {
			return nil
		}
	}
}

func (b *CloudHypervisor) Remove(ctx context.Context, r Resource) error {
	_, alive, err := b.process(r)
	if err != nil {
		return err
	}
	if alive {
		return api.Fail(api.Conflict, "refusing cleanup of live VMM")
	}
	if err = b.networkRemove(ctx, r); err != nil {
		return err
	}
	// All historical instance scopes must be empty before release of ownership.
	entries, err := os.ReadDir(b.opts.CgroupRoot)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), r.VMID+"-") {
			p := filepath.Join(b.opts.CgroupRoot, entry.Name())
			procs, x := os.ReadFile(filepath.Join(p, "cgroup.procs"))
			if x != nil {
				return x
			}
			if len(strings.TrimSpace(string(procs))) > 0 {
				return api.Fail(
					api.ManualRecovery,
					"historical VMM scope still populated",
				)
			}
			if x = os.Remove(p); x != nil {
				return x
			}
		}
	}
	return nil
}
