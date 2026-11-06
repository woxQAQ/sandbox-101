//go:build linux && kernel_isolation

package engine

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"golang.org/x/sys/unix"
)

// This exercises production setupScope/arm and the same CLONE_INTO_CGROUP
// mechanism as Launch, with bounded test processes. It does not execute the
// root launcher, pivot_root, privilege dropping, CH, or daemon adoption. The
// memory/PID/CPU/I/O enforcement probes tighten only this test's new owned scope;
// production limits are first read back unchanged. No existing cgroup is edited.
func TestKernelIsolationRootlessCgroup(t *testing.T) {
	if !rootlessRun(t, "cgroup") {
		return
	}
	base := rootlessDelegatedCgroup(t)
	root, err := os.MkdirTemp(base, "sandbox-kernel-acceptance-")
	if err != nil {
		t.Fatalf("new subgroup in delegated cgroup: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(root); err != nil {
			t.Errorf("remove owned cgroup %s: %v", root, err)
		}
	})
	if os.Getenv("SANDBOX_KERNEL_ISOLATION_USER_UNIT") != "" {
		// This service root was created by this acceptance run, rather than
		// inherited from the host/session. All owned harness processes are in
		// its runner subgroup, leaving the new delegation root empty.
		if procs := rootlessRead(t, filepath.Join(base, "cgroup.procs")); procs != "" {
			t.Fatalf("new transient service root is not empty: %s", procs)
		}
		rootlessWrite(t, filepath.Join(base, "cgroup.subtree_control"), "+cpu +memory +pids +io")
	}
	controllers := strings.Fields(rootlessRead(t, filepath.Join(root, "cgroup.controllers")))
	diagnostic := fmt.Sprintf("delegated root: %s\nnew owned subgroup: %s\nparent controllers: %s\navailable controllers: %s\nparent subtree_control: %s\n", base, root, rootlessRead(t, filepath.Join(base, "cgroup.controllers")), strings.Join(controllers, " "), rootlessRead(t, filepath.Join(base, "cgroup.subtree_control")))
	rootlessArtifact(t, "cgroup-delegation.txt", []byte(diagnostic))
	t.Log(diagnostic)
	for _, need := range []string{"cpu", "memory", "pids", "io"} {
		found := false
		for _, available := range controllers {
			found = found || available == need
		}
		if !found {
			t.Fatalf("controller %s is not delegated to new subgroup; existing cgroups remain untouched", need)
		}
	}
	rootlessWrite(t, filepath.Join(root, "cgroup.subtree_control"), "+cpu +memory +pids +io")
	r := Resource{VMID: "bounded", Instance: "kernel-acceptance", Config: api.Config{Shape: api.Shape{VCPUs: 1, MemoryMiB: 64}}}
	b := &CloudHypervisor{opts: CloudOptions{CgroupRoot: root}}
	scope := b.scope(r)
	t.Cleanup(func() {
		if err := os.Remove(scope); err != nil && !os.IsNotExist(err) {
			t.Errorf("remove owned scope %s: %v", scope, err)
		}
	})
	// Select an actual accounted backing device below, before applying io.max.
	if err := b.setupScope(r, true); err != nil {
		t.Fatal(err)
	}
	var applied strings.Builder
	for file, want := range map[string]string{
		"memory.max": "201326592", "memory.swap.max": "0", "pids.max": "256",
		"cpu.max": "max 100000", "cpu.max.burst": "25000", "cpu.weight": "100",
	} {
		got := rootlessRead(t, filepath.Join(scope, file))
		if got != want {
			t.Fatalf("production %s: got %q want %q", file, got, want)
		}
		fmt.Fprintf(&applied, "%s=%s\n", file, got)
	}
	if err := b.arm(r); err != nil {
		t.Fatal(err)
	}
	if got := rootlessRead(t, filepath.Join(scope, "cpu.max")); got != "100000 100000" {
		t.Fatalf("production resume CPU quota: %s", got)
	}
	fmt.Fprintln(&applied, "armed cpu.max=100000 100000")
	rootlessArtifact(t, "cgroup-applied-limits.txt", []byte(applied.String()))

	holder := rootlessCgroupProcess(t, scope, "hold")
	current := rootlessRead(t, filepath.Join(scope, "pids.current"))
	rootlessWrite(t, filepath.Join(scope, "pids.max"), current)
	pidsBefore := rootlessCgroupEvent(t, scope, "pids.events", "max")
	fd, err := os.Open(scope)
	if err != nil {
		t.Fatal(err)
	}
	probe := exec.Command("sh", "-c", "exit 0")
	probe.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(fd.Fd()), Pdeathsig: syscall.SIGKILL}
	err = probe.Run()
	_ = fd.Close()
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("pids.max=%s did not reject one additional owned child with EAGAIN: %v", current, err)
	}
	if rootlessCgroupEvent(t, scope, "pids.events", "max") <= pidsBefore {
		t.Fatal("pids controller rejection was not recorded")
	}
	rootlessWrite(t, filepath.Join(scope, "pids.max"), "256")
	holder.release(t)
	if out, err := holder.wait(); err != nil {
		t.Fatalf("bounded cgroup holder: %v: %s", err, out)
	}
	t.Logf("real CLONE_INTO_CGROUP placement; pids.max=%s rejected one extra process and recorded pids.events max", current)

	cpu := rootlessCgroupProcess(t, scope, "cpu")
	rootlessWrite(t, filepath.Join(scope, "cpu.max.burst"), "0")
	rootlessWrite(t, filepath.Join(scope, "cpu.max"), "5000 100000")
	throttledBefore := rootlessCgroupEvent(t, scope, "cpu.stat", "nr_throttled")
	cpu.release(t)
	if out, err := cpu.wait(); err != nil {
		t.Fatalf("bounded CPU worker: %v: %s", err, out)
	}
	throttledAfter := rootlessCgroupEvent(t, scope, "cpu.stat", "nr_throttled")
	if throttledAfter <= throttledBefore {
		t.Fatal("bounded CPU worker was not throttled by cpu.max")
	}
	t.Logf("real cpu.max=5000/100000 throttled a 25ms CPU-time worker: nr_throttled %d -> %d", throttledBefore, throttledAfter)
	if err := b.arm(r); err != nil {
		t.Fatal(err)
	}

	memory := rootlessCgroupProcess(t, scope, "memory")
	rootlessWrite(t, filepath.Join(scope, "memory.max"), "33554432")
	oomBefore := rootlessCgroupEvent(t, scope, "memory.events", "oom_kill")
	memory.release(t)
	out, err := memory.wait()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("32MiB memory.max did not kill bounded 48MiB page-touch worker: %v: %s", err, out)
	}
	oomAfter := rootlessCgroupEvent(t, scope, "memory.events", "oom_kill")
	if oomAfter <= oomBefore {
		t.Fatal("memory controller OOM kill was not recorded")
	}
	t.Logf("real memory.max=32MiB stopped at most 48MiB of anonymous page touches: oom_kill %d -> %d", oomBefore, oomAfter)
	rootlessArtifact(t, "cgroup-enforcement.txt", []byte(fmt.Sprintf("pids.max=%s; extra child=EAGAIN\ncpu.max=5000 100000; nr_throttled=%d->%d\nmemory.max=33554432; bounded requested allocation=50331648; child=SIGKILL; oom_kill=%d->%d\n", current, throttledBefore, throttledAfter, oomBefore, oomAfter)))
	rootlessCgroupIO(t, b, r)
	deadline := time.Now().Add(2 * time.Second)
	for rootlessCgroupEvent(t, scope, "cgroup.events", "populated") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("owned scope remains populated after all child waits")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if procs := rootlessRead(t, filepath.Join(scope, "cgroup.procs")); procs != "" {
		t.Fatalf("owned processes remain before cleanup: %s", procs)
	}
}

const rootlessIOBytes = 4 << 20

func rootlessCgroupIO(t *testing.T, b *CloudHypervisor, r Resource) {
	t.Helper()
	scope := b.scope(r)
	if err := b.setupScope(r, false); err != nil {
		t.Fatal(err)
	}
	// Artifact storage must have an actual block backing. A tmpfs path cannot
	// establish I/O throttling and will fail the accounting check below.
	dir, err := os.MkdirTemp(os.Getenv("SANDBOX_KERNEL_ISOLATION_ARTIFACTS"), "io-load-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove bounded I/O fixture: %v", err)
		}
	})
	run := func(name string) (time.Duration, map[string]uint64) {
		worker := rootlessCgroupProcess(t, scope, "io", "SANDBOX_KERNEL_IO_FILE="+filepath.Join(dir, name))
		before := rootlessIOStats(t, scope)
		start := time.Now()
		worker.release(t)
		if out, err := worker.wait(); err != nil {
			t.Fatalf("bounded direct-I/O worker: %v: %s", err, out)
		}
		elapsed := time.Since(start)
		after := rootlessIOStats(t, scope)
		delta := make(map[string]uint64)
		for device, bytes := range after {
			if bytes >= before[device] {
				delta[device] = bytes - before[device]
			}
		}
		return elapsed, delta
	}
	baseline, baseBytes := run("baseline")
	var devices []string
	for device, bytes := range baseBytes {
		if bytes >= rootlessIOBytes {
			devices = append(devices, device)
		}
	}
	sort.Strings(devices)
	if len(devices) == 0 {
		t.Fatalf("O_DIRECT write has no actual backing-device accounting for %d bytes: %v", rootlessIOBytes, baseBytes)
	}
	device := devices[0]
	b.opts.IOMax = device + " wbps=1048576"
	if err := b.setupScope(r, false); err != nil {
		t.Fatal(err)
	}
	readback := rootlessRead(t, filepath.Join(scope, "io.max"))
	matched := false
	for _, line := range strings.Split(readback, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 1 && fields[0] == device {
			for _, field := range fields[1:] {
				matched = matched || field == "wbps=1048576"
			}
		}
	}
	if !matched {
		t.Fatalf("production io.max does not contain configured device budget: %q", readback)
	}
	limited, limitedBytes := run("limited")
	evidence := fmt.Sprintf("payload_bytes=%d; direct_io=true; fdatasync=true\ndevice=%s; io.max=%s\nbaseline_elapsed=%s; baseline_wbytes=%v\nlimited_elapsed=%s; limited_wbytes=%v\n", rootlessIOBytes, device, readback, baseline, baseBytes, limited, limitedBytes)
	rootlessArtifact(t, "cgroup-io-enforcement.txt", []byte(evidence))
	t.Log(evidence)
	if limitedBytes[device] < rootlessIOBytes {
		t.Fatalf("limited writes bypassed the selected accounted device: %v", limitedBytes)
	}
	// Four MiB at one MiB/s leaves room for the kernel's throttling slices,
	// while requiring a substantial slowdown compared with the same load.
	if limited < 2500*time.Millisecond || limited < baseline*2+time.Second {
		t.Fatalf("actual io.max budget did not throttle the bounded direct writes: baseline=%s limited=%s", baseline, limited)
	}
}

func rootlessIOStats(t *testing.T, scope string) map[string]uint64 {
	t.Helper()
	stats := make(map[string]uint64)
	for _, line := range strings.Split(rootlessRead(t, filepath.Join(scope, "io.stat")), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		for _, field := range fields[1:] {
			if value, ok := strings.CutPrefix(field, "wbytes="); ok {
				bytes, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					t.Fatalf("invalid kernel io.stat: %q: %v", line, err)
				}
				stats[fields[0]] = bytes
			}
		}
	}
	return stats
}

func rootlessDelegatedCgroup(t *testing.T) string {
	t.Helper()
	var relative string
	for _, line := range strings.Split(rootlessRead(t, "/proc/self/cgroup"), "\n") {
		if strings.HasPrefix(line, "0::/") {
			relative = strings.TrimPrefix(line, "0::")
		}
	}
	if relative == "" || relative == "/" {
		t.Fatal("refusing allocation at global cgroup root")
	}
	base := filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(relative, "/"))
	// The runner may request one new transient user service with an owned runner
	// subgroup. Its empty delegated service root is then the allocation parent.
	if unit := os.Getenv("SANDBOX_KERNEL_ISOLATION_USER_UNIT"); unit != "" {
		if !strings.HasPrefix(unit, "sandbox-kernel-acceptance-") || !strings.HasSuffix(unit, ".service") || strings.ContainsAny(unit, "/\\") || filepath.Base(base) != "runner" || filepath.Base(filepath.Dir(base)) != unit {
			t.Fatalf("refusing cgroup outside the runner's named transient user unit: %q at %s", unit, base)
		}
		base = filepath.Dir(base)
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(base, &fs); err != nil || fs.Type != unix.CGROUP2_SUPER_MAGIC {
		t.Fatalf("real cgroup v2 filesystem required: %v", err)
	}
	return base
}

func rootlessCgroupEvent(t *testing.T, scope, file, name string) uint64 {
	t.Helper()
	for _, line := range strings.Split(rootlessRead(t, filepath.Join(scope, file)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("kernel %s lacks %s", file, name)
	return 0
}

type rootlessOwnedProcess struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	output *bufio.Reader
	cancel context.CancelFunc
}

func rootlessCgroupProcess(t *testing.T, scope, mode string, env ...string) *rootlessOwnedProcess {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fd, err := os.Open(scope)
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 11*time.Second)
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestKernelIsolationRootlessCgroupWorker$", "-test.timeout=10s")
	cmd.Env = append(os.Environ(), rootlessChildEnv+"=cgroup-worker", "SANDBOX_KERNEL_CGROUP_WORK="+mode, "GOMAXPROCS=1")
	cmd.Env = append(cmd.Env, env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(fd.Fd()), Pdeathsig: syscall.SIGKILL}
	input, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("real CLONE_INTO_CGROUP: %v", err)
	}
	owned := &rootlessOwnedProcess{cmd: cmd, input: input, output: bufio.NewReader(output), cancel: cancel}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_, _ = owned.wait()
		}
		cancel()
	})
	ready, err := owned.output.ReadString('\n')
	if err != nil || !strings.HasPrefix(ready, "ready 0::") || !strings.HasSuffix(strings.TrimSpace(ready), "/"+filepath.Base(scope)) {
		t.Fatalf("child did not begin in owned cgroup: %q: %v", ready, err)
	}
	procs := strings.Fields(rootlessRead(t, filepath.Join(scope, "cgroup.procs")))
	if len(procs) != 1 || procs[0] != strconv.Itoa(cmd.Process.Pid) {
		t.Fatalf("unexpected child membership in owned cgroup: %v; want PID %d", procs, cmd.Process.Pid)
	}
	return owned
}

func (p *rootlessOwnedProcess) release(t *testing.T) {
	t.Helper()
	if _, err := io.WriteString(p.input, "run\n"); err != nil {
		t.Fatal(err)
	}
	_ = p.input.Close()
}

func (p *rootlessOwnedProcess) wait() ([]byte, error) {
	out, readErr := io.ReadAll(p.output)
	err := p.cmd.Wait()
	p.cancel()
	return out, errors.Join(err, readErr)
}

func TestKernelIsolationRootlessCgroupWorker(t *testing.T) {
	if os.Getenv(rootlessChildEnv) != "cgroup-worker" {
		t.Skip("bounded cgroup subprocess only")
	}
	rootlessRequireNamespaces(t)
	fmt.Println("ready", rootlessRead(t, "/proc/self/cgroup"))
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	switch mode := os.Getenv("SANDBOX_KERNEL_CGROUP_WORK"); mode {
	case "hold":
		return
	case "cpu":
		var start, now unix.Timespec
		if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &start); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Second)
		var sum uint64
		for {
			for i := uint64(0); i < 4096; i++ {
				sum += i * (sum | 1)
			}
			if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &now); err != nil {
				t.Fatal(err)
			}
			if now.Nano()-start.Nano() >= int64(25*time.Millisecond) {
				fmt.Println("bounded cpu work", sum)
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("bounded CPU worker did not finish within 2s")
			}
		}
	case "io":
		path := os.Getenv("SANDBOX_KERNEL_IO_FILE")
		if !filepath.IsAbs(path) {
			t.Fatal("bounded I/O fixture requires an absolute private file path")
		}
		fd, err := unix.Open(path, unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_DIRECT|unix.O_WRONLY, 0600)
		if err != nil {
			t.Fatalf("open actual O_DIRECT fixture: %v", err)
		}
		defer unix.Close(fd)
		pages, err := unix.Mmap(-1, 0, 1<<20, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Munmap(pages)
		for i := range pages {
			pages[i] = byte(i%251 + 1)
		}
		for written := 0; written < rootlessIOBytes; written += len(pages) {
			n, err := unix.Write(fd, pages)
			if err != nil || n != len(pages) {
				t.Fatalf("bounded direct write: bytes=%d error=%v", n, err)
			}
		}
		if err := unix.Fdatasync(fd); err != nil {
			t.Fatalf("sync actual block writes: %v", err)
		}
		fmt.Println("bounded direct I/O bytes", rootlessIOBytes)
	case "memory":
		pages, err := unix.Mmap(-1, 0, 48<<20, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Munmap(pages)
		for i := 0; i < len(pages); i += os.Getpagesize() {
			pages[i] = 1
		}
		t.Fatal("bounded allocation exceeded memory.max without an OOM kill")
	default:
		t.Fatalf("unknown cgroup work: %q", mode)
	}
}
