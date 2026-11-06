package engine

import (
	"bufio"
	"context"
	"debug/elf"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"golang.org/x/sys/unix"
)

// This exact upstream tree includes CopyOnWrite restore and reflink/pagemap
// snapshot reuse. Upgrades require a new build receipt and compatibility ID.
const CloudHypervisorCommit = "a02b114bd42c65b73033231a27bd23f3c487fc4c"
const CloudBuildFeatures = "kvm,io_uring,seccomp"

type chHTTPError struct {
	Status         int
	Endpoint, Body string
}

func (e *chHTTPError) Error() string {
	return fmt.Sprintf("CH %s HTTP %d: %.4096s", e.Endpoint, e.Status, e.Body)
}

type CloudOptions struct {
	Binary       string
	BuildReceipt string
	CgroupRoot   string
	HostPolicy   string
	UID          uint32
	GID          uint32
	IOMax        string
}
type BuildReceipt struct {
	Commit   string `json:"commit"`
	SHA256   string `json:"sha256"`
	Features string `json:"features"`
}
type HostPolicy struct {
	Version       int  `json:"version"`
	StorageQuota  bool `json:"storage_quota"`
	NetworkQuotas bool `json:"network_quotas"`
}
type processReceipt struct {
	Instance  string `json:"instance"`
	PID       int    `json:"pid"`
	StartTime uint64 `json:"start_time"`
	BootID    string `json:"boot_id"`
}
type CloudHypervisor struct {
	opts    CloudOptions
	mu      sync.Mutex
	epoll   int
	watched map[int]int
	events  chan struct{}
	done    chan struct{}
	once    sync.Once
}

var _ Backend = (*CloudHypervisor)(nil)

func NewCloudHypervisor(opts CloudOptions) *CloudHypervisor {
	if opts.Binary == "" {
		opts.Binary = "/usr/local/libexec/sandbox/cloud-hypervisor"
	}
	if opts.BuildReceipt == "" {
		opts.BuildReceipt = opts.Binary + ".json"
	}
	if opts.CgroupRoot == "" {
		opts.CgroupRoot = "/sys/fs/cgroup/sandbox-engine"
	}
	if opts.UID == 0 {
		opts.UID = 65534
	}
	if opts.GID == 0 {
		opts.GID = 65534
	}
	b := &CloudHypervisor{opts: opts, epoll: -1, watched: map[int]int{}, events: make(chan struct{}, 1), done: make(chan struct{})}
	return b
}
func (b *CloudHypervisor) RequiresImageCatalog() bool { return true }
func (b *CloudHypervisor) Compatibility() string {
	var r BuildReceipt
	if readJSON(b.opts.BuildReceipt, &r) != nil {
		return "cloud-hypervisor/" + CloudHypervisorCommit + "/unverified"
	}
	return "cloud-hypervisor/" + CloudHypervisorCommit + "/" + r.SHA256 + "/" + CloudBuildFeatures + "/layout-v2"
}
func (b *CloudHypervisor) Events() <-chan struct{} { return b.events }
func (b *CloudHypervisor) Close() error {
	b.once.Do(func() {
		close(b.done)
		b.mu.Lock()
		defer b.mu.Unlock()
		for fd := range b.watched {
			unix.Close(fd)
		}
		if b.epoll >= 0 {
			unix.Close(b.epoll)
			b.epoll = -1
		}
	})
	return nil
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
	if err = unix.EpollCtl(b.epoll, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(fd)}); err != nil {
		unix.Close(fd)
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
				unix.Close(fd)
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
func (b *CloudHypervisor) checkHostPolicy(ctx context.Context) error {
	var p HostPolicy
	if err := readJSON(b.opts.HostPolicy, &p); err != nil {
		return api.Fail(api.Incompatible, "host policy receipt required: %v", err)
	}
	if p.Version != 1 || !p.StorageQuota || !p.NetworkQuotas {
		return api.Fail(api.Incompatible, "storage and network quota deployment acknowledgement missing")
	}
	for _, set := range []string{"active_ifaces", "active_ips"} {
		if _, err := command(ctx, "nft", "list", "set", "inet", "sandbox_engine", set); err != nil {
			return api.Fail(api.Incompatible, "host admission set missing: %v", err)
		}
	}
	forwarding, err := os.ReadFile("/proc/sys/net/ipv4/ip_forward")
	if err != nil || strings.TrimSpace(string(forwarding)) != "1" {
		return api.Fail(api.Incompatible, "host IPv4 forwarding must be enabled")
	}
	return nil
}
func (b *CloudHypervisor) Check(ctx context.Context, c api.Config) error {
	if err := checkConfig(&c); err != nil {
		return err
	}
	if os.Geteuid() != 0 {
		return api.Fail(api.Incompatible, "real backend requires root for namespace/cgroup setup")
	}
	for _, tool := range []string{"ip", "nft", "mkfs.ext4", "sysctl"} {
		if _, err := exec.LookPath(tool); err != nil {
			return api.Fail(api.Incompatible, "required host tool missing: %s", tool)
		}
	}
	var receipt BuildReceipt
	if err := readJSON(b.opts.BuildReceipt, &receipt); err != nil {
		return api.Fail(api.Incompatible, "pinned CH build receipt: %v", err)
	}
	if receipt.Commit != CloudHypervisorCommit || receipt.Features != CloudBuildFeatures {
		return api.Fail(api.Incompatible, "CH revision or build configuration is not pinned baseline")
	}
	hash, err := digestFile(b.opts.Binary)
	if err != nil || hash != receipt.SHA256 {
		return api.Fail(api.Incompatible, "CH binary digest does not match trusted build receipt")
	}
	f, err := elf.Open(b.opts.Binary)
	if err != nil {
		return err
	}
	libs, err := f.ImportedLibraries()
	f.Close()
	if err != nil || len(libs) != 0 {
		return api.Fail(api.Incompatible, "CH must be statically linked for the restricted root")
	}
	for _, dev := range []string{"/dev/kvm", "/dev/net/tun"} {
		fd, x := unix.Open(dev, unix.O_RDWR|unix.O_CLOEXEC, 0)
		if x != nil {
			return api.Fail(api.Incompatible, "required device %s: %v", dev, x)
		}
		if dev == "/dev/kvm" {
			version, x := unix.IoctlRetInt(fd, 0xae00 /* KVM_GET_API_VERSION, linux/kvm.h */)
			if x != nil || version != 12 {
				unix.Close(fd)
				return api.Fail(api.Incompatible, "KVM API unavailable")
			}
		}
		unix.Close(fd)
	}
	if err := b.checkDeviceIdentity(ctx); err != nil {
		return err
	}
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return api.Fail(api.Incompatible, "pidfd unavailable: %v", err)
	}
	unix.Close(fd)
	controllers, err := os.ReadFile(filepath.Join(b.opts.CgroupRoot, "cgroup.subtree_control"))
	if err != nil {
		return api.Fail(api.Incompatible, "delegated cgroup v2 root required: %v", err)
	}
	for _, need := range []string{"cpu", "memory", "io", "pids"} {
		if !strings.Contains(string(controllers), need) {
			return api.Fail(api.Incompatible, "cgroup controller %s not enabled", need)
		}
	}
	if b.opts.IOMax == "" || strings.ContainsAny(b.opts.IOMax, "\n\r") {
		return api.Fail(api.Incompatible, "an explicit cgroup io.max device budget is required")
	}
	return b.checkHostPolicy(ctx)
}
func (b *CloudHypervisor) Prepare(ctx context.Context, r Resource) error {
	if err := b.Check(ctx, r.Config); err != nil {
		return err
	}
	var image ImageManifest
	if err := readJSON(filepath.Join(r.ImageDir, "image.json"), &image); err != nil {
		return api.Fail(api.Incompatible, "image not imported: %v", err)
	}
	if err := image.Verify(r.ImageDir, r.Config.Image); err != nil {
		return err
	}
	for _, dir := range []string{r.Dir, filepath.Dir(r.Disk), filepath.Join(r.Dir, "capture")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}
	if _, err := os.Stat(r.Disk); os.IsNotExist(err) {
		tmp := r.Disk + ".pending"
		_ = os.Remove(tmp)
		f, x := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if x != nil {
			return x
		}
		x = f.Truncate(r.Config.Shape.DiskMiB << 20)
		f.Close()
		if x != nil {
			return x
		}
		if _, x = command(ctx, "mkfs.ext4", "-q", "-F", "-L", "sandbox-upper", tmp); x != nil {
			return x
		}
		if x = syncTree(filepath.Dir(r.Disk)); x != nil {
			return x
		}
		if x = os.Rename(tmp, r.Disk); x != nil {
			return x
		}
		if x = syncDir(filepath.Dir(r.Disk)); x != nil {
			return x
		}
	}
	return b.networkPrepare(ctx, r)
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
			return p, false, api.Fail(api.ManualRecovery, "process exists in scope without identity receipt")
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
		return p, false, api.Fail(api.ManualRecovery, "process receipt belongs to another instance")
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
		return p, false, api.Fail(api.ManualRecovery, "PID reused: refusing to adopt or signal")
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
		return p, false, api.Fail(api.ManualRecovery, "VMM executable identity mismatch or launcher did not finish")
	}
	cgroup, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", p.PID))
	if err != nil {
		return p, false, err
	}
	if !strings.Contains(string(cgroup), "/"+filepath.Base(b.scope(r))+"\n") {
		return p, false, api.Fail(api.ManualRecovery, "VMM cgroup identity mismatch")
	}
	if err = b.watch(p.PID); err != nil {
		return p, false, err
	}
	return p, true, nil
}
func (b *CloudHypervisor) Inspect(ctx context.Context, r Resource) (Observation, error) {
	entries, x := os.ReadDir(b.opts.CgroupRoot)
	if x != nil && !os.IsNotExist(x) {
		return Observation{}, x
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), r.VMID+"-") && entry.Name() != filepath.Base(b.scope(r)) {
			procs, x := os.ReadFile(filepath.Join(b.opts.CgroupRoot, entry.Name(), "cgroup.procs"))
			if x != nil {
				return Observation{}, x
			}
			if strings.TrimSpace(string(procs)) != "" {
				return Observation{}, api.Fail(api.ManualRecovery, "unrecorded live instance scope %s", entry.Name())
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
		return obs, api.Fail(api.ManualRecovery, "live VMM API unreachable: %v", err)
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
	if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	cpu := fmt.Sprintf("%d 100000", r.Config.Shape.VCPUs*100000)
	if loading {
		cpu = "max 100000"
	}
	values := map[string]string{"memory.max": strconv.FormatInt((r.Config.Shape.MemoryMiB+128)<<20, 10), "memory.swap.max": "0", "pids.max": "256", "cpu.max": cpu, "cpu.max.burst": "25000", "cpu.weight": "100", "io.max": b.opts.IOMax}
	for name, value := range values {
		if err := os.WriteFile(filepath.Join(path, name), []byte(value), 0600); err != nil {
			return api.Fail(api.Incompatible, "cgroup %s: %v", name, err)
		}
	}
	return nil
}
func (b *CloudHypervisor) arm(r Resource) error {
	return os.WriteFile(filepath.Join(b.scope(r), "cpu.max"), []byte(fmt.Sprintf("%d 100000", r.Config.Shape.VCPUs*100000)), 0600)
}
func (b *CloudHypervisor) Launch(ctx context.Context, r Resource, restore bool) error {
	if _, err := os.Lstat(filepath.Join(r.RunDir, "process.json")); err == nil {
		return api.Fail(api.Unknown, "this instance previously launched and exited; submit a new intent after inspection")
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := b.Check(ctx, r.Config); err != nil {
		return err
	}
	if err := b.setupScope(r, restore); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(r.RunDir, "io"), 0700); err != nil {
		return err
	}
	if err := os.Chown(filepath.Join(r.RunDir, "io"), int(b.opts.UID), int(b.opts.GID)); err != nil {
		return err
	}
	for _, path := range []string{r.Disk, filepath.Join(r.Dir, "capture")} {
		if err := os.Chown(path, int(b.opts.UID), int(b.opts.GID)); err != nil {
			return err
		}
	}
	root := filepath.Join(r.RunDir, "root")
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	spec := launchSpec{Resource: r, Binary: b.opts.Binary, Root: root, UID: b.opts.UID, GID: b.opts.GID}
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
	defer scope.Close()
	log, err := os.OpenFile(filepath.Join(r.RunDir, "console.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	cmd := exec.Command(exe, "--internal-launch", specPath)
	cmd.Env = []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "GOMAXPROCS=1"}
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Cloneflags: unix.CLONE_NEWNS | unix.CLONE_NEWPID | unix.CLONE_NEWIPC | unix.CLONE_NEWUTS, UseCgroupFD: true, CgroupFD: int(scope.Fd()), Pdeathsig: 0}
	if err = cmd.Start(); err != nil {
		return api.Fail(api.Incompatible, "isolated CLONE_INTO_CGROUP launcher failed: %v", err)
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
			return api.Fail(api.ManualRecovery, "launcher did not publish a usable API; preserve scope and console.log")
		case <-tick.C:
			if _, alive, x := b.process(r); x == nil && alive {
				if _, x = b.api(ctx, r, "GET", "vmm.ping", nil, nil); x == nil {
					return nil
				}
			}
		}
	}
}
func (b *CloudHypervisor) Boot(ctx context.Context, r Resource, restore bool) error {
	obs, err := b.Inspect(ctx, r)
	if err != nil {
		return err
	}
	if obs.Booted {
		if restore {
			if err = b.verifyCOWMappings(r); err != nil {
				return err
			}
		}
		if obs.Paused {
			if err = b.arm(r); err != nil {
				return err
			}
			return b.Resume(ctx, r)
		}
		return nil
	}
	tap, err := openTapInNamespace(r)
	if err != nil {
		return err
	}
	defer tap.Close()
	if restore {
		if obs.Configured {
			return api.Fail(api.ManualRecovery, "partially loaded restore state requires inspection before retry")
		}
		if err = b.validateCOW(r); err != nil {
			return err
		}
		body := map[string]any{"source_url": "file:///restore", "prefault": false, "memory_restore_mode": "CopyOnWrite", "resume": false, "net_fds": []any{map[string]any{"id": "net0", "num_fds": 1, "fds": []int{int(tap.Fd())}}}}
		if _, err = b.api(ctx, r, "PUT", "vm.restore", body, []int{int(tap.Fd())}); err != nil {
			return err
		}
		if err = b.verifyCOWMappings(r); err != nil {
			return err
		}
		if err = b.arm(r); err != nil {
			return err
		}
		return b.Resume(ctx, r)
	}
	image, bindings, err := runtimeImage(r)
	if err != nil {
		return err
	}
	dataParameter, err := image.DataKernelParameter()
	if err != nil {
		return err
	}
	cmdline := "console=hvc0 panic=-1 reboot=k ip=192.168.127.2::192.168.127.1:255.255.255.252:sandbox:eth0:off sandbox.lower_serial=sandbox-root sandbox.upper_serial=sandbox-write sandbox.dns=1.1.1.1"
	if dataParameter != "" {
		cmdline += " " + dataParameter
	}
	body := map[string]any{
		"cpus":    map[string]any{"boot_vcpus": r.Config.Shape.VCPUs, "max_vcpus": r.Config.Shape.VCPUs},
		"memory":  map[string]any{"size": r.Config.Shape.MemoryMiB << 20, "shared": false, "hugepages": false, "mergeable": false},
		"payload": map[string]any{"kernel": "/boot/kernel", "initramfs": "/boot/initramfs", "cmdline": cmdline},
		"disks":   cloudDisks(bindings),
		"vsock":   map[string]any{"id": "vsock0", "cid": 3, "socket": "/run/vsock"}, "rng": map[string]any{"src": "/dev/urandom"}, "console": map[string]any{"mode": "Tty"}, "serial": map[string]any{"mode": "Null"}}
	if !obs.Configured {
		if _, err = b.api(ctx, r, "PUT", "vm.create", body, nil); err != nil {
			return err
		}
	}
	if err = b.ensureColdNetwork(ctx, r, tap); err != nil {
		return err
	}
	if err = b.arm(r); err != nil {
		return err
	}
	_, err = b.api(ctx, r, "PUT", "vm.boot", nil, nil)
	return err
}

// vm.create deliberately discards SCM_RIGHTS in the pinned CH API. Cold-add the
// TAP before boot through vm.add-net, which accepts descriptors. Omitting mtu
// avoids SIOCSIFMTU in the unprivileged VMM; the engine configured it beforehand.
func (b *CloudHypervisor) ensureColdNetwork(ctx context.Context, r Resource, tap *os.File) error {
	data, err := b.api(ctx, r, "GET", "vm.info", nil, nil)
	if err != nil {
		return err
	}
	var info struct {
		Config struct {
			Net []struct {
				ID  string `json:"id"`
				Tap string `json:"tap"`
				FDs []int  `json:"fds"`
			} `json:"net"`
		} `json:"config"`
	}
	if err = json.Unmarshal(data, &info); err != nil {
		return err
	}
	if len(info.Config.Net) != 0 {
		// A daemon may have died after CH committed vm.add-net. Preserve the
		// already held queue rather than attaching a duplicate on replay.
		if len(info.Config.Net) != 1 || info.Config.Net[0].ID != "net0" || info.Config.Net[0].Tap != "" || len(info.Config.Net[0].FDs) != 1 || info.Config.Net[0].FDs[0] < 0 {
			return api.Fail(api.ManualRecovery, "configured cold network has no verified TAP descriptor")
		}
		return nil
	}
	network := map[string]any{"id": "net0", "mac": "02:00:00:00:00:02", "num_queues": 2, "queue_size": 256, "offload_tso": false, "offload_ufo": false, "offload_csum": false}
	_, err = b.api(ctx, r, "PUT", "vm.add-net", network, []int{int(tap.Fd())})
	return err
}

func (b *CloudHypervisor) validateCOW(r Resource) error {
	var cfg map[string]json.RawMessage
	if err := readJSON(filepath.Join(r.RestoreDir, "config.json"), &cfg); err != nil {
		return err
	}
	for _, k := range []string{"devices", "user_devices", "vdpa", "balloon", "pvmemcontrol", "memory_zones", "platform", "landlock_rules"} {
		if v := cfg[k]; len(v) > 0 && string(v) != "null" && string(v) != "[]" && string(v) != "false" {
			return api.Fail(api.Incompatible, "unapproved COW device/configuration %s", k)
		}
	}
	var mem struct {
		Size        int64           `json:"size"`
		Shared      bool            `json:"shared"`
		Hugepages   bool            `json:"hugepages"`
		Mergeable   bool            `json:"mergeable"`
		HotplugSize int64           `json:"hotplug_size"`
		File        string          `json:"file"`
		Zones       json.RawMessage `json:"zones"`
	}
	if err := json.Unmarshal(cfg["memory"], &mem); err != nil {
		return err
	}
	if mem.Size != r.Config.Shape.MemoryMiB<<20 || mem.Shared || mem.Hugepages || mem.Mergeable || mem.HotplugSize != 0 || mem.File != "" || (len(mem.Zones) > 0 && string(mem.Zones) != "null" && string(mem.Zones) != "[]") {
		return api.Fail(api.Incompatible, "COW requires static private RAM without zones")
	}
	// Only snapshots of this exact engine layout are accepted. Configuration
	// checks supplement the published manifest's checksums and compatibility ID.
	var disks []cloudDisk
	if err := json.Unmarshal(cfg["disks"], &disks); err != nil {
		return err
	}
	_, bindings, err := runtimeImage(r)
	if err != nil {
		return err
	}
	if err = validateSnapshotDisks(disks, cloudDisks(bindings)); err != nil {
		return err
	}
	st, err := os.Stat(filepath.Join(r.RestoreDir, "memory-ranges"))
	if err != nil {
		return err
	}
	if st.Size() < mem.Size || st.Size()%int64(os.Getpagesize()) != 0 {
		return api.Fail(api.Incompatible, "unaligned or truncated COW memory file")
	}
	return validateCOWTopology(r.RestoreDir, uint64(mem.Size))
}
func (b *CloudHypervisor) Pause(ctx context.Context, r Resource) error {
	_, err := b.api(ctx, r, "PUT", "vm.pause", nil, nil)
	return err
}
func (b *CloudHypervisor) Resume(ctx context.Context, r Resource) error {
	_, err := b.api(ctx, r, "PUT", "vm.resume", nil, nil)
	return err
}
func (b *CloudHypervisor) Capture(ctx context.Context, r Resource, dst string) error {
	if err := os.Chown(filepath.Dir(dst), int(b.opts.UID), int(b.opts.GID)); err != nil {
		return err
	}
	if err := os.Chown(dst, int(b.opts.UID), int(b.opts.GID)); err != nil {
		return err
	}
	rel, err := filepath.Rel(filepath.Join(r.Dir, "capture"), dst)
	if err != nil || strings.HasPrefix(rel, "..") {
		return api.Fail(api.Invalid, "capture outside instance-owned directory")
	}
	_, err = b.api(ctx, r, "PUT", "vm.snapshot", map[string]string{"destination_url": "file:///capture/" + filepath.ToSlash(rel)}, nil)
	return err
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
	defer unix.Close(fd)
	start, _, err := procStart(p.PID)
	if err != nil {
		return err
	}
	if start != p.StartTime {
		return api.Fail(api.ManualRecovery, "process changed before signal")
	}
	// SIGKILL via pidfd has no PID-reuse race and also works for namespace PID 1.
	if err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
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
				return api.Fail(api.ManualRecovery, "historical VMM scope still populated")
			}
			if x = os.Remove(p); x != nil {
				return x
			}
		}
	}
	return nil
}
func (b *CloudHypervisor) api(ctx context.Context, r Resource, method, endpoint string, input any, fds []int) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(r.RunDir, "io", "api.sock"))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	var body []byte
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return nil, err
		}
	}
	header := fmt.Sprintf("%s /api/v1/%s HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n", method, endpoint, len(body))
	if len(fds) > 0 {
		n, _, x := conn.(*net.UnixConn).WriteMsgUnix([]byte(header), unix.UnixRights(fds...), nil)
		if x != nil {
			return nil, x
		}
		if n < len(header) {
			if _, err = io.WriteString(conn, header[n:]); err != nil {
				return nil, err
			}
		}
	} else if _, err = io.WriteString(conn, header); err != nil {
		return nil, err
	}
	if len(body) > 0 {
		if _, err = conn.Write(body); err != nil {
			return nil, err
		}
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, &chHTTPError{Status: resp.StatusCode, Endpoint: endpoint, Body: string(out)}
	}
	return out, nil
}
