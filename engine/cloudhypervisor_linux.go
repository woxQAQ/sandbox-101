package engine

import (
	"context"
	"debug/elf"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/sandbox-engine/api"
)

// This exact upstream tree includes CopyOnWrite restore and reflink/pagemap
// snapshot reuse. Upgrades require a new build receipt and compatibility ID.
const (
	cloudHypervisorCommit = "a02b114bd42c65b73033231a27bd23f3c487fc4c"
	cloudBuildFeatures    = "kvm,io_uring,seccomp"
)

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
	b := &CloudHypervisor{
		opts:    opts,
		epoll:   -1,
		watched: map[int]int{},
		events:  make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	return b
}

func (b *CloudHypervisor) RequiresImageCatalog() bool { return true }

func (b *CloudHypervisor) Compatibility() string {
	var r BuildReceipt
	if readJSON(b.opts.BuildReceipt, &r) != nil {
		return "cloud-hypervisor/" + cloudHypervisorCommit + "/unverified"
	}
	return "cloud-hypervisor/" + cloudHypervisorCommit + "/" + r.SHA256 + "/" + cloudBuildFeatures + "/layout-v2"
}

func (b *CloudHypervisor) checkHostPolicy(ctx context.Context) error {
	var p HostPolicy
	if err := readJSON(b.opts.HostPolicy, &p); err != nil {
		return api.Fail(
			api.Incompatible,
			"host policy receipt required: %v",
			err,
		)
	}
	if p.Version != 1 || !p.StorageQuota || !p.NetworkQuotas {
		return api.Fail(
			api.Incompatible,
			"storage and network quota deployment acknowledgement missing",
		)
	}
	for _, set := range []string{"active_ifaces", "active_ips"} {
		if _, err := command(
			ctx,
			"nft",
			"list",
			"set",
			"inet",
			"sandbox_engine",
			set,
		); err != nil {
			return api.Fail(
				api.Incompatible,
				"host admission set missing: %v",
				err,
			)
		}
	}
	forwarding, err := os.ReadFile("/proc/sys/net/ipv4/ip_forward")
	if err != nil || strings.TrimSpace(string(forwarding)) != "1" {
		return api.Fail(
			api.Incompatible,
			"host IPv4 forwarding must be enabled",
		)
	}
	return nil
}

func (b *CloudHypervisor) Check(ctx context.Context, c api.Config) error {
	if err := checkConfig(&c); err != nil {
		return err
	}
	if os.Geteuid() != 0 {
		return api.Fail(
			api.Incompatible,
			"real backend requires root for namespace/cgroup setup",
		)
	}
	for _, tool := range []string{"ip", "nft", "mkfs.ext4", "sysctl"} {
		if _, err := exec.LookPath(tool); err != nil {
			return api.Fail(
				api.Incompatible,
				"required host tool missing: %s",
				tool,
			)
		}
	}
	var receipt BuildReceipt
	if err := readJSON(b.opts.BuildReceipt, &receipt); err != nil {
		return api.Fail(api.Incompatible, "pinned CH build receipt: %v", err)
	}
	if receipt.Commit != cloudHypervisorCommit ||
		receipt.Features != cloudBuildFeatures {
		return api.Fail(
			api.Incompatible,
			"CH revision or build configuration is not pinned baseline",
		)
	}
	hash, err := digestFile(b.opts.Binary)
	if err != nil || hash != receipt.SHA256 {
		return api.Fail(
			api.Incompatible,
			"CH binary digest does not match trusted build receipt",
		)
	}
	f, err := elf.Open(b.opts.Binary)
	if err != nil {
		return err
	}
	libs, err := f.ImportedLibraries()
	err = errors.Join(err, f.Close())
	if err != nil || len(libs) != 0 {
		return api.Fail(
			api.Incompatible,
			"CH must be statically linked for the restricted root",
		)
	}
	for _, dev := range []string{"/dev/kvm", "/dev/net/tun"} {
		fd, x := unix.Open(dev, unix.O_RDWR|unix.O_CLOEXEC, 0)
		if x != nil {
			return api.Fail(api.Incompatible, "required device %s: %v", dev, x)
		}
		if dev == "/dev/kvm" {
			version, x := unix.IoctlRetInt(
				fd,
				0xae00, /* KVM_GET_API_VERSION, linux/kvm.h */
			)
			if x != nil || version != 12 {
				_ = unix.Close(fd)
				return api.Fail(api.Incompatible, "KVM API unavailable")
			}
		}
		if err := unix.Close(fd); err != nil {
			return err
		}
	}
	if err := b.checkDeviceIdentity(ctx); err != nil {
		return err
	}
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return api.Fail(api.Incompatible, "pidfd unavailable: %v", err)
	}
	if err = unix.Close(fd); err != nil {
		return err
	}
	controllers, err := os.ReadFile(
		filepath.Join(b.opts.CgroupRoot, "cgroup.subtree_control"),
	)
	if err != nil {
		return api.Fail(
			api.Incompatible,
			"delegated cgroup v2 root required: %v",
			err,
		)
	}
	for _, need := range []string{"cpu", "memory", "io", "pids"} {
		if !strings.Contains(string(controllers), need) {
			return api.Fail(
				api.Incompatible,
				"cgroup controller %s not enabled",
				need,
			)
		}
	}
	if b.opts.IOMax == "" || strings.ContainsAny(b.opts.IOMax, "\n\r") {
		return api.Fail(
			api.Incompatible,
			"an explicit cgroup io.max device budget is required",
		)
	}
	return b.checkHostPolicy(ctx)
}
