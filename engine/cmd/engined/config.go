package main

import (
	"time"

	"github.com/spf13/pflag"

	"github.com/woxQAQ/sandbox-engine/config"
)

type commandConfig struct {
	Config             string        `mapstructure:"config"`
	StateDir           string        `mapstructure:"state-dir"`
	Socket             string        `mapstructure:"socket"`
	Init               bool          `mapstructure:"init"`
	Mode               string        `mapstructure:"mode"`
	GCDryRun           bool          `mapstructure:"gc-dry-run"`
	GCBatch            int           `mapstructure:"gc-batch"`
	GCInterval         time.Duration `mapstructure:"gc-interval"`
	Backup             string        `mapstructure:"backup"`
	RecoveryNote       string        `mapstructure:"recovery-note"`
	OCILayout          string        `mapstructure:"oci-layout"`
	Digest             string        `mapstructure:"digest"`
	Materialization    string        `mapstructure:"materialization"`
	CloudHypervisor    string        `mapstructure:"cloud-hypervisor"`
	BuildReceipt       string        `mapstructure:"build-receipt"`
	CgroupRoot         string        `mapstructure:"cgroup-root"`
	HostPolicy         string        `mapstructure:"host-policy"`
	IOMax              string        `mapstructure:"io-max"`
	VMMUID             uint          `mapstructure:"vmm-uid"`
	VMMGID             uint          `mapstructure:"vmm-gid"`
	MaxMemoryMiB       int64         `mapstructure:"max-memory-mib"`
	MaxVCPUs           int           `mapstructure:"max-vcpus"`
	StorageMarginBytes int64         `mapstructure:"storage-margin-bytes"`
}

func parseConfig(args []string) (commandConfig, error) {
	f := pflag.NewFlagSet("engined", pflag.ContinueOnError)
	f.SetInterspersed(false)
	f.String(
		"config",
		"",
		"configuration file (YAML, JSON, TOML or another Viper-supported format)",
	)
	f.String("state-dir", "/var/lib/sandbox-engine", "durable state directory")
	f.String("socket", "/run/sandbox-engine/engine.sock", "UDS HTTP socket")
	f.Bool("init", false, "explicitly initialize an empty state directory")
	f.String(
		"mode",
		"serve",
		"serve, import, gc, backup, restore-backup, recover, or doctor",
	)
	f.Bool("gc-dry-run", false, "report eligible GC artifacts without deleting")
	f.Int("gc-batch", 16, "maximum artifacts per GC pass (1..1024)")
	f.Duration("gc-interval", 0, "background GC interval (default 1m)")
	f.String(
		"backup",
		"",
		"backup DB input/output path (receipt uses .json suffix)",
	)
	f.String("recovery-note", "", "operator review note for explicit recovery")
	f.String("oci-layout", "", "local OCI image layout to import")
	f.String("digest", "", "sha256 manifest digest to import")
	f.String(
		"materialization",
		"",
		"image layout: layered-erofs-v1 (default) or merged-oci-erofs-v1",
	)
	f.String(
		"cloud-hypervisor",
		"/usr/local/libexec/sandbox/cloud-hypervisor",
		"static pinned CH binary",
	)
	f.String(
		"build-receipt",
		"",
		"trusted pinned build receipt; defaults to binary.json",
	)
	f.String(
		"cgroup-root",
		"/sys/fs/cgroup/sandbox-engine",
		"delegated cgroup v2 root",
	)
	f.String(
		"host-policy",
		"/etc/sandbox-engine/host-policy.json",
		"host quota/network deployment receipt",
	)
	f.String(
		"io-max",
		"",
		"explicit cgroup v2 io.max budget, e.g. 259:0 rbps=... wbps=...",
	)
	f.Uint("vmm-uid", 65534, "unprivileged VMM UID")
	f.Uint("vmm-gid", 65534, "unprivileged VMM GID")
	f.Int64(
		"max-memory-mib",
		0,
		"physical memory reservation ceiling (default 80% RAM)",
	)
	f.Int(
		"max-vcpus",
		0,
		"vCPU reservation ceiling (default host logical CPU count)",
	)
	f.Int64("storage-margin-bytes", 256<<20, "free filesystem safety margin")
	var c commandConfig
	if err := f.Parse(args); err != nil {
		return c, err
	}
	err := config.Load(f, "ENGINE", &c)
	return c, err
}
