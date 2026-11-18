package main

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"

	loader "github.com/woxQAQ/haven/common/config"
	"github.com/woxQAQ/haven/config"
)

func newFlagSet() *pflag.FlagSet {
	flags := pflag.NewFlagSet("haven", pflag.ContinueOnError)
	flags.SetInterspersed(false)
	flags.String("config", "", "explicit configuration file")
	flags.String("state-dir", "/var/lib/haven", "single durable state directory")
	flags.String("socket", "/run/haven/management.sock", "local administration socket")
	flags.String("listen", "127.0.0.1:7777", "business HTTP address")
	flags.Bool("init", false, "explicitly initialize an empty state directory")
	flags.String("mode", "serve", "serve, backup, restore-backup, recover, or doctor")
	flags.String("root-token-file", "", "private HTTP administrator token file")
	flags.String("tls-cert", "", "HTTP TLS certificate")
	flags.String("tls-key", "", "HTTP TLS private key")
	flags.String("backup", "", "offline backup input or output directory")
	flags.String("recovery-note", "", "explicit metadata recovery review note")
	flags.String("log-level", "info", "debug, info, warn, or error")
	flags.String("log-format", "text", "text or json logs on stderr")
	flags.String("cloud-hypervisor", "/usr/local/libexec/sandbox/cloud-hypervisor", "pinned Cloud Hypervisor binary")
	flags.String("build-receipt", "", "trusted CH build receipt (defaults to binary.json)")
	flags.String("cgroup-root", "/sys/fs/cgroup/haven", "delegated cgroup v2 root")
	flags.String("host-policy", "/etc/haven/host-policy.json", "host resource policy receipt")
	flags.String("io-max", "", "explicit cgroup v2 io.max budget")
	flags.Uint("vmm-uid", 65534, "unprivileged VMM UID")
	flags.Uint("vmm-gid", 65534, "unprivileged VMM GID")
	flags.Int64("max-memory-mib", 0, "host memory reservation ceiling (0 selects 80 percent of RAM)")
	flags.Int("max-vcpus", 0, "host CPU reservation ceiling (0 selects logical CPU count)")
	flags.Int64("storage-margin-bytes", 256<<20, "free filesystem safety margin")
	flags.Duration("max-ttl", time.Hour, "maximum running sandbox lease")
	flags.Duration("paused-ttl", 24*time.Hour, "maximum paused sandbox lease")
	flags.Int("max-warm", 32, "per-template warm pool ceiling")
	flags.Duration("maintenance-interval", time.Second, "workflow, binding, lease and pool maintenance interval")
	flags.Duration("gc-interval", time.Minute, "background collection interval (0 disables)")
	flags.Int("gc-batch", 16, "maximum artifacts per collection pass")
	return flags
}

func parseConfig(args []string) (config.Daemon, error) {
	flags := newFlagSet()
	var result config.Daemon
	if err := flags.Parse(args); err != nil {
		return result, err
	}
	if flags.NArg() != 0 {
		return result, fmt.Errorf("unexpected positional arguments")
	}
	if err := loader.Load(flags, "HAVEN", &result); err != nil {
		return result, err
	}
	if result.Mode != "serve" && result.Mode != "backup" && result.Mode != "restore-backup" && result.Mode != "recover" && result.Mode != "doctor" {
		return result, fmt.Errorf("unsupported --mode %s", result.Mode)
	}
	if result.MaxMemoryMiB < 0 || result.MaxVCPUs < 0 || result.StorageMarginBytes < 0 || result.MaxTTL < time.Second || result.PausedTTL < time.Second || result.MaxWarm < 1 || result.MaintenanceInterval <= 0 || result.GCInterval < 0 || result.GCBatch < 1 || result.GCBatch > 1024 {
		return result, fmt.Errorf("invalid resource or maintenance bounds")
	}
	if result.VMMUID == 0 || result.VMMGID == 0 || uint64(result.VMMUID) > 1<<32-1 || uint64(result.VMMGID) > 1<<32-1 {
		return result, fmt.Errorf("VMM UID/GID must be nonzero 32-bit IDs")
	}
	if (result.TLSCert == "") != (result.TLSKey == "") {
		return result, fmt.Errorf("TLS certificate and key must be provided together")
	}
	return result, nil
}
