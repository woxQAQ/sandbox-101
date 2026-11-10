package config

import "time"

// Config describes engine host settings and administrative actions. Its flat
// mapstructure keys are shared by configuration files, ENGINE_* environment
// variables and engined flags. It is distinct from api.Config, which describes
// one VM. Loading defaults and validating runtime prerequisites are separate
// from this data model; engined registers defaults in its flag set.
type Config struct {
	// Config selects an explicit configuration file; empty disables file loading.
	Config string `mapstructure:"config"`
	// StateDir holds the private durable store, images, disks and snapshots.
	StateDir string `mapstructure:"state-dir"`
	// Socket is the permission-protected Unix HTTP socket used in serve mode.
	Socket string `mapstructure:"socket"`
	// Init explicitly authorizes initialization of an empty state directory.
	Init bool `mapstructure:"init"`
	// Mode selects serve, import, gc, backup, restore-backup, recover or doctor.
	Mode string `mapstructure:"mode"`
	// GCDryRun reports eligible artifacts without deleting them in gc mode.
	GCDryRun bool `mapstructure:"gc-dry-run"`
	// GCBatch bounds artifacts per administrative GC pass to 1..1024.
	GCBatch int `mapstructure:"gc-batch"`
	// GCInterval controls background GC; zero selects the runtime default.
	GCInterval time.Duration `mapstructure:"gc-interval"`
	// Backup is the metadata backup input/output path, with a .json receipt.
	Backup string `mapstructure:"backup"`
	// RecoveryNote records operator review when acknowledging explicit recovery.
	RecoveryNote string `mapstructure:"recovery-note"`
	// OCILayout is the local OCI image layout consumed in import mode.
	OCILayout string `mapstructure:"oci-layout"`
	// Digest pins the exact sha256 manifest to import from OCILayout.
	Digest string `mapstructure:"digest"`
	// Materialization selects the image layout; empty selects layered-erofs-v1.
	Materialization string `mapstructure:"materialization"`
	// CloudHypervisor names the static, pinned VMM executable.
	CloudHypervisor string `mapstructure:"cloud-hypervisor"`
	// BuildReceipt identifies its trusted build; empty uses the binary's .json.
	BuildReceipt string `mapstructure:"build-receipt"`
	// CgroupRoot is the delegated cgroup v2 subtree for VMM resource limits.
	CgroupRoot string `mapstructure:"cgroup-root"`
	// HostPolicy is the deployment receipt for host quota and networking checks.
	HostPolicy string `mapstructure:"host-policy"`
	// IOMax is the required cgroup v2 device I/O budget, in io.max syntax.
	IOMax string `mapstructure:"io-max"`
	// VMMUID is the nonzero 32-bit unprivileged UID used by VMM processes.
	VMMUID uint `mapstructure:"vmm-uid"`
	// VMMGID is the nonzero 32-bit GID with the required device access.
	VMMGID uint `mapstructure:"vmm-gid"`
	// MaxMemoryMiB caps durable memory reservations; zero selects 80% of host RAM.
	MaxMemoryMiB int64 `mapstructure:"max-memory-mib"`
	// MaxVCPUs caps vCPU reservations; zero selects the host logical CPU count.
	MaxVCPUs int `mapstructure:"max-vcpus"`
	// StorageMarginBytes reserves free filesystem space beyond resource budgets.
	StorageMarginBytes int64 `mapstructure:"storage-margin-bytes"`
}
