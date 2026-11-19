// Package config defines Haven daemon settings. Command-line parsing belongs to cmd/haven.
package config

import "time"

type Daemon struct {
	Config              string        `mapstructure:"config"`
	StateDir            string        `mapstructure:"state-dir"`
	Socket              string        `mapstructure:"socket"`
	Listen              string        `mapstructure:"listen"`
	Init                bool          `mapstructure:"init"`
	Mode                string        `mapstructure:"mode"`
	AdminTokenFile       string        `mapstructure:"root-token-file"`
	TLSCert             string        `mapstructure:"tls-cert"`
	TLSKey              string        `mapstructure:"tls-key"`
	Backup              string        `mapstructure:"backup"`
	RecoveryNote        string        `mapstructure:"recovery-note"`
	LogLevel            string        `mapstructure:"log-level"`
	LogFormat           string        `mapstructure:"log-format"`
	CloudHypervisor     string        `mapstructure:"cloud-hypervisor"`
	BuildReceipt        string        `mapstructure:"build-receipt"`
	CgroupRoot          string        `mapstructure:"cgroup-root"`
	HostPolicy          string        `mapstructure:"host-policy"`
	IOMax               string        `mapstructure:"io-max"`
	VMMUID              uint          `mapstructure:"vmm-uid"`
	VMMGID              uint          `mapstructure:"vmm-gid"`
	MaxMemoryMiB        int64         `mapstructure:"max-memory-mib"`
	MaxVCPUs            int           `mapstructure:"max-vcpus"`
	StorageMarginBytes  int64         `mapstructure:"storage-margin-bytes"`
	MaxTTL              time.Duration `mapstructure:"max-ttl"`
	PausedTTL           time.Duration `mapstructure:"paused-ttl"`
	MaxWarm             int           `mapstructure:"max-warm"`
	MaintenanceInterval time.Duration `mapstructure:"maintenance-interval"`
	GCInterval          time.Duration `mapstructure:"gc-interval"`
	GCBatch             int           `mapstructure:"gc-batch"`
}
