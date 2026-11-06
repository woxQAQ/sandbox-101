package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	engine "github.com/woxQAQ/sandbox-engine"
	"github.com/woxQAQ/sandbox-engine/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	// The launcher becomes CH in this very process, after kernel-enforced scope
	// placement. This flag is an implementation detail, never a bypass of checks.
	if len(os.Args) == 3 && os.Args[1] == "--internal-launch" {
		return engine.InternalLaunch(os.Args[2])
	}
	if len(os.Args) == 2 && os.Args[1] == "--internal-device-check" {
		return engine.InternalDeviceCheck()
	}
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	output := func(v any) error { enc := json.NewEncoder(os.Stdout); enc.SetIndent("", "  "); return enc.Encode(v) }
	switch cfg.Mode {
	case "doctor":
		d, err := engine.Doctor(ctx, cfg.StateDir)
		if err != nil {
			return err
		}
		return output(d)
	case "restore-backup":
		if cfg.Backup == "" {
			return fmt.Errorf("--backup required")
		}
		return engine.RestoreBackup(cfg.StateDir, cfg.Backup)
	case "recover":
		return engine.AcknowledgeRecovery(cfg.StateDir, cfg.RecoveryNote)
	case "serve", "import", "gc", "backup":
	default:
		return fmt.Errorf("unsupported --mode %s", cfg.Mode)
	}
	if cfg.VMMUID == 0 || cfg.VMMGID == 0 || uint64(cfg.VMMUID) > 1<<32-1 || uint64(cfg.VMMGID) > 1<<32-1 {
		return fmt.Errorf("VMM UID/GID must be nonzero 32-bit IDs")
	}
	backend := engine.NewCloudHypervisor(engine.CloudOptions{Binary: cfg.CloudHypervisor, BuildReceipt: cfg.BuildReceipt, CgroupRoot: cfg.CgroupRoot, HostPolicy: cfg.HostPolicy, UID: uint32(cfg.VMMUID), GID: uint32(cfg.VMMGID), IOMax: cfg.IOMax})
	defer backend.Close()
	e, err := engine.Open(engine.Options{StateDir: cfg.StateDir, Init: cfg.Init, Backend: backend, SafetyBytes: cfg.StorageMarginBytes, MaxMemoryMiB: cfg.MaxMemoryMiB, MaxVCPUs: cfg.MaxVCPUs, GCInterval: cfg.GCInterval})
	if err != nil {
		return err
	}
	defer e.Close()
	switch cfg.Mode {
	case "gc":
		r, collectErr := e.CollectGarbage(ctx, engine.GCOptions{DryRun: cfg.GCDryRun, MaxArtifacts: cfg.GCBatch})
		if err = output(r); err != nil {
			return err
		}
		return collectErr
	case "import":
		if cfg.OCILayout == "" || cfg.Digest == "" {
			return fmt.Errorf("--oci-layout and --digest required")
		}
		m, err := e.ImportOCI(ctx, engine.ImportOptions{Layout: cfg.OCILayout, Digest: cfg.Digest, Materialization: cfg.Materialization})
		if err != nil {
			return err
		}
		return output(m)
	case "backup":
		if cfg.Backup == "" {
			return fmt.Errorf("--backup destination required")
		}
		p, err := e.Backup(ctx, cfg.Backup)
		if err != nil {
			return err
		}
		return output(p)
	}
	// Exclusive state ownership is established before stale socket cleanup. Only
	// socket files in our private socket directory are eligible for removal.
	if err = os.MkdirAll(filepath.Dir(cfg.Socket), 0700); err != nil {
		return err
	}
	if info, x := os.Lstat(cfg.Socket); x == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("socket path is occupied by a non-socket")
		}
		if err = os.Remove(cfg.Socket); err != nil {
			return err
		}
	} else if !os.IsNotExist(x) {
		return x
	}
	return server.Serve(ctx, cfg.Socket, e)
}
