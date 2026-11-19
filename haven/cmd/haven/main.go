package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/pflag"
	"golang.org/x/sys/unix"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/api/management"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/common/logging"
	"github.com/woxQAQ/haven/config"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/localuds"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/types"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func output(value any) error {
	return json.NewEncoder(os.Stdout).Encode(value)
}

func run(args []string) (result error) {
	if len(args) == 2 && args[0] == "--internal-launch" {
		return host.InternalLaunch(args[1])
	}
	if len(args) == 1 && args[0] == "--internal-device-check" {
		return host.InternalDeviceCheck()
	}
	cfg, err := parseConfig(args)
	if errors.Is(err, pflag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	logger, err := logging.New(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	gin.SetMode(gin.ReleaseMode)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch cfg.Mode {
	case "backup":
		if cfg.Backup == "" {
			return fmt.Errorf("--backup destination required")
		}
		point, err := application.BackupState(ctx, cfg.StateDir, cfg.Backup)
		if err != nil {
			return err
		}
		return output(point)
	case "restore-backup":
		if cfg.Backup == "" {
			return fmt.Errorf("--backup source required")
		}
		point, err := application.RestoreBackup(ctx, cfg.StateDir, cfg.Backup)
		if err != nil {
			return err
		}
		return output(point)
	case "recover":
		if err := application.AcknowledgeRecovery(ctx, cfg.StateDir, cfg.RecoveryNote); err != nil {
			return err
		}
		return output(map[string]bool{
			"reviewed": true,
		})
	case "doctor":
		status, err := application.InspectState(ctx, cfg.StateDir)
		if err != nil {
			return err
		}
		return output(status)
	}
	if cfg.AdminTokenFile == "" {
		return fmt.Errorf("--root-token-file required")
	}
	rawToken, err := os.ReadFile(cfg.AdminTokenFile)
	if err != nil {
		return fmt.Errorf("read administrator token: %w", err)
	}
	token := strings.TrimSpace(string(rawToken))
	if len(token) < 32 {
		return fmt.Errorf("administrator token must contain at least 32 characters")
	}
	if cfg.MaxMemoryMiB == 0 {
		var memory unix.Sysinfo_t
		if err := unix.Sysinfo(&memory); err != nil {
			return err
		}
		unit := uint64(memory.Unit)
		if unit == 0 {
			unit = 1
		}
		cfg.MaxMemoryMiB = int64((memory.Totalram * unit / 5 * 4) >> 20)
	}
	if cfg.MaxVCPUs == 0 {
		cfg.MaxVCPUs = runtime.NumCPU()
	}
	backend := host.NewCloudHypervisor(host.CloudHypervisorOptions{
		Binary:       cfg.CloudHypervisor,
		BuildReceipt: cfg.BuildReceipt,
		CgroupRoot:   cfg.CgroupRoot,
		HostPolicy:   cfg.HostPolicy,
		UID:          uint32(cfg.VMMUID),
		GID:          uint32(cfg.VMMGID),
		IOMax:        cfg.IOMax,
	})
	defer func() {
		_ = backend.Close()
	}()
	service, err := application.Open(application.Options{
		Context:      ctx,
		StateDir:     cfg.StateDir,
		Initialize:   cfg.Init,
		Backend:      backend,
		Guest:        guest.Client{},
		AdminToken:   token,
		MaxMemoryMiB: cfg.MaxMemoryMiB,
		MaxVCPUs:     cfg.MaxVCPUs,
		SafetyBytes:  cfg.StorageMarginBytes,
		MaxTTL:       cfg.MaxTTL,
		PausedTTL:    cfg.PausedTTL,
		MaxWarm:      cfg.MaxWarm,
		Interval:     cfg.MaintenanceInterval,
		EgressFactory: func(host application.EgressCallbacks) (application.Egress, string, error) {
			manager, err := egress.New(egress.Options{
				PrepareSocket: func(binding egress.Binding, path string, info os.FileInfo) error {
					prepareCtx, stop := context.WithTimeout(ctx, 3*time.Second)
					defer stop()
					return host.PrepareProxySocket(prepareCtx, binding, path, info)
				},
				InstallDirect:      host.InstallDirect,
				RecoverStaleSocket: host.RecoverStaleSocket,
				AuditRecord: func(event egress.AuditEvent) error {
					auditCtx, stop := context.WithTimeout(ctx, 3*time.Second)
					defer stop()
					return host.Audit(auditCtx, event)
				},
			})
			if err != nil {
				return nil, "", err
			}
			return manager, string(manager.CACertPEM()), nil
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		result = errors.Join(result, service.Close(closeCtx))
	}()
	return serve(ctx, cfg, service)
}

func serve(ctx context.Context, cfg config.Daemon, service *application.App) error {
	local, err := localuds.Open(cfg.Socket)
	if err != nil {
		return err
	}
	defer func() {
		_ = local.Close()
	}()
	business, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer func() {
		_ = business.Close()
	}()
	httpServer := &http.Server{
		Handler: api.New(api.Services{
			Authentication: service.Authentication,
			Catalog:        service.Catalog,
			Templates:      service.TemplateCatalog,
			Operations:     service.Operations,
			Tenants:        service.Tenants,
			Requests:       service.SandboxRequests,
			Dataplane:      service.Dataplane,
			Management:     service.Management,
			Delivery:       service.Delivery,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    32 << 10,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	localServer := &http.Server{
		Handler:           management.New(service.Management),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    32 << 10,
		ConnContext:       localuds.PeerContext,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	done := make(chan error, 2)
	go func() {
		if cfg.TLSCert != "" {
			done <- httpServer.ServeTLS(business, cfg.TLSCert, cfg.TLSKey)
		} else {
			done <- httpServer.Serve(business)
		}
	}()
	go func() {
		done <- localServer.Serve(local)
	}()
	workCtx, stopWork := context.WithCancel(ctx)
	workDone := make(chan struct{})
	go func() {
		defer close(workDone)
		maintain(workCtx, cfg, service)
	}()
	slog.Info("Haven listening", "http", business.Addr().String(), "management", cfg.Socket, "state", cfg.StateDir)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-done:
	}
	service.Availability.Drain()
	stopWork()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	httpErr := httpServer.Shutdown(shutdownCtx)
	localErr := localServer.Shutdown(shutdownCtx)
	if httpErr != nil {
		_ = httpServer.Close()
	}
	if localErr != nil {
		_ = localServer.Close()
	}
	select {
	case <-workDone:
	case <-shutdownCtx.Done():
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, httpErr, localErr)
}

func maintain(ctx context.Context, cfg config.Daemon, service *application.App) {
	ticker := time.NewTicker(cfg.MaintenanceInterval)
	defer ticker.Stop()
	var collect <-chan time.Time
	if cfg.GCInterval > 0 {
		gcTicker := time.NewTicker(cfg.GCInterval)
		defer gcTicker.Stop()
		collect = gcTicker.C
	}
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := service.Reconcile.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("Haven reconciliation deferred", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case now := <-collect:
			_, err := service.GC.Collect(ctx, authorization.Principal{
				Root:     true,
				TenantID: "node-admin",
			}, "gc:"+now.UTC().Format(time.RFC3339Nano), types.GCOptions{
				Batch: cfg.GCBatch,
			})
			if err != nil && ctx.Err() == nil {
				slog.Warn("Haven collection deferred", "error", err)
			}
		}
	}
}
