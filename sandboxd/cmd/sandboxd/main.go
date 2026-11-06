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
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/woxQAQ/sandbox-engine/client"
	"github.com/woxQAQ/sandboxd"
	"github.com/woxQAQ/sandboxd/agent"
	"github.com/woxQAQ/sandboxd/egress"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Sandbox service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		return err
	}
	switch cfg.Mode {
	case "backup":
		if cfg.Backup == "" {
			return errors.New("--backup new directory is required")
		}
		point, err := sandboxd.BackupState(
			context.Background(),
			cfg.StateDir,
			cfg.Backup,
		)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(point)
	case "restore-backup":
		if cfg.Backup == "" {
			return errors.New("--backup source directory is required")
		}
		point, err := sandboxd.RestoreStateBackup(cfg.StateDir, cfg.Backup)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(point)
	case "recover":
		return sandboxd.AcknowledgeStateRecovery(cfg.StateDir, cfg.RecoveryNote)
	case "serve":
	default:
		return fmt.Errorf("unknown mode %q", cfg.Mode)
	}
	if cfg.RootTokenFile == "" {
		return errors.New("--root-token-file is required")
	}
	raw, err := os.ReadFile(cfg.RootTokenFile)
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(raw))
	if len(token) < 32 {
		return errors.New("root token must contain at least 32 characters")
	}
	if (cfg.TLSCert == "") != (cfg.TLSKey == "") {
		return errors.New("both --tls-cert and --tls-key are required")
	}
	engine := client.New(cfg.EngineSocket)
	defer engine.Close()
	var service *sandboxd.Service
	ready := make(chan struct{})
	proxy, err := egress.New(
		egress.Options{
			PrepareSocket: func(binding egress.Binding, path string, info os.FileInfo) error {
				<-ready
				ctx, cancel := context.WithTimeout(
					context.Background(),
					3*time.Second,
				)
				defer cancel()
				return service.PrepareProxySocket(ctx, binding, path, info)
			},
			InstallDirect: func(ctx context.Context, binding egress.Binding, policy egress.Policy) (func() error, error) {
				select {
				case <-ready:
					return service.InstallDirect(ctx, binding, policy)
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
			AuditRecord: func(event egress.AuditEvent) error {
				<-ready
				ctx, cancel := context.WithTimeout(
					context.Background(),
					3*time.Second,
				)
				defer cancel()
				return service.Audit(ctx, event)
			},
			RecoverStaleSocket: func(binding egress.Binding, path string, info os.FileInfo) bool {
				<-ready
				ctx, cancel := context.WithTimeout(
					context.Background(),
					3*time.Second,
				)
				defer cancel()
				b, err := service.Get(
					ctx,
					sandboxd.Principal{Root: true},
					binding.SandboxID,
				)
				if err != nil || b.VMID == "" ||
					b.TenantID != binding.TenantID {
					return false
				}
				v, err := engine.GetVM(ctx, b.VMID)
				if err != nil || !v.Verified || v.Instance == "" {
					return false
				}
				return path == v.VSockPath+"_2049" ||
					path == v.VSockPath+"_2050"
			},
		},
	)
	if err != nil {
		return err
	}
	defer func() { _ = proxy.Close() }()
	service, err = sandboxd.Open(
		sandboxd.Options{
			StateDir:   cfg.StateDir,
			Initialize: cfg.Init,
			RootToken:  token,
			Engine:     engine,
			Agent:      &agent.Client{},
			Egress:     proxy,
			CAPEM:      string(proxy.CACertPEM()),
		},
	)
	if err != nil {
		return err
	}
	close(ready)
	defer func() { _ = service.Close() }()
	server := &http.Server{
		Addr:              cfg.Listen,
		Handler:           service.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	signals, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		if cfg.TLSCert != "" {
			done <- server.ServeTLS(ln, cfg.TLSCert, cfg.TLSKey)
		} else {
			done <- server.Serve(ln)
		}
	}()
	slog.Info(
		"Sandbox service listening",
		"address",
		ln.Addr().String(),
		"state",
		filepath.Clean(cfg.StateDir),
	)
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-signals.Done():
	}
	service.Drain()
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err = server.Shutdown(ctx); err != nil {
		return fmt.Errorf("HTTP shutdown: %w", err)
	}
	return nil
}
