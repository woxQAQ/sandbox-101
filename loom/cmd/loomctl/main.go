// loomctl is a protocol client. Haven owns its workflows and database.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"

	"github.com/woxQAQ/haven/common/clijson"
	"github.com/woxQAQ/haven/sdk"
	"github.com/woxQAQ/haven/types"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	cfg, args, err := parseConfig(arguments)
	if errors.Is(err, pflag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("usage: loomctl [--socket PATH] [--key KEY] {status|operation ID|import|gc|supply-cleanup|image-cleanup|request}")
	}
	if cfg.RequestFile != "" && args[0] != "request" {
		return errors.New("--request-file is only valid for request")
	}
	client := sdk.NewManagement(cfg.Socket)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var value any
	switch args[0] {
	case "status":
		if len(args) != 1 {
			return errors.New("status takes no arguments")
		}
		value, err = client.Status(ctx)
	case "operation":
		if len(args) != 2 {
			return errors.New("operation requires an id")
		}
		value, err = client.Operation(ctx, args[1])
	case "import", "gc", "supply-cleanup", "image-cleanup", "request":
		if len(args) != 1 {
			return errors.New("management commands take options before the command")
		}
		request := types.ManagementRequest{
			Key: cfg.Key,
		}
		switch args[0] {
		case "import":
			request.Action = types.ManagementActionImport
			request.Import = &types.ImageImport{
				Layout:          cfg.OCILayout,
				Digest:          cfg.Digest,
				Materialization: cfg.Materialization,
				MaxBytes:        cfg.MaxBytes,
				MaxFiles:        cfg.MaxFiles,
			}
		case "gc":
			request.Action = types.ManagementActionGC
			request.GC = &types.GCOptions{
				DryRun: cfg.DryRun,
				Batch:  cfg.GCBatch,
				Budget: cfg.GCBudget,
			}
		case "supply-cleanup":
			request.Action = types.ManagementActionSupplyCleanup
			request.OperationID = cfg.SourceOperation
		case "image-cleanup":
			request.Action = types.ManagementActionImageCleanup
			request.OperationID = cfg.SourceOperation
			request.CacheBuilder = cfg.CacheBuilder
			request.CacheDiffID = cfg.CacheDiffID
		case "request":
			if err := clijson.ReadFile(cfg.RequestFile, &request); err != nil {
				return err
			}
			if cfg.Key != "" {
				if request.Key != "" && request.Key != cfg.Key {
					return errors.New("request file key differs from --key")
				}
				request.Key = cfg.Key
			}
		}
		if request.Key == "" && (request.Action != types.ManagementActionGC || request.GC == nil || !request.GC.DryRun) {
			return errors.New("--key is required; reuse it when retrying this intent")
		}
		value, err = client.Manage(ctx, request)
	default:
		if len(args) != 1 {
			return errors.New("management commands take options before the command")
		}
		return fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		return err
	}
	return clijson.Write(os.Stdout, value)
}
