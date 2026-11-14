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

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/internal/cli"
	"github.com/woxQAQ/haven/internal/config"
	"github.com/woxQAQ/haven/sdk"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	cfg, args, err := config.ParseLoomCLI(arguments)
	if errors.Is(err, pflag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("usage: loomctl [--socket PATH] [--key KEY] {status|operation ID|import|gc|supply-cleanup|image-cleanup|request}")
	}
	client := sdk.NewManagement(cfg.Socket)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if args[0] == "status" {
		if len(args) != 1 {
			return errors.New("status takes no arguments")
		}
		value, err := client.Status(ctx)
		if err != nil {
			return err
		}
		return cli.JSON(os.Stdout, value)
	}
	if args[0] == "operation" {
		if len(args) != 2 {
			return errors.New("operation requires an id")
		}
		value, err := client.Operation(ctx, args[1])
		if err != nil {
			return err
		}
		return cli.JSON(os.Stdout, value)
	}
	if len(args) != 1 {
		return errors.New("management commands take options before the command")
	}
	request := api.ManagementRequest{
		Key: cfg.Key,
	}
	switch args[0] {
	case "import":
		request.Action = "import"
		request.Import = &api.ImageImport{
			Layout:          cfg.OCILayout,
			Digest:          cfg.Digest,
			Materialization: cfg.Materialization,
			MaxBytes:        cfg.MaxBytes,
			MaxFiles:        cfg.MaxFiles,
		}
	case "gc":
		request.Action = "gc"
		request.GC = &api.GCOptions{
			DryRun: cfg.DryRun,
			Batch:  cfg.GCBatch,
			Budget: cfg.GCBudget,
		}
	case "supply-cleanup":
		request.Action = "supply_cleanup"
		request.OperationID = cfg.SourceOperation
	case "image-cleanup":
		request.Action = "image_cleanup"
		request.OperationID = cfg.SourceOperation
		request.CacheBuilder = cfg.CacheBuilder
		request.CacheDiffID = cfg.CacheDiffID
	case "request":
		if err := cli.ReadJSON(os.Stdin, &request); err != nil {
			return err
		}
		if cfg.Key != "" {
			if request.Key != "" && request.Key != cfg.Key {
				return errors.New("stdin key differs from --key")
			}
			request.Key = cfg.Key
		}
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if request.Key == "" && (request.Action != "gc" || request.GC == nil || !request.GC.DryRun) {
		return errors.New("--key is required; reuse it when retrying this intent")
	}
	value, err := client.Manage(ctx, request)
	if err != nil {
		return err
	}
	return cli.JSON(os.Stdout, value)
}
