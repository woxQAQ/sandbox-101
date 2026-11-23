// havenctl accesses Haven's business HTTP API and local management socket.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
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
	if len(arguments) > 0 && arguments[0] == "admin" {
		return runAdmin(arguments[1:])
	}
	cfg, args, err := parseConfig(arguments)
	if errors.Is(err, pflag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(args) == 0 || cfg.TokenFile == "" {
		return errors.New(businessUsage)
	}
	if cfg.RequestFile != "" && args[0] != "tenant" && args[0] != "claim" && args[0] != "template" && args[0] != "change" && args[0] != "fork-checkpoint" {
		return errors.New("--request-file is only valid for tenant, claim, template, change or fork-checkpoint")
	}
	token, err := os.ReadFile(cfg.TokenFile)
	if err != nil {
		return err
	}
	client := sdk.New(cfg.Endpoint, strings.TrimSpace(string(token)))
	client.Tenant = cfg.Tenant
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var value any
	switch args[0] {
	case "status", "list", "templates", "checkpoints":
		if len(args) != 1 {
			return fmt.Errorf("%s takes no arguments", args[0])
		}
		switch args[0] {
		case "status":
			value, err = client.Status(ctx)
		case "list":
			value, err = client.List(ctx)
		case "templates":
			value, err = client.Templates(ctx)
		case "checkpoints":
			value, err = client.Checkpoints(ctx)
		}
	case "get", "operation":
		if len(args) != 2 {
			return fmt.Errorf("%s requires an id", args[0])
		}
		if args[0] == "get" {
			value, err = client.Get(ctx, args[1])
		} else {
			value, err = client.Operation(ctx, args[1])
		}
	case "tenant":
		if len(args) != 1 {
			return errors.New("tenant requires --request-file FILE and takes no arguments")
		}
		var tenant types.Tenant
		if err = clijson.ReadFile(cfg.RequestFile, &tenant); err != nil {
			return err
		}
		value, err = client.PutTenant(ctx, tenant)
	case "claim", "template", "change", "fork-checkpoint":
		if cfg.Key == "" {
			return errors.New("--key is required; reuse it when retrying this intent")
		}
		wantArgs := 1
		if args[0] == "change" || args[0] == "fork-checkpoint" {
			wantArgs = 2
		}
		if len(args) != wantArgs {
			return fmt.Errorf("%s expects %d positional arguments", args[0], wantArgs-1)
		}
		var command types.Command
		if err = clijson.ReadFile(cfg.RequestFile, &command); err != nil {
			return err
		}
		switch args[0] {
		case "claim":
			value, err = client.Claim(ctx, cfg.Key, command)
		case "template":
			if command.Template == nil {
				return errors.New("template field is required")
			}
			value, err = client.PutTemplate(ctx, cfg.Key, *command.Template, command.Action == "template_build")
		case "fork-checkpoint":
			value, err = client.ForkCheckpoint(ctx, args[1], cfg.Key, command)
		case "change":
			value, err = client.Change(ctx, args[1], cfg.Key, command)
		}
	case "exec":
		if len(args) < 3 {
			return errors.New("exec requires sandbox id and command")
		}
		connection, err := client.Connect(ctx, args[1])
		if err != nil {
			return err
		}
		defer func() {
			_ = connection.Close()
		}()
		command := args[2]
		if len(args) > 3 {
			parts := make([]string, len(args)-2)
			for index, argument := range args[2:] {
				parts[index] = "'" + strings.ReplaceAll(argument, "'", "'\\''") + "'"
			}
			command = strings.Join(parts, " ")
		}
		code, err := connection.Exec(ctx, command, os.Stdout, os.Stderr)
		if err != nil {
			return err
		}
		if code != 0 {
			return fmt.Errorf("guest command exited with status %d", code)
		}
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		return err
	}
	return clijson.Write(os.Stdout, value)
}
