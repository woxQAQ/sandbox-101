// sandboxctl is a small control client; the SDK exposes the full streaming protocol.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/woxQAQ/sandboxd"
	"github.com/woxQAQ/sandboxd/sdk"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	cfg, args, err := parseConfig(os.Args[1:])
	if err != nil {
		return err
	}
	if len(args) == 0 || cfg.TokenFile == "" {
		return errors.New("usage: sandboxctl --token-file FILE [--key KEY] {list|get ID|claim|template|change ID|exec ID COMMAND...}; JSON commands on stdin")
	}
	raw, err := os.ReadFile(cfg.TokenFile)
	if err != nil {
		return err
	}
	client := sdk.New(cfg.Endpoint, strings.TrimSpace(string(raw)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	printJSON := func(v any) error { out := json.NewEncoder(os.Stdout); out.SetIndent("", "  "); return out.Encode(v) }
	readCommand := func() (sandboxd.Command, error) {
		var c sandboxd.Command
		d := json.NewDecoder(io.LimitReader(os.Stdin, (1<<20)+1))
		d.DisallowUnknownFields()
		if err := d.Decode(&c); err != nil {
			return c, err
		}
		var more any
		if err := d.Decode(&more); err != io.EOF {
			return c, errors.New("expected exactly one JSON command")
		}
		return c, nil
	}
	switch args[0] {
	case "list":
		v, err := client.List(ctx)
		if err != nil {
			return err
		}
		return printJSON(v)
	case "get":
		if len(args) != 2 {
			return errors.New("get requires sandbox ID")
		}
		v, err := client.Get(ctx, args[1])
		if err != nil {
			return err
		}
		return printJSON(v)
	case "claim", "template", "change":
		if cfg.Key == "" {
			return errors.New("--key is required; reuse it when retrying this intent")
		}
		c, err := readCommand()
		if err != nil {
			return err
		}
		var result sandboxd.Result
		switch args[0] {
		case "claim":
			result, err = client.Claim(ctx, cfg.Key, c)
		case "template":
			if c.Template == nil {
				return errors.New("template field is required")
			}
			result, err = client.PutTemplate(ctx, cfg.Key, *c.Template, c.Action == "template_build")
		case "change":
			if len(args) != 2 {
				return errors.New("change requires sandbox ID")
			}
			result, err = client.Change(ctx, args[1], cfg.Key, c)
		}
		if err != nil {
			return err
		}
		return printJSON(result)
	case "exec":
		if len(args) < 3 {
			return errors.New("exec requires sandbox ID and command")
		}
		conn, err := client.Connect(ctx, args[1])
		if err != nil {
			return err
		}
		defer conn.Close()
		// One argument is a shell program; multiple arguments preserve their boundaries.
		command := args[2]
		if len(args) > 3 {
			parts := make([]string, len(args)-2)
			for i, a := range args[2:] {
				parts[i] = "'" + strings.ReplaceAll(a, "'", "'\\''") + "'"
			}
			command = strings.Join(parts, " ")
		}
		code, err := conn.Exec(ctx, command, os.Stdout, os.Stderr)
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
}
