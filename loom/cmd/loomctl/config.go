package main

import (
	"time"

	"github.com/spf13/pflag"

	loader "github.com/woxQAQ/haven/common/config"
	"github.com/woxQAQ/haven/loom/config"
)

func newFlagSet() *pflag.FlagSet {
	flags := pflag.NewFlagSet("loomctl", pflag.ContinueOnError)
	flags.SetInterspersed(false)
	flags.String("config", "", "explicit configuration file")
	flags.String("socket", "/run/haven/management.sock", "Haven local administration socket")
	flags.String("key", "", "stable idempotency key for one intent")
	flags.String("request-file", "", "private JSON intent file for request")
	flags.String("oci-layout", "", "local OCI layout")
	flags.String("digest", "", "OCI manifest digest")
	flags.String("materialization", "", "layered-erofs-v1 or merged-oci-erofs-v1")
	flags.Int64("max-bytes", 0, "bounded unpacked image bytes (0 selects importer default)")
	flags.Int64("max-files", 0, "bounded image entries (0 selects importer default)")
	flags.Bool("dry-run", false, "read-only GC inventory")
	flags.Int("gc-batch", 16, "maximum artifacts per collection pass")
	flags.Duration("gc-budget", 2*time.Second, "collection time budget")
	flags.String("source-operation", "", "original supply or import operation")
	flags.String("cache-builder", "", "original image-cache builder identity")
	flags.String("cache-diff-id", "", "original image-cache diff identity")
	return flags
}

func parseConfig(args []string) (config.CLI, []string, error) {
	flags := newFlagSet()
	var result config.CLI
	if err := flags.Parse(args); err != nil {
		return result, nil, err
	}
	err := loader.Load(flags, "LOOMCTL", &result)
	return result, flags.Args(), err
}
