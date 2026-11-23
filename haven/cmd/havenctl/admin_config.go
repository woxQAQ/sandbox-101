package main

import (
	"time"

	"github.com/spf13/pflag"

	loader "github.com/woxQAQ/haven/common/config"
)

type adminSettings struct {
	Config          string        `mapstructure:"config"`
	Socket          string        `mapstructure:"socket"`
	Key             string        `mapstructure:"key"`
	RequestFile     string        `mapstructure:"request-file"`
	OCILayout       string        `mapstructure:"oci-layout"`
	Digest          string        `mapstructure:"digest"`
	Materialization string        `mapstructure:"materialization"`
	MaxBytes        int64         `mapstructure:"max-bytes"`
	MaxFiles        int64         `mapstructure:"max-files"`
	DryRun          bool          `mapstructure:"dry-run"`
	GCBatch         int           `mapstructure:"gc-batch"`
	GCBudget        time.Duration `mapstructure:"gc-budget"`
	SourceOperation string        `mapstructure:"source-operation"`
	CacheBuilder    string        `mapstructure:"cache-builder"`
	CacheDiffID     string        `mapstructure:"cache-diff-id"`
}

const adminUsage = "usage: havenctl admin {status|operation ID|import|gc|supply-cleanup|image-cleanup|request} [options]"

func adminFlagSet() *pflag.FlagSet {
	flags := commandFlags("havenctl admin", adminUsage)
	flags.String("socket", "/run/haven/management.sock", "Haven local administration socket")
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

func parseAdminConfig(args []string) (adminSettings, []string, error) {
	flags := adminFlagSet()
	var result adminSettings
	if err := flags.Parse(args); err != nil {
		return result, nil, err
	}
	err := loader.Load(flags, "HAVENCTL_ADMIN", &result)
	return result, flags.Args(), err
}
