package config

import (
	"time"

	"github.com/spf13/pflag"
)

type HavenCLI struct {
	Config    string `mapstructure:"config"`
	Endpoint  string `mapstructure:"endpoint"`
	TokenFile string `mapstructure:"token-file"`
	Tenant    string `mapstructure:"tenant"`
	Key       string `mapstructure:"key"`
}

func ParseHavenCLI(args []string) (HavenCLI, []string, error) {
	flags := pflag.NewFlagSet("havenctl", pflag.ContinueOnError)
	flags.SetInterspersed(false)
	flags.String("config", "", "explicit configuration file")
	flags.String("endpoint", "http://127.0.0.1:7777", "Haven business HTTP endpoint")
	flags.String("token-file", "", "private bearer token file")
	flags.String("tenant", "", "root administrator's target tenant")
	flags.String("key", "", "stable idempotency key for one intent")
	var result HavenCLI
	if err := flags.Parse(args); err != nil {
		return result, nil, err
	}
	err := Load(flags, "HAVENCTL", &result)
	return result, flags.Args(), err
}

type LoomCLI struct {
	Config          string        `mapstructure:"config"`
	Socket          string        `mapstructure:"socket"`
	Key             string        `mapstructure:"key"`
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

func ParseLoomCLI(args []string) (LoomCLI, []string, error) {
	flags := pflag.NewFlagSet("loomctl", pflag.ContinueOnError)
	flags.SetInterspersed(false)
	flags.String("config", "", "explicit configuration file")
	flags.String("socket", "/run/haven/management.sock", "Haven local administration socket")
	flags.String("key", "", "stable idempotency key for one intent")
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
	var result LoomCLI
	if err := flags.Parse(args); err != nil {
		return result, nil, err
	}
	err := Load(flags, "LOOMCTL", &result)
	return result, flags.Args(), err
}
