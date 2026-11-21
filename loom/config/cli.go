// Package config defines Loom management-client settings; parsing lives in cmd/loomctl.
package config

import "time"

type CLI struct {
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
