package main

import (
	"github.com/spf13/pflag"
	"github.com/woxQAQ/sandbox-engine/config"
)

type commandConfig struct {
	Config    string `mapstructure:"config"`
	Endpoint  string `mapstructure:"endpoint"`
	TokenFile string `mapstructure:"token-file"`
	Key       string `mapstructure:"key"`
}

func parseConfig(args []string) (commandConfig, []string, error) {
	f := pflag.NewFlagSet("sandboxctl", pflag.ContinueOnError)
	// Stop at the command, preserving guest arguments such as -c and --help.
	f.SetInterspersed(false)
	f.String("config", "", "configuration file (YAML, JSON, TOML or another Viper-supported format)")
	f.String("endpoint", "http://127.0.0.1:7777", "sandboxd endpoint")
	f.String("token-file", "", "file containing tenant, sandbox or root token")
	f.String("key", "", "persistent Idempotency-Key for mutations")
	var c commandConfig
	if err := f.Parse(args); err != nil {
		return c, nil, err
	}
	err := config.Load(f, "SANDBOXCTL", &c)
	return c, f.Args(), err
}
