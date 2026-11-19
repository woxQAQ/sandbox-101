package main

import (
	"github.com/spf13/pflag"

	loader "github.com/woxQAQ/haven/common/config"
)

type settings struct {
	Config      string `mapstructure:"config"`
	Endpoint    string `mapstructure:"endpoint"`
	TokenFile   string `mapstructure:"token-file"`
	Tenant      string `mapstructure:"tenant"`
	Key         string `mapstructure:"key"`
	RequestFile string `mapstructure:"request-file"`
}

func newFlagSet() *pflag.FlagSet {
	flags := pflag.NewFlagSet("havenctl", pflag.ContinueOnError)
	flags.SetInterspersed(false)
	flags.String("config", "", "explicit configuration file")
	flags.String("endpoint", "http://127.0.0.1:7777", "Haven business HTTP endpoint")
	flags.String("token-file", "", "private bearer token file")
	flags.String("tenant", "", "root administrator's target tenant")
	flags.String("key", "", "stable idempotency key for one intent")
	flags.String("request-file", "", "private JSON intent file for tenant, claim, template, change or fork-checkpoint")
	return flags
}

func parseConfig(args []string) (settings, []string, error) {
	flags := newFlagSet()
	var result settings
	if err := flags.Parse(args); err != nil {
		return result, nil, err
	}
	err := loader.Load(flags, "HAVENCTL", &result)
	return result, flags.Args(), err
}
