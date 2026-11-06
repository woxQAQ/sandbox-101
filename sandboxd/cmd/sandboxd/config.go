package main

import (
	"github.com/spf13/pflag"

	"github.com/woxQAQ/sandbox-engine/config"
)

type commandConfig struct {
	Config        string `mapstructure:"config"`
	StateDir      string `mapstructure:"state-dir"`
	EngineSocket  string `mapstructure:"engine-socket"`
	Listen        string `mapstructure:"listen"`
	RootTokenFile string `mapstructure:"root-token-file"`
	Init          bool   `mapstructure:"init"`
	Mode          string `mapstructure:"mode"`
	Backup        string `mapstructure:"backup"`
	RecoveryNote  string `mapstructure:"recovery-note"`
	TLSCert       string `mapstructure:"tls-cert"`
	TLSKey        string `mapstructure:"tls-key"`
}

func parseConfig(args []string) (commandConfig, error) {
	f := pflag.NewFlagSet("sandboxd", pflag.ContinueOnError)
	f.SetInterspersed(false)
	f.String(
		"config",
		"",
		"configuration file (YAML, JSON, TOML or another Viper-supported format)",
	)
	f.String("state-dir", "/var/lib/sandboxd", "product metadata directory")
	f.String(
		"engine-socket",
		"/run/sandbox-engine/engine.sock",
		"engine Unix socket",
	)
	f.String(
		"listen",
		"127.0.0.1:7777",
		"HTTP address behind TLS gateway or on trusted network",
	)
	f.String("root-token-file", "", "file containing high entropy root token")
	f.Bool(
		"init",
		false,
		"initialize a fresh product database (first start only)",
	)
	f.String("mode", "serve", "serve, backup, restore-backup, or recover")
	f.String("backup", "", "new backup directory or restore source")
	f.String(
		"recovery-note",
		"",
		"explicit operator review of a restored metadata point",
	)
	f.String("tls-cert", "", "optional server TLS certificate")
	f.String("tls-key", "", "optional server TLS private key")
	var c commandConfig
	if err := f.Parse(args); err != nil {
		return c, err
	}
	err := config.Load(f, "SANDBOXD", &c)
	return c, err
}
