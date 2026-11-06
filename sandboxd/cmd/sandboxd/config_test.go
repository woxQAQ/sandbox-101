package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigDefaults(t *testing.T) {
	c, err := parseConfig(nil)
	if err != nil || c.StateDir != "/var/lib/sandboxd" ||
		c.EngineSocket != "/run/sandbox-engine/engine.sock" ||
		c.Listen != "127.0.0.1:7777" ||
		c.Mode != "serve" ||
		c.Init {
		t.Fatalf("defaults: %+v, %v", c, err)
	}
}

func TestConfigPrecedenceAndNamespaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sandboxd.yaml")
	if err := os.WriteFile(
		path,
		[]byte(
			"state-dir: /file\ninit: true\nroot-token-file: /file/token\nmode: backup\nbackup: /file/backup\n",
		),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SANDBOXD_STATE_DIR", "/env")
	t.Setenv("SANDBOXD_ROOT_TOKEN_FILE", "/env/token")
	t.Setenv("ENGINE_MODE", "doctor")
	c, err := parseConfig(
		[]string{"--config", path, "--state-dir=/flag", "--init=false"},
	)
	if err != nil || c.StateDir != "/flag" || c.Init ||
		c.RootTokenFile != "/env/token" ||
		c.Mode != "backup" ||
		c.Backup != "/file/backup" {
		t.Fatalf("precedence: %+v, %v", c, err)
	}
	c, err = parseConfig(nil)
	if err != nil || c.StateDir != "/env" || c.Init || c.Mode != "serve" {
		t.Fatalf("config leaked between calls: %+v, %v", c, err)
	}
}

func TestConfigRejectsInvalidSettings(t *testing.T) {
	for _, content := range []string{"init: sometimes\n", "root-token-fiel: /typo\n"} {
		path := filepath.Join(t.TempDir(), "sandboxd.yaml")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := parseConfig([]string{"--config", path}); err == nil {
			t.Fatalf("accepted %q", content)
		}
	}
}

func TestConfigExample(t *testing.T) {
	c, err := parseConfig(
		[]string{"--config", "../../../deploy/sandboxd/sandboxd.example.yaml"},
	)
	if err != nil || c.RootTokenFile != "/etc/sandboxd/root-token" ||
		c.Mode != "serve" {
		t.Fatalf("example: %+v, %v", c, err)
	}
}
