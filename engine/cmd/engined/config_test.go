package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	c, err := parseConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Mode != "serve" || c.StateDir != "/var/lib/sandbox-engine" || c.Socket != "/run/sandbox-engine/engine.sock" || c.Init || c.GCDryRun || c.GCBatch != 16 || c.GCInterval != 0 || c.VMMUID != 65534 || c.VMMGID != 65534 || c.StorageMarginBytes != 256<<20 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestConfigPrecedenceAndNamespaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engine.yaml")
	if err := os.WriteFile(path, []byte("mode: gc\ninit: true\ngc-dry-run: true\ngc-batch: 32\ngc-interval: 2m\nmax-memory-mib: 4096\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ENGINE_GC_BATCH", "64")
	t.Setenv("ENGINE_GC_INTERVAL", "3m")
	t.Setenv("SANDBOXD_STATE_DIR", "/wrong-namespace")
	c, err := parseConfig([]string{"--config", path, "--init=false", "--gc-dry-run=false", "--max-memory-mib=0"})
	if err != nil || c.Mode != "gc" || c.Init || c.GCDryRun || c.GCBatch != 64 || c.GCInterval != 3*time.Minute || c.MaxMemoryMiB != 0 || c.StateDir != "/var/lib/sandbox-engine" {
		t.Fatalf("precedence: %+v, %v", c, err)
	}
	c, err = parseConfig(nil)
	if err != nil || c.Mode != "serve" || c.GCDryRun || c.MaxMemoryMiB != 0 {
		t.Fatalf("config leaked between calls: %+v, %v", c, err)
	}
}

func TestConfigRejectsNegativeIDs(t *testing.T) {
	for _, name := range []string{"VMM_UID", "VMM_GID"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("ENGINE_"+name, "-1")
			if _, err := parseConfig(nil); err == nil {
				t.Fatal("accepted negative unsigned ID")
			}
		})
	}
}

func TestConfigExample(t *testing.T) {
	c, err := parseConfig([]string{"--config", "../../../deploy/host/engined.example.yaml"})
	if err != nil || c.Mode != "serve" || c.VMMUID != 65534 || c.GCInterval != 0 {
		t.Fatalf("example: %+v, %v", c, err)
	}
}
