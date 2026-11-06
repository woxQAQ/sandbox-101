package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

type testConfig struct {
	Config   string        `mapstructure:"config"`
	StateDir string        `mapstructure:"state-dir"`
	Init     bool          `mapstructure:"init"`
	Interval time.Duration `mapstructure:"interval"`
	Count    int64         `mapstructure:"count"`
	UID      uint          `mapstructure:"uid"`
}

func loadTestConfig(args []string, prefix string) (testConfig, error) {
	f := pflag.NewFlagSet("test", pflag.ContinueOnError)
	f.String("config", "", "")
	f.String("state-dir", "/default", "")
	f.Bool("init", false, "")
	f.Duration("interval", time.Minute, "")
	f.Int64("count", 16, "")
	f.Uint("uid", 65534, "")
	var c testConfig
	if err := f.Parse(args); err != nil {
		return c, err
	}
	err := Load(f, prefix, &c)
	return c, err
}

func configFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPrecedence(t *testing.T) {
	path := configFile(t, "config.yaml", "state-dir: /file\ninit: true\ninterval: 2m\ncount: 32\nuid: 1000\n")
	c, err := loadTestConfig(nil, "CONFIG_TEST")
	if err != nil || c.StateDir != "/default" || c.Init || c.Interval != time.Minute || c.Count != 16 || c.UID != 65534 {
		t.Fatalf("defaults: %+v, %v", c, err)
	}
	c, err = loadTestConfig([]string{"--config", path}, "CONFIG_TEST")
	if err != nil || c.StateDir != "/file" || !c.Init || c.Interval != 2*time.Minute || c.Count != 32 || c.UID != 1000 {
		t.Fatalf("file: %+v, %v", c, err)
	}
	t.Setenv("CONFIG_TEST_STATE_DIR", "/env")
	t.Setenv("CONFIG_TEST_INIT", "true")
	t.Setenv("CONFIG_TEST_INTERVAL", "3m")
	t.Setenv("CONFIG_TEST_COUNT", "64")
	t.Setenv("CONFIG_TEST_UID", "1001")
	c, err = loadTestConfig([]string{"--config", path}, "CONFIG_TEST")
	if err != nil || c.StateDir != "/env" || !c.Init || c.Interval != 3*time.Minute || c.Count != 64 || c.UID != 1001 {
		t.Fatalf("env: %+v, %v", c, err)
	}
	c, err = loadTestConfig([]string{"--config", path, "--state-dir=/flag", "--init=false", "--interval=0", "--count=0", "--uid=0"}, "CONFIG_TEST")
	if err != nil || c.StateDir != "/flag" || c.Init || c.Interval != 0 || c.Count != 0 || c.UID != 0 {
		t.Fatalf("flags: %+v, %v", c, err)
	}
}

func TestInvalidValues(t *testing.T) {
	for _, content := range []string{
		"init: perhaps", "init: 2", "interval: someday", "count: many", "count: 1.5",
		"count: 18446744073709551615", "uid: -1", "uid: -1.5", "uid: 1.5",
		"unknown-option: true", "unknown-option: null", "unknown-option: {}", "unknown-option: []",
		"init: null", "init: {}", "init: [true]", "count: {nested: 1}",
	} {
		t.Run(content, func(t *testing.T) {
			path := configFile(t, "config.yaml", content+"\n")
			if c, err := loadTestConfig([]string{"--config", path}, "CONFIG_TEST"); err == nil {
				t.Fatalf("accepted %q: %+v", content, c)
			}
		})
	}
	for key, value := range map[string]string{"INIT": "perhaps", "INTERVAL": "someday", "COUNT": "many", "UID": "-1"} {
		t.Run("env/"+key, func(t *testing.T) {
			t.Setenv("CONFIG_TEST_"+key, value)
			if _, err := loadTestConfig(nil, "CONFIG_TEST"); err == nil {
				t.Fatalf("accepted invalid environment %s=%q", key, value)
			}
		})
	}
	for _, arg := range []string{"--init=perhaps", "--interval=someday", "--count=many", "--uid=-1"} {
		if _, err := loadTestConfig([]string{arg}, "CONFIG_TEST"); err == nil {
			t.Errorf("accepted invalid flag %s", arg)
		}
	}
}

func TestExplicitConfigFiles(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	invalid := configFile(t, "invalid.yaml", "init: [\n")
	unsupported := configFile(t, "config.unsupported", "init: true\n")
	for _, path := range []string{missing, invalid, unsupported} {
		if _, err := loadTestConfig([]string{"--config", path}, "CONFIG_TEST"); err == nil {
			t.Errorf("accepted unreadable config %s", path)
		}
	}
	for name, content := range map[string]string{
		"config.json": `{"state-dir":"/json","init":true,"interval":"2s","count":42,"uid":1000}`,
		"config.toml": "state-dir = '/toml'\ninit = true\ninterval = '2s'\ncount = 42\nuid = 1000\n",
	} {
		path := configFile(t, name, content)
		c, err := loadTestConfig([]string{"--config", path}, "CONFIG_TEST")
		if err != nil || !c.Init || c.Interval != 2*time.Second || c.Count != 42 || c.UID != 1000 {
			t.Errorf("%s: %+v, %v", name, c, err)
		}
	}
}

func TestEnvironmentAndInstanceIsolation(t *testing.T) {
	path := configFile(t, "config.yaml", "state-dir: /file\n")
	t.Setenv("CONFIG_ONE_CONFIG", path)
	t.Setenv("CONFIG_ONE_STATE_DIR", "/one")
	t.Setenv("CONFIG_TWO_STATE_DIR", "/two")
	first, err := loadTestConfig(nil, "CONFIG_ONE")
	if err != nil || first.Config != path || first.StateDir != "/one" {
		t.Fatalf("first: %+v, %v", first, err)
	}
	second, err := loadTestConfig(nil, "CONFIG_TWO")
	if err != nil || second.Config != "" || second.StateDir != "/two" {
		t.Fatalf("second: %+v, %v", second, err)
	}
	t.Setenv("CONFIG_ONE_STATE_DIR", "")
	empty, err := loadTestConfig(nil, "CONFIG_ONE")
	if err != nil || empty.StateDir != "" {
		t.Fatalf("empty environment override: %+v, %v", empty, err)
	}
	t.Setenv("CONFIG_ONE_INIT", "")
	if _, err = loadTestConfig(nil, "CONFIG_ONE"); err == nil {
		t.Fatal("accepted empty bool environment")
	}
}

func TestNoImplicitDiscovery(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("init: [invalid\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	if _, err := loadTestConfig(nil, "CONFIG_TEST"); err != nil {
		t.Fatalf("loaded an implicit config: %v", err)
	}
}
