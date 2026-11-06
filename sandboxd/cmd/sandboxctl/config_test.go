package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestConfigPrecedenceAndNamespaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sandboxctl.yaml")
	if err := os.WriteFile(path, []byte("endpoint: http://file\ntoken-file: /file/token\nkey: file-key\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SANDBOXCTL_ENDPOINT", "http://env")
	t.Setenv("SANDBOXCTL_TOKEN_FILE", "/env/token")
	t.Setenv("SANDBOXD_KEY", "wrong-namespace")
	c, args, err := parseConfig([]string{"--config", path, "--endpoint=http://flag", "--key=", "list"})
	if err != nil || c.Endpoint != "http://flag" || c.TokenFile != "/env/token" || c.Key != "" || !reflect.DeepEqual(args, []string{"list"}) {
		t.Fatalf("precedence: %+v, %v, %v", c, args, err)
	}
	c, _, err = parseConfig([]string{"list"})
	if err != nil || c.Endpoint != "http://env" || c.Key != "" {
		t.Fatalf("config leaked between calls: %+v, %v", c, err)
	}
}

func TestCommandArgumentPassthrough(t *testing.T) {
	for _, command := range [][]string{
		{"exec", "sandbox-id", "sh", "-c", "printf '%s' hello", "--help"},
		{"exec", "sandbox-id", "--endpoint=http://guest", "--key=guest-key", "-x"},
		{"exec", "sandbox-id", "--config", "/guest/config"},
		{"get", "--looks-like-a-flag"},
	} {
		input := append([]string{"--token-file=/host/token"}, command...)
		c, args, err := parseConfig(input)
		if err != nil || !reflect.DeepEqual(args, command) || c.TokenFile != "/host/token" || c.Endpoint != "http://127.0.0.1:7777" || c.Key != "" {
			t.Errorf("%q: %+v, %q, %v", input, c, args, err)
		}
	}
}

func TestConfigRejectsUnknownKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sandboxctl.yaml")
	if err := os.WriteFile(path, []byte("token-fiel: /typo\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseConfig([]string{"--config", path, "list"}); err == nil {
		t.Fatal("accepted unknown config key")
	}
}

func TestConfigExample(t *testing.T) {
	c, args, err := parseConfig([]string{"--config", "../../../deploy/client/sandboxctl.example.yaml", "list"})
	if err != nil || c.Endpoint != "http://127.0.0.1:7777" || c.TokenFile != "/etc/sandboxd/tenant-token" || !reflect.DeepEqual(args, []string{"list"}) {
		t.Fatalf("example: %+v, %q, %v", c, args, err)
	}
}
