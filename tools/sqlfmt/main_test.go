package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Exercise the complete tool on a migration mixing compact and expanded
// statements, nested parentheses, string literals and index suffixes. Repeated
// invocation must be stable and invalid input must not be rewritten.
func TestFormatterCommand(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("testdata", "input.sql.txt"))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := os.ReadFile(filepath.Join("testdata", "expected.sql.txt"))
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "migration.sql")
	if err := os.WriteFile(file, source, 0o640); err != nil {
		t.Fatal(err)
	}
	invoke := func(args ...string) ([]byte, error) {
		t.Helper()
		command := exec.Command("go", append([]string{
			"run",
			".",
		}, args...)...)
		return command.CombinedOutput()
	}
	formatted, err := invoke(file)
	if err != nil {
		t.Fatalf("formatter: %v: %s", err, formatted)
	}
	if !bytes.Equal(expected, formatted) {
		t.Fatalf("unexpected output:\n%s", formatted)
	}
	if preserved, err := os.ReadFile(file); err != nil || !bytes.Equal(source, preserved) {
		t.Fatal("stdout mode changed the input", err)
	}
	if output, err := invoke("-w", file); err != nil {
		t.Fatalf("formatter: %v: %s", err, output)
	}
	if written, err := os.ReadFile(file); err != nil || !bytes.Equal(expected, written) {
		t.Fatal("stdout and write modes differ", err)
	}
	if second, err := invoke(file); err != nil || !bytes.Equal(expected, second) {
		t.Fatalf("formatter is not idempotent: %v:\n%s", err, second)
	}
	invalid := filepath.Join(t.TempDir(), "invalid.sql")
	if err := os.WriteFile(invalid, []byte("CREATE TABLE broken(id TEXT;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke("-w", invalid); err == nil {
		t.Fatal("invalid input succeeded")
	}
	if preserved, err := os.ReadFile(invalid); err != nil || string(preserved) != "CREATE TABLE broken(id TEXT;\n" {
		t.Fatal("invalid input was rewritten", err)
	}
}
