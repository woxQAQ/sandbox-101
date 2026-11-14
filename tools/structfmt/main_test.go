package main

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Exercise the complete tool on real files, including nested literals, field
// declarations, statement blocks and comments. Repeated invocations must be stable.
func TestFormatterCommand(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "input.go")
	source, err := os.ReadFile(filepath.Join("testdata", "input.go.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, source, 0o640); err != nil {
		t.Fatal(err)
	}
	original, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	invoke := func(path string) ([]byte, error) {
		command := exec.Command("go", "run", ".", "-w", path)
		return command.CombinedOutput()
	}
	if output, err := invoke(file); err != nil {
		t.Fatalf("formatter: %v: %s", err, output)
	}
	first, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = parser.ParseFile(token.NewFileSet(), file, first, parser.ParseComments); err != nil {
		t.Fatal(err)
	}
	for _, field := range [][]byte{
		[]byte("\n\tX: 1,"),
		[]byte("\n\tY: 2,"),
		[]byte("\n\tX: 3,"),
		[]byte("/* preserve */"),
		[]byte("`X:1,Y:2`"),
		[]byte("type Point struct {\n\tX, Y int\n}"),
		[]byte("\n\tLabel string `json:\"label\"`\n\t*Point\n}"),
		[]byte("\n\t\"first\": {\n"),
		[]byte("\n\t\"second\": {\n"),
		[]byte("\n\t\tX: 11,"),
		[]byte("\n\t1: \"one\",\n\t2: \"two\",\n}"),
		[]byte("\n\t3: \"three\",\n\t4: \"four\",\n}"),
		[]byte("/* tail comment */"),
		[]byte("\n\t\treturn 13\n\t},"),
		[]byte("\n\t\treturn 14\n\t},"),
		[]byte("func settings() map[string]int {\n\treturn map[string]int{\n"),
		[]byte("func sum(a, b int) int {\n\tvalue := a + b\n\tif value > 0 {\n\t\treturn value\n\t}\n\treturn 0\n}"),
		[]byte("var callback = func() int {\n\tvalue := 1\n\tvalue++\n\treturn value\n}"),
		[]byte("for i := 0; i < 2; i++ {\n\t\t_ = i\n\t}"),
	} {
		if !bytes.Contains(first, field) {
			t.Fatalf("missing field or changed content %q:\n%s", field, first)
		}
	}
	if output, err := invoke(file); err != nil {
		t.Fatalf("second formatter: %v: %s", err, output)
	}
	second, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("formatter is not idempotent", err)
	}
	info, err := os.Stat(file)
	if err != nil || info.Mode().Perm() != original.Mode().Perm() {
		t.Fatal("formatter changed file permissions", err)
	}
	invalid := filepath.Join(directory, "invalid.go")
	if err = os.WriteFile(invalid, []byte("package invalid; var ="), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = invoke(invalid); err == nil {
		t.Fatal("invalid input succeeded")
	}
	preserved, err := os.ReadFile(invalid)
	if err != nil || string(preserved) != "package invalid; var =" {
		t.Fatal("invalid input was rewritten", err)
	}
}
