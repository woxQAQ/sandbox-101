// structfmt expands keyed and array/slice literals, struct fields and statement blocks, then runs gofmt.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"sort"
)

type insertion struct {
	comma   bool
	newline bool
}

// layout records insertions at original source offsets. All braced sequences use
// the same line-break rule; only composite literals need a trailing comma.
type layout struct {
	source     []byte
	positions  *token.FileSet
	insertions map[int]insertion
}

func (layout *layout) newLine(position token.Pos) {
	offset := layout.positions.Position(position).Offset
	start := bytes.LastIndexByte(layout.source[:offset], '\n') + 1
	if len(bytes.TrimSpace(layout.source[start:offset])) != 0 {
		edit := layout.insertions[offset]
		edit.newline = true
		layout.insertions[offset] = edit
	}
}

func expand[T ast.Node](layout *layout, items []T, closing token.Pos, comma bool) {
	if len(items) == 0 {
		return
	}
	for _, item := range items {
		layout.newLine(item.Pos())
	}
	if comma {
		end := layout.positions.Position(items[len(items)-1].End()).Offset
		tail := layout.source[end:layout.positions.Position(closing).Offset]
		var scan scanner.Scanner
		scan.Init(token.NewFileSet().AddFile("", -1, len(tail)), tail, nil, 0)
		_, next, _ := scan.Scan()
		if next != token.COMMA {
			edit := layout.insertions[end]
			edit.comma = true
			layout.insertions[end] = edit
		}
	}
	layout.newLine(closing)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	write := flag.Bool("w", false, "write formatted files in place")
	flag.Parse()
	if flag.NArg() == 0 {
		return fmt.Errorf("usage: go tool structfmt [-w] FILE")
	}
	for _, name := range flag.Args() {
		source, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, name, source, parser.ParseComments)
		if err != nil {
			return err
		}
		layout := &layout{
			source:     source,
			positions:  positions,
			insertions: map[int]insertion{},
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.CompositeLit:
				if _, array := node.Type.(*ast.ArrayType); !array {
					for _, item := range node.Elts {
						if _, keyed := item.(*ast.KeyValueExpr); !keyed {
							return true
						}
					}
				}
				expand(layout, node.Elts, node.Rbrace, true)
			case *ast.StructType:
				expand(layout, node.Fields.List, node.Fields.Closing, false)
			case *ast.BlockStmt:
				expand(layout, node.List, node.Rbrace, false)
			}
			return true
		})
		offsets := make([]int, 0, len(layout.insertions))
		for offset := range layout.insertions {
			offsets = append(offsets, offset)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(offsets)))
		for _, offset := range offsets {
			edit := layout.insertions[offset]
			var text string
			if edit.comma {
				text += ","
			}
			if edit.newline {
				text += "\n"
			}
			source = append(append(append([]byte{}, source[:offset]...), text...), source[offset:]...)
		}
		formatted, err := format.Source(source)
		if err != nil {
			return fmt.Errorf("format %s: %w", name, err)
		}
		if *write {
			if err = os.WriteFile(name, formatted, 0o600); err != nil {
				return err
			}
		} else if _, err = os.Stdout.Write(formatted); err != nil {
			return err
		}
	}
	return nil
}
