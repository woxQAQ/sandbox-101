// Command longcond reports Go control-flow conditions with too many logical terms.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	maxTerms := flag.Int("max", 4, "maximum logical terms per condition")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: longcond [flags] FILE_OR_DIR...")
		flag.PrintDefaults()
	}
	flag.Parse()
	if *maxTerms < 1 || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	status := 0
	for _, path := range flag.Args() {
		err := filepath.WalkDir(path, func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if name != path && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor" || entry.Name() == "testdata") {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(name) != ".go" {
				return nil
			}
			found, err := checkFile(name, *maxTerms)
			if found {
				status = max(status, 1)
			}
			return err
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			status = 2
		}
	}
	os.Exit(status)
}

func checkFile(path string, maxTerms int) (bool, error) {
	positions := token.NewFileSet()
	file, err := parser.ParseFile(positions, path, nil, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil || ast.IsGenerated(file) {
		return false, err
	}
	found := false
	check := func(expr ast.Expr) {
		if count := logicalTerms(expr); count > maxTerms {
			fmt.Printf("%s: condition has %d terms (max %d)\n", positions.PositionFor(expr.Pos(), false), count, maxTerms)
			found = true
		}
	}
	for node := range ast.Preorder(file) {
		switch stmt := node.(type) {
		case *ast.IfStmt:
			check(stmt.Cond)
		case *ast.ForStmt:
			if stmt.Cond != nil {
				check(stmt.Cond)
			}
		case *ast.SwitchStmt:
			if stmt.Tag == nil {
				for _, clause := range stmt.Body.List {
					for _, expr := range clause.(*ast.CaseClause).List {
						check(expr)
					}
				}
			}
		}
	}
	return found, nil
}

func logicalTerms(expr ast.Expr) int {
	switch e := ast.Unparen(expr).(type) {
	case *ast.BinaryExpr:
		if e.Op == token.LAND || e.Op == token.LOR {
			return logicalTerms(e.X) + logicalTerms(e.Y)
		}
	case *ast.UnaryExpr:
		if e.Op == token.NOT {
			return logicalTerms(e.X)
		}
	}
	return 1
}
