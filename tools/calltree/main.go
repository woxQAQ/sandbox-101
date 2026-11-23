// Command calltree exports the Haven daemon's Go source and call relationships for naming review.
package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

const (
	daemon = "github.com/woxQAQ/haven/cmd/haven"
	api    = "github.com/woxQAQ/haven/api"
)

type report struct {
	Schema    int                    `json:"schema"`
	Scope     string                 `json:"scope"`
	Main      string                 `json:"main"`
	Routes    []route                `json:"routes"`
	Functions map[string]function    `json:"functions"`
	Files     map[string]*sourceFile `json:"files"`
}

type sourceFile struct {
	Package string            `json:"package"`
	Source  string            `json:"source"`
	Names   map[string]string `json:"definitions"` // line:column -> typed definition
	Uses    map[string]string `json:"uses"`        // line:column -> repo-relative definition position
}

type function struct {
	Signature string   `json:"signature"`
	At        string   `json:"at,omitempty"`
	End       string   `json:"end,omitempty"`
	Synthetic string   `json:"synthetic,omitempty"`
	Callers   []string `json:"callers,omitempty"`
	Calls     []site   `json:"calls,omitempty"`
}

type site struct {
	At           string   `json:"at,omitempty"`
	Dispatch     string   `json:"dispatch"`
	Mode         string   `json:"mode,omitempty"`
	Operand      string   `json:"operand,omitempty"`
	Targets      []string `json:"targets"`
	FunctionArgs []string `json:"function_args,omitempty"`
}

type index struct {
	root     string
	fset     *token.FileSet
	program  *ssa.Program
	local    map[*types.Package]bool
	literals map[*ast.FuncLit]*ssa.Function
}

//go:embed viewer.html
var viewer string

//go:embed graph.js
var graphScript string

func main() {
	html := flag.Bool("html", false, "write an offline HTML viewer with the JSON report embedded")
	flag.Parse()
	root, err := os.Getwd()
	if err == nil {
		var result *report
		result, err = analyze(root)
		if err == nil {
			err = writeReport(os.Stdout, result, *html)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func writeReport(w io.Writer, result *report, html bool) error {
	if !html {
		return json.NewEncoder(w).Encode(result)
	}
	before, after, found := strings.Cut(viewer, "<!-- snapshot -->")
	if !found {
		return fmt.Errorf("viewer is missing its snapshot slot")
	}
	if _, err := io.WriteString(w, before+`<script type="application/json" id="snapshot">`); err != nil {
		return err
	}
	// The encoder escapes HTML characters in source text inside the script element.
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return err
	}
	after = strings.Replace(after, `<script src="graph.js"></script>`, "<script>"+graphScript+"</script>", 1)
	_, err := io.WriteString(w, "</script>"+after)
	return err
}

func analyze(root string) (*report, error) {
	cfg := &packages.Config{Dir: root, Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps | packages.NeedModule}
	deps, err := packages.Load(cfg, "./haven/cmd/haven")
	if err != nil {
		return nil, err
	}
	if len(deps) != 1 || deps[0].PkgPath != daemon || packages.PrintErrors(deps) != 0 {
		return nil, fmt.Errorf("load %s from the repository root", daemon)
	}
	var paths []string
	packages.Visit(deps, func(p *packages.Package) bool {
		if p.Module != nil && p.Module.Main {
			paths = append(paths, p.PkgPath)
		}
		return true
	}, nil)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no workspace dependencies found, enable the repository go.work")
	}
	slices.Sort(paths)
	cfg.Mode = packages.LoadSyntax
	pkgs, err := packages.Load(cfg, paths...)
	if err != nil {
		return nil, err
	}
	if packages.PrintErrors(pkgs) != 0 {
		return nil, fmt.Errorf("type checking failed")
	}
	program, _ := ssautil.Packages(pkgs, ssa.InstantiateGenerics)
	program.Build()
	x := index{
		root: root, fset: program.Fset, program: program,
		local: map[*types.Package]bool{}, literals: map[*ast.FuncLit]*ssa.Function{},
	}
	out := &report{
		Schema: 1,
		Scope:  "Current Go build, workspace packages imported by haven/cmd/haven, excluding tests. External bodies, reflection, native and guest code are boundaries. Dynamic targets are VTA candidates; branches are not pruned.",
		Main:   daemon + ".main",
		Files:  map[string]*sourceFile{}, Functions: map[string]function{},
	}
	for _, p := range pkgs {
		x.local[p.Types] = true
	}
	funcs := ssautil.AllFunctions(program)
	// Include declarations on private receiver types even when no address is taken.
	for _, p := range pkgs {
		for _, obj := range p.TypesInfo.Defs {
			if method, ok := obj.(*types.Func); ok {
				if f := program.FuncValue(method); f != nil {
					addBodies(funcs, f)
				}
			}
		}
	}
	graph := vta.CallGraph(funcs, nil)
	for f, n := range graph.Nodes {
		if !x.owns(f) {
			continue
		}
		if lit, ok := f.Syntax().(*ast.FuncLit); ok {
			x.literals[lit] = f
		}
		entry := function{Signature: f.Signature.String(), At: x.position(f.Pos()), Synthetic: f.Synthetic}
		if syntax := f.Syntax(); syntax != nil {
			entry.End = x.position(syntax.End())
		}
		resolved := map[ssa.CallInstruction][]string{}
		for _, e := range n.Out {
			resolved[e.Site] = append(resolved[e.Site], functionID(e.Callee.Func))
		}
		for _, block := range f.Blocks {
			for _, inst := range block.Instrs {
				if call, ok := inst.(ssa.CallInstruction); ok {
					entry.Calls = append(entry.Calls, x.callSite(call, resolved[call]))
				}
			}
		}
		// SSA can materialize equivalent bound-method wrappers at different sites.
		id := functionID(f)
		if previous, exists := out.Functions[id]; exists && !reflect.DeepEqual(previous, entry) {
			return nil, fmt.Errorf("ambiguous SSA function ID %s", id)
		}
		out.Functions[id] = entry
	}
	linkCallers(out.Functions)
	for _, p := range pkgs {
		if err := x.sources(out, p); err != nil {
			return nil, err
		}
		if p.PkgPath == api {
			out.Routes, err = x.routes(p)
			if err != nil {
				return nil, err
			}
		}
	}
	if len(out.Routes) == 0 {
		return nil, fmt.Errorf("no API routes found")
	}
	return out, nil
}

func linkCallers(functions map[string]function) {
	for caller, f := range functions {
		seen := map[string]bool{}
		for _, call := range f.Calls {
			for _, id := range call.Targets {
				target, ok := functions[id]
				if ok && !seen[id] {
					target.Callers = append(target.Callers, caller)
					functions[id] = target
					seen[id] = true
				}
			}
		}
	}
	for _, f := range functions {
		slices.Sort(f.Callers)
	}
}

func functionID(f *ssa.Function) string {
	if f.Synthetic != "" {
		return f.String() + " [" + f.Synthetic + "]"
	}
	return f.String()
}

func addBodies(funcs map[*ssa.Function]bool, f *ssa.Function) {
	funcs[f] = true
	for _, child := range f.AnonFuncs {
		addBodies(funcs, child)
	}
}

func (x index) owns(f *ssa.Function) bool {
	if f.Pkg != nil {
		return x.local[f.Pkg.Pkg]
	}
	if parent := f.Parent(); parent != nil {
		return x.owns(parent)
	}
	if recv := f.Signature.Recv(); recv != nil {
		t := recv.Type()
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		if t, ok := t.(*types.Named); ok && x.local[t.Obj().Pkg()] {
			return true
		}
	}
	if obj := f.Object(); obj != nil {
		return x.local[obj.Pkg()]
	}
	return false
}

func (x index) callSite(call ssa.CallInstruction, targets []string) site {
	if targets == nil {
		targets = []string{}
	}
	slices.Sort(targets)
	s := site{At: x.position(call.Pos()), Dispatch: "direct", Targets: slices.Compact(targets)}
	cc := call.Common()
	switch {
	case cc.IsInvoke():
		s.Dispatch, s.Operand = "interface", cc.Method.FullName()
	case cc.StaticCallee() != nil:
		s.Targets = []string{functionID(cc.StaticCallee())}
	case cc.Value != nil:
		if builtin, ok := cc.Value.(*ssa.Builtin); ok {
			s.Dispatch, s.Operand = "builtin", builtin.Name()
		} else {
			s.Dispatch, s.Operand = "function_value", cc.Value.Name()
		}
	}
	switch call.(type) {
	case *ssa.Go:
		s.Mode = "go"
	case *ssa.Defer:
		s.Mode = "defer"
	}
	for _, arg := range cc.Args {
		switch v := arg.(type) {
		case *ssa.Function:
			s.FunctionArgs = append(s.FunctionArgs, functionID(v))
		case *ssa.MakeClosure:
			s.FunctionArgs = append(s.FunctionArgs, functionID(v.Fn.(*ssa.Function)))
		}
	}
	return s
}

func (x index) position(pos token.Pos) string {
	if !pos.IsValid() {
		return ""
	}
	p := x.fset.PositionFor(pos, false)
	name, err := filepath.Rel(x.root, p.Filename)
	if err != nil || !filepath.IsLocal(name) {
		return ""
	}
	return fmt.Sprintf("%s:%d:%d", filepath.ToSlash(name), p.Line, p.Column)
}

func (x index) sources(out *report, p *packages.Package) error {
	for _, file := range p.Syntax {
		path := x.fset.PositionFor(file.Pos(), false).Filename
		name, err := filepath.Rel(x.root, path)
		if err != nil || !filepath.IsLocal(name) {
			return fmt.Errorf("unexpected workspace source %s", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		s := &sourceFile{p.PkgPath, string(data), map[string]string{}, map[string]string{}}
		out.Files[filepath.ToSlash(name)] = s
		ast.Inspect(file, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok || id.Name == "_" {
				return true
			}
			pos := x.fset.PositionFor(id.Pos(), false)
			key := fmt.Sprintf("%d:%d", pos.Line, pos.Column)
			if obj := p.TypesInfo.Defs[id]; obj != nil {
				s.Names[key] = types.ObjectString(obj, types.RelativeTo(p.Types))
			} else if obj := p.TypesInfo.Uses[id]; obj != nil && x.local[obj.Pkg()] {
				if target := x.position(obj.Pos()); target != "" {
					s.Uses[key] = target
				}
			}
			return true
		})
	}
	return nil
}
