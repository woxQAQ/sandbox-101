package main

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"maps"
	"slices"

	"golang.org/x/tools/go/packages"
)

type handler struct {
	Function string            `json:"function"`
	Bindings map[string]string `json:"bindings,omitempty"`
}

type route struct {
	Surface  string    `json:"surface"`
	Method   string    `json:"method"`
	Path     string    `json:"path,omitempty"`
	At       string    `json:"at"`
	Handlers []handler `json:"handlers"`
}

type group struct {
	prefix string
	chain  []handler
}

// The registration walk interprets the straight-line Gin constructors, including
// constant ranges. Unsupported control flow must surface before emitting a report.
type registrations struct {
	index

	pkg     *packages.Package
	surface string
	groups  map[string]group
	lists   map[string][]string
	values  map[string]string
	entries *[]route
}

func (x index) routes(p *packages.Package) ([]route, error) {
	var result []route
	for _, entry := range []struct{ name, surface string }{{"New", "business"}, {"NewManagement", "management"}} {
		obj, ok := p.Types.Scope().Lookup(entry.name).(*types.Func)
		if !ok {
			return nil, fmt.Errorf("missing API constructor %s", entry.name)
		}
		fn := x.program.FuncValue(obj)
		decl := fn.Syntax().(*ast.FuncDecl)
		r := registrations{x, p, entry.surface, map[string]group{}, map[string][]string{}, map[string]string{}, &result}
		if err := r.block(decl.Body); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (r registrations) block(block *ast.BlockStmt) error {
	for _, stmt := range block.List {
		var err error
		switch s := stmt.(type) {
		case *ast.BlockStmt:
			err = r.block(s)
		case *ast.AssignStmt:
			err = r.assignment(s)
		case *ast.ExprStmt:
			if c, ok := s.X.(*ast.CallExpr); ok {
				err = r.register(c)
			}
		case *ast.RangeStmt:
			err = r.each(s)
		case *ast.ReturnStmt:
		default:
			err = fmt.Errorf("unsupported router statement %T", s)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", r.position(stmt.Pos()), err)
		}
	}
	return nil
}

func (r registrations) assignment(s *ast.AssignStmt) error {
	if len(s.Lhs) != 1 || len(s.Rhs) != 1 {
		return fmt.Errorf("unsupported router assignment")
	}
	id, ok := s.Lhs[0].(*ast.Ident)
	if !ok {
		return nil
	} // Engine configuration fields.
	switch expr := s.Rhs[0].(type) {
	case *ast.CompositeLit:
		values, err := r.strings(expr)
		if err != nil {
			return err
		}
		r.lists[id.Name] = values
	case *ast.CallExpr:
		sel, ok := expr.Fun.(*ast.SelectorExpr)
		if !ok {
			return nil
		}
		obj := r.pkg.TypesInfo.Uses[sel.Sel]
		if obj == nil || obj.Pkg() == nil || obj.Pkg().Path() != "github.com/gin-gonic/gin" {
			return nil
		}
		switch sel.Sel.Name {
		case "New":
			r.groups[id.Name] = group{}
		case "SetTrustedProxies":
		case "Group":
			parent, err := r.receiver(sel)
			if err != nil {
				return err
			}
			prefix, err := r.text(expr.Args[0])
			if err != nil {
				return err
			}
			chain, err := r.handlers(parent.chain, expr.Args[1:])
			if err != nil {
				return err
			}
			r.groups[id.Name] = group{parent.prefix + prefix, chain}
		default:
			return fmt.Errorf("unsupported Gin assignment %s", sel.Sel.Name)
		}
	}
	return nil
}

func (r registrations) receiver(sel *ast.SelectorExpr) (group, error) {
	if id, ok := sel.X.(*ast.Ident); ok {
		if g, ok := r.groups[id.Name]; ok {
			return g, nil
		}
	}
	return group{}, fmt.Errorf("unknown router for %s", sel.Sel.Name)
}

func (r registrations) each(s *ast.RangeStmt) error {
	id, ok := s.Value.(*ast.Ident)
	if !ok {
		return fmt.Errorf("unsupported range binding")
	}
	items, err := r.strings(s.X)
	if err != nil {
		return err
	}
	r.values = maps.Clone(r.values)
	for _, value := range items {
		r.values[id.Name] = value
		if err := r.block(s.Body); err != nil {
			return err
		}
	}
	return nil
}

func (r registrations) strings(expr ast.Expr) ([]string, error) {
	if id, ok := expr.(*ast.Ident); ok {
		if list, ok := r.lists[id.Name]; ok {
			return list, nil
		}
	}
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, fmt.Errorf("expected constant string list")
	}
	var result []string
	for _, x := range lit.Elts {
		value, err := r.text(x)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (r registrations) text(expr ast.Expr) (string, error) {
	if v := r.pkg.TypesInfo.Types[expr].Value; v != nil && v.Kind() == constant.String {
		return constant.StringVal(v), nil
	}
	switch x := expr.(type) {
	case *ast.Ident:
		if value, ok := r.values[x.Name]; ok {
			return value, nil
		}
	case *ast.BinaryExpr:
		if x.Op == token.ADD {
			a, err := r.text(x.X)
			if err != nil {
				return "", err
			}
			b, err := r.text(x.Y)
			return a + b, err
		}
	}
	return "", fmt.Errorf("unsupported route value at %s", r.position(expr.Pos()))
}

func (r registrations) register(c *ast.CallExpr) error {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok {
		return fmt.Errorf("unsupported registration call")
	}
	g, err := r.receiver(sel)
	if err != nil {
		return err
	}
	args := c.Args
	methods := []string{sel.Sel.Name}
	path := ""
	switch sel.Sel.Name {
	case "Use":
		g.chain, err = r.handlers(g.chain, args)
		r.groups[sel.X.(*ast.Ident).Name] = g
		return err
	case "NoMethod", "NoRoute":
	default:
		switch sel.Sel.Name {
		case "Match":
			methods, err = r.strings(args[0])
			args = args[1:]
		case "Any":
			methods = []string{"GET", "POST", "PUT", "PATCH", "HEAD", "OPTIONS", "DELETE", "CONNECT", "TRACE"}
		case "GET", "POST", "PUT", "PATCH", "HEAD", "OPTIONS", "DELETE":
		default:
			return fmt.Errorf("unsupported Gin registration %s", sel.Sel.Name)
		}
		if err != nil {
			return err
		}
		path, err = r.text(args[0])
		if err != nil {
			return err
		}
		path = g.prefix + path
		args = args[1:]
	}
	chain, err := r.handlers(g.chain, args)
	if err != nil {
		return err
	}
	for _, method := range methods {
		*r.entries = append(*r.entries, route{r.surface, method, path, r.position(c.Pos()), chain})
	}
	return nil
}

func (r registrations) handlers(base []handler, exprs []ast.Expr) ([]handler, error) {
	chain := slices.Clone(base)
	for _, expr := range exprs {
		h, err := r.handler(expr)
		if err != nil {
			return nil, err
		}
		chain = append(chain, h)
	}
	return chain, nil
}

func (r registrations) handler(expr ast.Expr) (handler, error) {
	if lit, ok := expr.(*ast.FuncLit); ok {
		if fn := r.literals[lit]; fn != nil {
			return handler{Function: fn.String()}, nil
		}
	}
	callee := expr
	factory, called := expr.(*ast.CallExpr)
	if called {
		callee = factory.Fun
	}
	var id *ast.Ident
	switch e := callee.(type) {
	case *ast.SelectorExpr:
		id = e.Sel
	case *ast.Ident:
		id = e
	}
	obj, ok := r.pkg.TypesInfo.Uses[id].(*types.Func)
	if !ok {
		return handler{}, fmt.Errorf("unsupported handler at %s", r.position(expr.Pos()))
	}
	fn := r.program.FuncValue(obj)
	if !called {
		return handler{Function: fn.String()}, nil
	}
	decl, ok := fn.Syntax().(*ast.FuncDecl)
	if ok && len(decl.Body.List) == 1 {
		if ret, ok := decl.Body.List[0].(*ast.ReturnStmt); ok && len(ret.Results) == 1 {
			if lit, ok := ret.Results[0].(*ast.FuncLit); ok && r.literals[lit] != nil {
				h := handler{Function: r.literals[lit].String(), Bindings: map[string]string{}}
				for i, arg := range factory.Args {
					v, err := r.text(arg)
					if err != nil {
						return handler{}, err
					}
					h.Bindings[fn.Signature.Params().At(i).Name()] = v
				}
				return h, nil
			}
		}
	}
	return handler{}, fmt.Errorf("unsupported handler factory %s", fn)
}
