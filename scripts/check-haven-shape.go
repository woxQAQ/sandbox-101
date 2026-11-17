package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/constant"
	"go/format"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/tools/go/packages"
)

const module = "github.com/woxQAQ/haven"
const application = module + "/internal/application"

type compiledPackage struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
}

// SQLite maintenance stays explicit by file, function and statement. Business
// queries, including metadata identity and epoch, belong to generated bindings.
var maintenanceSQL = map[string][]string{
	"store/state_linux.go#initialize": {
		"CREATE TRIGGER epoch_%s_%s AFTER %s ON %s BEGIN UPDATE store_metadata SET epoch=epoch+1 WHERE singleton=1; END",
		"PRAGMA wal_checkpoint(FULL)",
	},
	"store/state_linux.go#validate": {
		"PRAGMA quick_check", "PRAGMA application_id", "PRAGMA user_version",
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
		"SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?",
	},
	"store/migrations.go#Migrate": {
		"PRAGMA user_version", "PRAGMA application_id",
		"SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' AND name <> 'goose_db_version'",
		"SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='goose_db_version'",
		"PRAGMA application_id=%d; PRAGMA user_version=%d",
	},
	"store/validation.go#HasForeignKeyViolations": {"PRAGMA foreign_key_check"},
	"store/backup_linux.go#backup":                {"VACUUM INTO ?"},
}

var maintenanceCalls = map[string][]string{
	"store/state_linux.go#initialize": {"tx.Exec(statement)"},
	"store/migrations.go#Migrate": {
		"tx.ExecContext(ctx, baselineSQL)",
		"tx.ExecContext(ctx, fmt.Sprintf(\"PRAGMA application_id=%d; PRAGMA user_version=%d\", ApplicationID, Version))",
	},
	"store/statements.go#readStatement": {"database.PrepareContext(ctx, query)"},
	"store/statements.go#QueryRowContext": {
		"statement.QueryRowContext(ctx, args...)", "database.DB.QueryRowContext(ctx, query, args...)",
	},
	"store/statements.go#QueryContext": {
		"statement.QueryContext(ctx, args...)", "database.DB.QueryContext(ctx, query, args...)",
	},
}

// These calls prepare sandbox commands rather than SQL statements.
var commandCalls = map[string][]string{
	"sandbox/configure.go#admitConfiguration": {"service.commands.Prepare(desired)"},
	"sandbox/fork_admission.go#AdmitFork":     {"service.commands.Prepare(command)"},
	"sandbox/claim.go#AdmitClaim":             {"service.commands.Prepare(command)"},
}

var sqlText = regexp.MustCompile(`(?is)^\s*(SELECT\s+\S|WITH\s+\S|INSERT\s+(INTO|OR)\b|UPDATE\s+\S|DELETE\s+FROM\b|REPLACE\s+INTO\b|CREATE\s+(TABLE|INDEX|TRIGGER)\b|ALTER\s+TABLE\b|DROP\s+(TABLE|INDEX|TRIGGER)\b|PRAGMA\s+\S|VACUUM(?:\s+\S|$)|ATTACH\s+\S|DETACH\s+\S)`)

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func text(files *token.FileSet, node ast.Node) string {
	var output bytes.Buffer
	if err := format.Node(&output, files, node); err != nil {
		panic(err)
	}
	return output.String()
}

func approvedQuery(scope string, call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	index := 0
	if strings.HasSuffix(selector.Sel.Name, "Context") {
		index = 1
	}
	if len(call.Args) <= index {
		return false
	}
	literal, ok := call.Args[index].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return false
	}
	value, err := strconv.Unquote(literal.Value)
	return err == nil && contains(maintenanceSQL[scope], value)
}

// Step keys are capability namespaces even though their rows share a table.
func stepKeyViolation(literal *ast.CompositeLit, bindings map[string]string, info *types.Info) string {
	named, ok := types.Unalias(info.TypeOf(literal)).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return ""
	}
	owner, keys := "", []string(nil)
	switch named.Obj().Pkg().Path() {
	case module + "/image/internal/dbgen":
		owner, keys = "image", []string{"image_import", "image_cleanup"}
	case module + "/machine/internal/dbgen":
		owner, keys = "machine", []string{"resource", "capture", "stop", "discard", "capture_discard", "capture_cleanup", "snapshot_delete", "direct_retirement"}
	case module + "/sandbox/internal/dbgen":
		owner, keys = "sandbox", []string{"lifecycle", "warm_claim", "fork_group", "activation", "activation:warm", "activation:golden", "checkpoint_delete", "configuration", "lease"}
	case module + "/supply/internal/dbgen":
		owner, keys = "supply", []string{"supply", "supply_cleanup"}
	case module + "/collection/internal/dbgen":
		owner, keys = "collection", []string{"gc"}
	default:
		return ""
	}
	switch named.Obj().Name() {
	case "GetStepParams", "InsertStepParams", "RecordLiveStepProgressParams", "FailLiveStepParams", "CompleteLiveStepParams", "AdvanceLiveStepParams", "CompleteUnchangedStepParams", "AdvanceRunningStepParams", "CompleteRunningStepParams", "CompleteRunningStepByInputParams", "FailUnchangedLiveStepParams", "BeginPendingStepParams", "ReplaceRunningStepBodyParams", "CompletePendingStepParams", "FinishStepParams":
	case "InsertDeletionReceiptParams", "DeleteMatchingDeletionReceiptParams", "GetDeletionReceiptParams":
		if owner == "image" {
			keys = []string{"image_cleanup"}
		}
	default:
		return ""
	}
	for _, field := range literal.Elts {
		pair, ok := field.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		name, ok := pair.Key.(*ast.Ident)
		if !ok || name.Name != "StepKey" {
			continue
		}
		value, ok := pair.Value.(*ast.BasicLit)
		if ok && value.Kind == token.STRING {
			key, err := strconv.Unquote(value.Value)
			if err == nil && contains(keys, key) {
				return ""
			}
		}
		if key := info.Types[pair.Value].Value; key != nil && key.Kind() == constant.String && contains(keys, constant.StringVal(key)) {
			return ""
		}
		if ownedStepExpression(owner, pair.Value, info) {
			return ""
		}
		return owner + " query accesses an unowned or dynamic step namespace: " + types.ExprString(pair.Value)
	}
	return owner + " query omits its owned step namespace"
}

func cleanupKeyViolation(literal *ast.CompositeLit, info *types.Info) string {
	named, ok := types.Unalias(info.TypeOf(literal)).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return ""
	}
	path := named.Obj().Pkg().Path()
	if !strings.HasPrefix(path, module+"/") || !strings.HasSuffix(path, "/internal/dbgen") {
		return ""
	}
	owner := strings.TrimSuffix(strings.TrimPrefix(path, module+"/"), "/internal/dbgen")
	name := named.Obj().Name()
	if !strings.Contains(name, "Cleanup") || !(strings.HasPrefix(name, "Insert") || strings.HasPrefix(name, "Complete") || strings.HasPrefix(name, "Transfer") || strings.HasPrefix(name, "Transition") || strings.HasPrefix(name, "Retain")) {
		return ""
	}
	prefixes := map[string][]string{
		"machine": {"blocked:", "cleanup:", "discard:", "capture:", "capture_discard:", "snapshot_delete:", "raw:", "direct_policy:"},
		"sandbox": {"checkpoint:", "sleep:", "warm_claim:"},
		"supply":  {"supply:", "supply_cleanup:"},
		"image":   {"image_import:", "image_cleanup:"},
	}
	kinds := map[string][]string{
		"machine": {"vm", "snapshot", "capture", "capture_raw", "direct_policy"},
		"sandbox": {"snapshot", "warm_claim"},
		"supply":  {"supply", "supply_cleanup"},
		"image":   {"image_import", "image_cache"},
	}
	idFound := false
	for _, field := range literal.Elts {
		pair, ok := field.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := pair.Key.(*ast.Ident)
		if !ok {
			continue
		}
		expression := pair.Value
		switch key.Name {
		case "ID":
			idFound = true
			if binary, ok := expression.(*ast.BinaryExpr); ok && binary.Op == token.ADD {
				expression = binary.X
			}
			value := info.Types[expression].Value
			if value == nil || value.Kind() != constant.String || !contains(prefixes[owner], constant.StringVal(value)) {
				return owner + " cleanup query accesses an unowned or dynamic ID namespace"
			}
		case "ResourceKind":
			value := info.Types[expression].Value
			if value != nil && value.Kind() == constant.String && contains(kinds[owner], constant.StringVal(value)) {
				continue
			}
			if field, ok := expression.(*ast.SelectorExpr); ok && owner == "image" && field.Sel.Name == "Kind" {
				if typ, ok := info.TypeOf(field.X).(*types.Named); ok && typ.Obj().Pkg() != nil && typ.Obj().Pkg().Path() == module+"/image" && typ.Obj().Name() == "imageCleanupInput" {
					continue
				}
			}
			return owner + " cleanup query accesses an unowned or dynamic resource kind"
		}
	}
	if !idFound {
		return owner + " cleanup query omits its owned ID namespace"
	}
	return ""
}

func ownedStepExpression(owner string, expression ast.Expr, info *types.Info) bool {
	if binary, ok := expression.(*ast.BinaryExpr); ok && binary.Op == token.ADD && owner == "machine" {
		value := info.Types[binary.X].Value
		return value != nil && value.Kind() == constant.String && constant.StringVal(value) == "link:"
	}
	if call, ok := expression.(*ast.CallExpr); ok && owner == "sandbox" {
		if name, ok := call.Fun.(*ast.Ident); ok {
			object := info.ObjectOf(name)
			return object != nil && object.Pkg() != nil && object.Pkg().Path() == module+"/sandbox" && object.Name() == "ActivationKey"
		}
	}
	field, ok := expression.(*ast.SelectorExpr)
	if !ok || field.Sel.Name != "Key" {
		return false
	}
	typ := info.TypeOf(field.X)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = pointer.Elem()
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != module+"/"+owner {
		return false
	}
	allowed := map[string][]string{
		"machine":    {"ResourceStep", "CaptureStep", "StopStep", "DiscardStep", "CaptureDiscardStep", "CaptureCleanupStep", "SnapshotDeleteStep", "DirectRetirementStep"},
		"sandbox":    {"ActivationStep", "LifecycleStep", "WarmClaimStep", "ForkStep"},
		"supply":     {"SupplyStep", "SupplyCleanupStep"},
		"collection": {"GCStep"},
	}
	return contains(allowed[owner], named.Obj().Name())
}

type typedSource struct {
	file  *ast.File
	files *token.FileSet
	info  *types.Info
}

type functionBody struct {
	function *ast.FuncDecl
	info     *types.Info
	scope    string
}

// Follow resolved calls, including private generated queries, from proof readers
// and terminal projectors. SQLite query_only tests enforce the runtime boundary.
func readOnlyViolations(sources map[string]typedSource) []string {
	bodies := map[*types.Func]functionBody{}
	roots := []*types.Func{}
	for _, source := range sources {
		for _, declaration := range source.file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			object, ok := source.info.Defs[function.Name].(*types.Func)
			if !ok {
				continue
			}
			bodies[object] = functionBody{function, source.info, object.FullName()}
			root := object.Pkg().Path() == module+"/internal/publication"
			if receiver := object.Signature().Recv(); receiver != nil && object.Pkg().Path() == module+"/sandbox" {
				typ := receiver.Type()
				if pointer, ok := typ.(*types.Pointer); ok {
					typ = pointer.Elem()
				}
				if named, ok := typ.(*types.Named); ok {
					root = contains([]string{"ActivationHistory", "OwnershipHistory", "StopHistory", "CaptureDiscardHistory", "DeletionHistory", "SupplyCleanupHistory"}, named.Obj().Name())
				}
			}
			if root {
				roots = append(roots, object)
			}
		}
	}
	violations := []string{}
	for _, root := range roots {
		visited := map[*types.Func]bool{}
		var visit func(*types.Func)
		visit = func(object *types.Func) {
			if visited[object] {
				return
			}
			visited[object] = true
			body, ok := bodies[object]
			if !ok {
				return
			}
			ast.Inspect(body.function.Body, func(node ast.Node) bool {
				if _, ok := node.(*ast.GoStmt); ok {
					violations = append(violations, root.FullName()+": read-only boundary starts a goroutine through "+body.scope)
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				var name *ast.Ident
				switch function := call.Fun.(type) {
				case *ast.Ident:
					name = function
				case *ast.SelectorExpr:
					name = function.Sel
				default:
					return true
				}
				callee, ok := body.info.ObjectOf(name).(*types.Func)
				if !ok {
					return true
				}
				if contains([]string{"Exec", "ExecContext", "Begin", "BeginTx", "Commit", "Checkpoint", "Start"}, callee.Name()) {
					violations = append(violations, root.FullName()+": read-only boundary reaches "+callee.FullName()+" through "+body.scope)
					return true
				}
				visit(callee)
				return true
			})
		}
		visit(root)
	}
	return violations
}

func main() {
	var compiled []compiledPackage
	if err := json.NewDecoder(os.Stdin).Decode(&compiled); err != nil {
		panic(err)
	}
	loaded, err := packages.Load(&packages.Config{
		Dir:  filepath.Join(os.Args[1], "haven"),
		Mode: packages.NeedName | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Env:  append(os.Environ(), "GOWORK="+filepath.Join(os.Args[1], "go.work")),
	}, "./...")
	if err != nil {
		panic(err)
	}
	if packages.PrintErrors(loaded) != 0 {
		os.Exit(1)
	}
	sources := map[string]typedSource{}
	for _, pkg := range loaded {
		for index, file := range pkg.Syntax {
			sources[pkg.CompiledGoFiles[index]] = typedSource{file, pkg.Fset, pkg.TypesInfo}
		}
	}
	violations := readOnlyViolations(sources)
	appFound := false
	for _, pkg := range compiled {
		if pkg.ImportPath != module && !strings.HasPrefix(pkg.ImportPath, module+"/") {
			continue
		}
		if pkg.ImportPath == module {
			violations = append(violations, "root compatibility facade must not replace internal application composition")
			continue
		}
		// Shared fixtures are fenced from every production package by the import gate.
		if pkg.ImportPath == module+"/internal/testfixture" || pkg.ImportPath == module+"/internal/testapp" {
			continue
		}
		for _, name := range append(pkg.GoFiles, pkg.CgoFiles...) {
			path := filepath.Join(pkg.Dir, name)
			relative, err := filepath.Rel(filepath.Join(os.Args[1], "haven"), path)
			if err != nil {
				panic(err)
			}
			if strings.HasPrefix(relative, "..") || strings.Contains(filepath.ToSlash(relative), "/internal/dbgen/") {
				continue
			}
			typed, ok := sources[path]
			if !ok {
				panic("missing typed source: " + path)
			}
			files, source := typed.files, typed.file
			bindings := map[string]string{}
			for _, spec := range source.Imports {
				path, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					panic(err)
				}
				name := filepath.Base(path)
				if spec.Name != nil {
					name = spec.Name.Name
				}
				bindings[name] = path
			}
			for _, declaration := range source.Decls {
				scope := filepath.ToSlash(relative) + "#<package>"
				if function, ok := declaration.(*ast.FuncDecl); ok {
					scope = filepath.ToSlash(relative) + "#" + function.Name.Name
					if pkg.ImportPath == application {
						allowed := contains([]string{"Open", "openStore", "storeValidation", "InspectState", "BackupState", "RestoreBackup", "AcknowledgeRecovery"}, function.Name.Name)
						if function.Recv != nil {
							allowed = function.Name.Name == "Close" && text(files, function.Recv.List[0].Type) == "*App"
						}
						if !allowed {
							violations = append(violations, scope+": root owns a business method instead of composition/lifecycle")
						}
					}
				}
				ast.Inspect(declaration, func(node ast.Node) bool {
					switch value := node.(type) {
					case *ast.CompositeLit:
						if reason := cleanupKeyViolation(value, typed.info); reason != "" {
							violations = append(violations, scope+": "+reason)
						}
						if reason := stepKeyViolation(value, bindings, typed.info); reason != "" {
							violations = append(violations, scope+": "+reason)
						}
					case *ast.TypeSpec:
						if pkg.ImportPath == module+"/image" && value.Name.Name == "Repository" {
							violations = append(violations, scope+": image repository facade was restored")
						}
						if pkg.ImportPath == module+"/supply" && contains([]string{"TemplateState", "PoolState"}, value.Name.Name) {
							violations = append(violations, scope+": supply row facade was restored")
						}
						if (pkg.ImportPath == module+"/machine" && contains([]string{"VMState", "SnapshotState"}, value.Name.Name)) || (pkg.ImportPath == module+"/sandbox" && value.Name.Name == "Repository") {
							violations = append(violations, scope+": retired persistence facade was restored")
						}
						if pkg.ImportPath != application {
							break
						}
						if value.Assign.IsValid() || value.Name.Name == "Service" {
							violations = append(violations, relative+": root compatibility alias/facade: "+value.Name.Name)
						}
						if value.Name.Name == "App" {
							structure, ok := value.Type.(*ast.StructType)
							if !ok {
								violations = append(violations, relative+": App must be an explicit composition struct")
								break
							}
							appFound = true
							for _, field := range structure.Fields.List {
								if len(field.Names) == 0 {
									violations = append(violations, relative+": App embeds a compatibility method set")
								}
							}
						}
					case *ast.BasicLit:
						if value.Kind != token.STRING {
							break
						}
						literal, err := strconv.Unquote(value.Value)
						if err == nil && sqlText.MatchString(literal) && !contains(maintenanceSQL[scope], literal) {
							violations = append(violations, scope+": handwritten SQL outside the reviewed maintenance statements")
						}
					case *ast.CallExpr:
						selector, ok := value.Fun.(*ast.SelectorExpr)
						if !ok {
							break
						}
						method := selector.Sel.Name
						if !contains([]string{"Query", "QueryContext", "QueryRow", "QueryRowContext", "Exec", "ExecContext", "Scan", "Prepare", "PrepareContext"}, method) {
							break
						}
						if method == "Prepare" && len(value.Args) != 1 {
							break
						} // Loom Prepare takes context and resource.
						if method == "Query" && len(value.Args) == 0 {
							break
						} // net/url.URL.Query has no SQL argument.
						if scope == "supply/build.go#runBuild" && text(files, value.Fun) == "service.opts.Guest.Exec" {
							break
						}
						if contains(commandCalls[scope], text(files, value)) || contains(maintenanceCalls[scope], text(files, value)) || approvedQuery(scope, value) {
							break
						}
						if method == "Scan" {
							if query, ok := selector.X.(*ast.CallExpr); ok && approvedQuery(scope, query) {
								break
							}
						}
						violations = append(violations, scope+": raw SQL method outside generated queries or reviewed maintenance: "+method)
					}
					return true
				})
			}
		}
	}
	if !appFound {
		violations = append(violations, "Haven App composition root missing")
	}
	if err := json.NewEncoder(os.Stdout).Encode(violations); err != nil {
		panic(err)
	}
}
