# Haven calltree

From the repository root, `go tool calltree` writes one JSON document to stdout. It type-checks the workspace packages imported by the production Haven daemon with the current Go build settings. The report includes source once per file so a reviewer can look up definitions, read callers, and compare sibling names using the same snapshot.

Open `tools/calltree/viewer.html` in a browser and select a report with **打开 JSON**. To package the current snapshot and the viewer in one offline HTML file, run `go tool calltree -html` and save stdout as HTML. The initial diagram shows workspace packages and directed cross-package calls. Select a package to highlight its incoming and outgoing edges. Pick a route or function from the index to open a symbol-level graph. Select a function node to expand its callers or callees or inspect its source. Drag the canvas to pan and scroll to zoom. **导出 SVG** saves the current graph as a standalone vector image.

| Field | How to read it |
| --- | --- |
| `routes` | Select by `surface`, `method` and `path`. Each entry lists its middleware followed by the request handler. Factory closures include their captured route arguments in `bindings`. `NoRoute` and `NoMethod` identify fallbacks. |
| `functions` | Keys identify Go functions, methods, closures and SSA-generated wrappers. `callers` gives the immediate upstream functions. `calls` gives individual downstream call sites with candidate `targets`. Use a visited set when expanding recursive calls. |
| `files` | Keys are repository-relative paths. Each file contains its package, source text, typed definitions by `line:column`, and references from identifier uses to definition positions. Positions use Go's one-based line and byte column. |
| `main` | Function ID for following startup and background work from the daemon entry point. |

Read `dispatch` before interpreting a call. `direct` identifies a statically determined callee. `interface` and `function_value` list Variable Type Analysis candidates across the production workspace packages. An empty candidate list for either means the target is unresolved. `builtin` identifies a Go built-in. `mode` records `go` or `defer` where applicable. `function_args` records function values visibly passed at a call site. Invoking such a callback can happen later.

A target outside `functions` is a boundary. Standard-library and third-party bodies, reflective calls and guest execution are outside the analysis. HTTP entries capture route constants; shared functions retain all their branches. A function can have multiple callers, appear under several routes or have no known HTTP path. The index includes declarations from the loaded packages regardless of reachability.
