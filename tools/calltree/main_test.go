package main

import (
	"path/filepath"
	"slices"
	"testing"
)

func TestHavenRouteReport(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOWORK", filepath.Join(root, "go.work"))
	r, err := analyze(root)
	if err != nil {
		t.Fatal(err)
	}
	routes := map[[3]string]route{}
	for _, entry := range r.Routes {
		key := [3]string{entry.Surface, entry.Method, entry.Path}
		if _, exists := routes[key]; exists {
			t.Fatalf("duplicate route %v", key)
		}
		routes[key] = entry
		for _, h := range entry.Handlers {
			if fn, ok := r.Functions[h.Function]; !ok || fn.At == "" {
				t.Fatalf("missing handler definition for %v: %s", key, h.Function)
			}
		}
	}
	business := routes[[3]string{"business", "GET", "/v1/status"}]
	management := routes[[3]string{"management", "GET", "/v1/status"}]
	if len(business.Handlers) != 3 || len(management.Handlers) != 3 || business.Handlers[1].Function == management.Handlers[1].Function {
		t.Fatal("expected distinct authentication chains for the two status routes")
	}
	pause := routes[[3]string{"business", "POST", "/v1/sandboxes/:id/pause"}]
	if len(pause.Handlers) != 3 || pause.Handlers[2].Bindings["action"] != "pause" {
		t.Fatalf("lost the route action: %+v", pause)
	}
	closure := pause.Handlers[2].Function
	submit := "(*" + api + ".Services).submit"
	admission := "(*github.com/woxQAQ/haven/sandbox.CommandAdmission).Submit"
	if !slices.Contains(r.Functions[submit].Callers, closure) || !slices.Contains(r.Functions[admission].Callers, submit) {
		t.Fatal("missing call chain from the pause closure through the HTTP adapter to command admission")
	}
	for caller, fn := range r.Functions {
		for _, call := range fn.Calls {
			for _, target := range call.Targets {
				if local, ok := r.Functions[target]; ok && !slices.Contains(local.Callers, caller) {
					t.Fatalf("missing caller %s for %s", caller, target)
				}
			}
		}
	}
}
