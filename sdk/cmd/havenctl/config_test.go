package main

import (
	"reflect"
	"testing"

	"github.com/spf13/pflag"
)

func TestFlagsMatchSettings(t *testing.T) {
	remaining := map[string]bool{}
	newFlagSet().VisitAll(func(flag *pflag.Flag) {
		remaining[flag.Name] = true
	})
	typ := reflect.TypeOf(settings{})
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("mapstructure")
		if !remaining[tag] {
			t.Errorf("settings field %q has no registered flag", tag)
		}
		delete(remaining, tag)
	}
	for name := range remaining {
		t.Errorf("flag %q has no settings field", name)
	}
}
