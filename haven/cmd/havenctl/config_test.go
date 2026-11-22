package main

import (
	"reflect"
	"testing"

	"github.com/spf13/pflag"
)

func TestFlagsMatchSettings(t *testing.T) {
	for _, tt := range []struct {
		flags *pflag.FlagSet
		typ   reflect.Type
	}{
		{newFlagSet(), reflect.TypeFor[settings]()},
		{adminFlagSet(), reflect.TypeFor[adminSettings]()},
	} {
		t.Run(tt.flags.Name(), func(t *testing.T) {
			remaining := map[string]bool{}
			tt.flags.VisitAll(func(flag *pflag.Flag) {
				remaining[flag.Name] = true
			})
			for field := range tt.typ.Fields() {
				tag := field.Tag.Get("mapstructure")
				if !remaining[tag] {
					t.Errorf("settings field %q has no registered flag", tag)
				}
				delete(remaining, tag)
			}
			for name := range remaining {
				t.Errorf("flag %q has no settings field", name)
			}
		})
	}
}
