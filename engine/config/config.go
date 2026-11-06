// Package config loads command configuration from local flags, environment and
// an explicitly selected Viper configuration file.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Load decodes flags into a typed struct whose mapstructure tags match the flag
// names. The flag set must include a string "config" flag, and the struct must
// include its corresponding field. No configuration file is discovered implicitly.
// Explicit flags take precedence over prefixed environment variables, the file,
// and flag defaults, in that order.
func Load(flags *pflag.FlagSet, prefix string, out any) error {
	v := viper.New()
	v.SetEnvPrefix(prefix)
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AllowEmptyEnv(true)
	v.AutomaticEnv()
	if err := v.BindPFlags(flags); err != nil {
		return err
	}
	if path := v.GetString("config"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read config: %w", err)
		}
		format := strings.TrimPrefix(filepath.Ext(path), ".")
		decoder, err := viper.NewCodecRegistry().Decoder(format)
		if err != nil {
			return fmt.Errorf("read config %q: %w", path, err)
		}
		values := make(map[string]any)
		if err := decoder.Decode(raw, values); err != nil {
			return fmt.Errorf("read config %q: %w", path, err)
		}
		// Check the flat file keys before merging: Viper's AllSettings omits
		// null values and empty maps, so UnmarshalExact alone cannot check them.
		for key, value := range values {
			if flags.Lookup(strings.ToLower(key)) == nil {
				return fmt.Errorf("config %q: unknown key %q", path, key)
			}
			if value == nil {
				return fmt.Errorf(
					"config %q: key %q must not be null",
					path,
					key,
				)
			}
		}
		if err := v.MergeConfigMap(values); err != nil {
			return fmt.Errorf("read config %q: %w", path, err)
		}
	}
	if err := v.UnmarshalExact(out, func(c *mapstructure.DecoderConfig) {
		c.WeaklyTypedInput = false
		c.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			exactNumber,
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToBasicTypeHookFunc(),
		)
	}); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	return nil
}

// Mapstructure otherwise truncates fractional numbers and permits integer
// overflow even with WeaklyTypedInput disabled. Parse numeric inputs at the
// destination width so JSON/YAML numbers receive the same checks as env strings.
func exactNumber(from, to reflect.Type, data any) (any, error) {
	if to.Kind() < reflect.Int || to.Kind() > reflect.Uint64 {
		return data, nil
	}
	value := reflect.ValueOf(data)
	var text string
	switch from.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		text = strconv.FormatInt(value.Int(), 10)
	case reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:
		text = strconv.FormatUint(value.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		text = strconv.FormatFloat(value.Float(), 'f', -1, from.Bits())
	default:
		return data, nil
	}
	if to.Kind() <= reflect.Int64 {
		return strconv.ParseInt(text, 10, to.Bits())
	}
	return strconv.ParseUint(text, 10, to.Bits())
}
