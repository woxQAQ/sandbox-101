package sandbox

import (
	"fmt"
	"strings"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
)

func SplitEnv(env map[string]operation.EnvVar) (map[string]string, map[string]egress.Secret, map[string]authorization.Injection, error) {
	visible := map[string]string{}
	secrets := map[string]egress.Secret{}
	descriptors := map[string]authorization.Injection{}
	for key, value := range env {
		if key == "" || len(key) > 256 || strings.ContainsAny(key, "=\x00\n") || strings.ContainsRune(value.Value, 0) {
			return nil, nil, nil, fmt.Errorf("%w: invalid environment variable", errs.ErrInvalid)
		}
		if value.IsSecret() {
			if value.Inject == nil || len(value.Inject.Hosts) == 0 || value.Inject.Header == "" || value.Value == "" {
				return nil, nil, nil, fmt.Errorf("%w: host credential requires authorized injection", errs.ErrInvalid)
			}
			descriptors[key] = *value.Inject
			secrets[key] = egress.Secret{
				Value:       value.Value,
				Hosts:       append([]string(nil), value.Inject.Hosts...),
				Header:      value.Inject.Header,
				Placeholder: value.Inject.Placeholder,
			}
			if value.Inject.Placeholder != "" {
				visible[key] = value.Inject.Placeholder
			}
		} else {
			visible[key] = value.Value
		}
	}
	return visible, secrets, descriptors, nil
}
