package sandboxd

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOperationJSONDoesNotExposeEnv(t *testing.T) {
	b, _ := json.Marshal(Sandbox{Env: map[string]string{"PASSWORD": "secret"}})
	if strings.Contains(string(b), "PASSWORD") ||
		strings.Contains(string(b), `"secret"`) {
		t.Fatal("sandbox API exposes env")
	}
}
