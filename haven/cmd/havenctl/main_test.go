package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/types"
)

func TestForkCheckpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/checkpoints/source/fork" || r.Header.Get("Idempotency-Key") != "fork-key" || r.Header.Get("X-Tenant-ID") != "tenant" {
			t.Errorf("unexpected checkpoint request %s %s", r.Method, r.URL)
		}
		var command types.Command
		if err := json.NewDecoder(r.Body).Decode(&command); err != nil || command.Count != 2 {
			t.Errorf("unexpected fork intent %+v, %v", command, err)
		}
		_ = json.NewEncoder(w).Encode(types.Result{OperationID: "accepted"})
	}))
	defer server.Close()
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	intent := filepath.Join(dir, "intent.json")
	if err := os.WriteFile(token, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(intent, []byte(`{"count":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"--endpoint", server.URL, "--token-file", token, "--tenant", "tenant", "--key", "fork-key", "--request-file", intent, "fork-checkpoint", "source"}); err != nil {
		t.Fatal(err)
	}
}
