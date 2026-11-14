//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/woxQAQ/haven/sdk"
	"github.com/woxQAQ/haven/types"
)

func controlHTTP(t *testing.T, endpoint, method, path, token, key string, body []byte, async bool, status int) []byte {
	t.Helper()
	request, err := http.NewRequest(method, endpoint+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	if async {
		request.Header.Set("Prefer", "respond-async")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("%s %s: HTTP %d, wanted %d: %s", method, path, response.StatusCode, status, payload)
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("control response may be cached")
	}
	return payload
}

func TestHavenHTTPContractAndIdempotency(t *testing.T) {
	_, scoped, _, firstID := transportFixture(t)
	endpoint := scoped.Endpoint
	for _, path := range []string{
		"/v1/sandboxes",
		"/v1/unknown",
	} {
		controlHTTP(t, endpoint, "GET", path, "", "", nil, false, 404)
	}
	controlHTTP(t, endpoint, "PUT", "/v1/sandboxes", "transport-token", "", nil, false, 405)
	controlHTTP(t, endpoint, "GET", "/v1/sandboxes/"+firstID+"/connect", scoped.Token, "", nil, false, 426)
	controlHTTP(t, endpoint, "GET", "/metrics", "transport-token", "", nil, false, 403)
	controlHTTP(t, endpoint, "GET", "/v1/sandboxes", scoped.Token, "", nil, false, 409)
	controlHTTP(t, endpoint, "POST", "/v1/sandboxes/"+firstID+"/checkpoint", scoped.Token, "scoped-forbidden", []byte(`{}`), false, 404)
	command := []byte(`{"template_id":"fixture","policy":{"mode":"none"}}`)
	controlHTTP(t, endpoint, "POST", "/v1/claim", "transport-token", "", command, false, 400)
	controlHTTP(t, endpoint, "POST", "/v1/claim", "transport-token", "trailing", []byte(`{} {}`), false, 400)
	controlHTTP(t, endpoint, "POST", "/v1/claim", "transport-token", "unknown-field", []byte(`{"observed_generation":"private"}`), false, 400)
	payload := controlHTTP(t, endpoint, "POST", "/v1/claim", "transport-token", "http-claim", command, true, 202)
	var operation types.Operation
	if err := json.Unmarshal(payload, &operation); err != nil {
		t.Fatal(err)
	}
	if operation.ID == "" || operation.Command.Action != "claim" || operation.CreatedAt.IsZero() {
		t.Fatal("async response lost the public operation schema", operation)
	}
	tenant := sdk.New(endpoint, "transport-token")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for operation.Status != "succeeded" {
		select {
		case <-ctx.Done():
			t.Fatal("async operation did not complete")
		default:
		}
		var err error
		operation, err = tenant.Operation(ctx, operation.ID)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if operation.Result.Sandbox == nil || operation.Result.Sandbox.TTLSeconds != 300 || operation.FinishedAt.IsZero() {
		t.Fatal(operation)
	}
	replay, err := tenant.Claim(ctx, "http-claim", types.Command{
		TemplateID: "fixture",
		Policy: types.Policy{
			Mode: "none",
		},
	})
	if err != nil || replay.OperationID != operation.ID {
		t.Fatal(replay, err)
	}
	expected, _ := json.Marshal(operation.Result)
	actual, _ := json.Marshal(replay)
	if !bytes.Equal(expected, actual) {
		t.Fatal("retry changed the successful public result")
	}
	controlHTTP(t, endpoint, "POST", "/v1/claim", "transport-token", "http-claim", []byte(`{"template_id":"fixture","ttl_seconds":301,"policy":{"mode":"none"}}`), false, 409)
	controlHTTP(t, endpoint, "GET", "/v1/operations/"+operation.ID, operation.Result.Tokens[operation.Result.Sandbox.ID], "", nil, false, 404)
	root := sdk.New(endpoint, "integration-root-token")
	if value, err := root.Operation(ctx, operation.ID); err != nil || value.ID != operation.ID {
		t.Fatal("administrator cannot read a tenant operation", value, err)
	}
	foreign, err := root.PutTenant(ctx, types.Tenant{
		ID:            "foreign",
		MaxSandboxes:  1,
		MaxStorageMiB: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	controlHTTP(t, endpoint, "GET", "/v1/sandboxes/"+operation.Result.Sandbox.ID, foreign.Token, "", nil, false, 404)
	controlHTTP(t, endpoint, "GET", "/v1/sandboxes/missing", foreign.Token, "", nil, false, 404)
	scopedClient := sdk.New(endpoint, operation.Result.Tokens[operation.Result.Sandbox.ID])
	released, err := scopedClient.Change(ctx, operation.Result.Sandbox.ID, "self-release", types.Command{
		Action: "release",
	})
	if err != nil || released.Sandbox == nil || released.Sandbox.State != "terminated" || released.Tokens != nil {
		t.Fatal(released, err)
	}
	if terminal, err := scopedClient.Operation(ctx, released.OperationID); err != nil || terminal.Status != "succeeded" {
		t.Fatal("retired capability lost its own release result", terminal, err)
	}
	controlHTTP(t, endpoint, "GET", "/v1/sandboxes/"+operation.Result.Sandbox.ID, scopedClient.Token, "", nil, false, 404)
}
