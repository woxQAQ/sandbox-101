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

func TestHavenHTTPContractAndIdempotency(t *testing.T) {
	_, scoped, _, firstID := transportFixture(t)
	endpoint := scoped.Endpoint
	for _, path := range []string{
		"/v1/sandboxes",
		"/v1/unknown",
	} {
		controlHTTP(t, endpoint, contractRequest{
			Method: "GET",
			Path:   path,
		}, 404)
	}
	controlHTTP(t, endpoint, contractRequest{
		Method: "PUT",
		Path:   "/v1/sandboxes",
		Token:  "transport-token",
	}, 405)
	controlHTTP(t, endpoint, contractRequest{
		Method: "GET",
		Path:   "/v1/sandboxes/" + firstID + "/connect",
		Token:  scoped.Token,
	}, 426)
	controlHTTP(t, endpoint, contractRequest{
		Method: "GET",
		Path:   "/metrics",
		Token:  "transport-token",
	}, 403)
	controlHTTP(t, endpoint, contractRequest{
		Method: "GET",
		Path:   "/v1/sandboxes",
		Token:  scoped.Token,
	}, 409)
	controlHTTP(t, endpoint, contractRequest{
		Method: "POST",
		Path:   "/v1/sandboxes/" + firstID + "/checkpoint",
		Token:  scoped.Token,
		Key:    "scoped-forbidden",
		Body:   []byte(`{}`),
	}, 404)
	command := []byte(`{"template_id":"fixture","policy":{"mode":"none"}}`)
	controlHTTP(t, endpoint, contractRequest{
		Method: "POST",
		Path:   "/v1/sandboxes",
		Token:  "transport-token",
		Body:   command,
	}, 400)
	controlHTTP(t, endpoint, contractRequest{
		Method: "POST",
		Path:   "/v1/sandboxes",
		Token:  "transport-token",
		Key:    "trailing",
		Body:   []byte(`{} {}`),
	}, 400)
	controlHTTP(t, endpoint, contractRequest{
		Method: "POST",
		Path:   "/v1/sandboxes",
		Token:  "transport-token",
		Key:    "unknown-field",
		Body:   []byte(`{"observed_generation":"private"}`),
	}, 400)
	payload := controlHTTP(t, endpoint, contractRequest{
		Method: "POST",
		Path:   "/v1/sandboxes",
		Token:  "transport-token",
		Key:    "http-claim",
		Body:   command,
		Prefer: "respond-async",
	}, 202)
	var acceptedRecord types.Operation
	if err := json.Unmarshal(payload, &acceptedRecord); err != nil {
		t.Fatal(err)
	}
	if acceptedRecord.ID == "" || acceptedRecord.Command.Action != "claim" || acceptedRecord.CreatedAt.IsZero() {
		t.Fatal("async response lost the public operation schema", acceptedRecord)
	}
	tenant := sdk.New(endpoint, "transport-token")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for acceptedRecord.Status != "succeeded" {
		select {
		case <-ctx.Done():
			t.Fatal("async operation did not complete")
		default:
		}
		var err error
		acceptedRecord, err = tenant.Operation(ctx, acceptedRecord.ID)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if acceptedRecord.Result.Sandbox == nil || acceptedRecord.Result.Sandbox.TTLSeconds != 300 || acceptedRecord.FinishedAt.IsZero() {
		t.Fatal(acceptedRecord)
	}
	replay, err := tenant.Claim(ctx, "http-claim", types.Command{
		TemplateID: "fixture",
		Policy: types.Policy{
			Mode: "none",
		},
	})
	if err != nil || replay.OperationID != acceptedRecord.ID {
		t.Fatal(replay, err)
	}
	expected, _ := json.Marshal(acceptedRecord.Result)
	actual, _ := json.Marshal(replay)
	if !bytes.Equal(expected, actual) {
		t.Fatal("retry changed the successful public result")
	}
	controlHTTP(t, endpoint, contractRequest{
		Method: "POST",
		Path:   "/v1/sandboxes",
		Token:  "transport-token",
		Key:    "http-claim",
		Body:   []byte(`{"template_id":"fixture","ttl_seconds":301,"policy":{"mode":"none"}}`),
	}, 409)
	controlHTTP(t, endpoint, contractRequest{
		Method: "GET",
		Path:   "/v1/operations/" + acceptedRecord.ID,
		Token:  acceptedRecord.Result.Tokens[acceptedRecord.Result.Sandbox.ID],
	}, 404)
	root := sdk.New(endpoint, "integration-root-token")
	if value, err := root.Operation(ctx, acceptedRecord.ID); err != nil || value.ID != acceptedRecord.ID {
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
	controlHTTP(t, endpoint, contractRequest{
		Method: "GET",
		Path:   "/v1/sandboxes/" + acceptedRecord.Result.Sandbox.ID,
		Token:  foreign.Token,
	}, 404)
	controlHTTP(t, endpoint, contractRequest{
		Method: "GET",
		Path:   "/v1/sandboxes/missing",
		Token:  foreign.Token,
	}, 404)
	scopedClient := sdk.New(endpoint, acceptedRecord.Result.Tokens[acceptedRecord.Result.Sandbox.ID])
	released, err := scopedClient.Change(ctx, acceptedRecord.Result.Sandbox.ID, "self-release", types.Command{
		Action: "release",
	})
	if err != nil || released.Sandbox == nil || released.Sandbox.State != "terminated" || released.Tokens != nil {
		t.Fatal(released, err)
	}
	if terminal, err := scopedClient.Operation(ctx, released.OperationID); err != nil || terminal.Status != "succeeded" {
		t.Fatal("retired capability lost its own release result", terminal, err)
	}
	controlHTTP(t, endpoint, contractRequest{
		Method: "GET",
		Path:   "/v1/sandboxes/" + acceptedRecord.Result.Sandbox.ID,
		Token:  scopedClient.Token,
	}, 404)
}

type contractRequest struct {
	Method string
	Path   string
	Token  string
	Key    string
	Body   []byte
	Prefer string
}

func controlHTTP(t *testing.T, endpoint string, message contractRequest, status int) []byte {
	t.Helper()
	method, path, token, key, body := message.Method, message.Path, message.Token, message.Key, message.Body
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
	if message.Prefer != "" {
		request.Header.Set("Prefer", message.Prefer)
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
