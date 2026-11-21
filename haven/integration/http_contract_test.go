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
	methodFailure := controlHTTP(t, endpoint, contractRequest{
		Method: "PUT",
		Path:   "/v1/sandboxes",
		Token:  "transport-token",
	}, 405)
	requireFailure(t, methodFailure, types.CodeInvalid, "method not allowed")
	upgradeFailure := controlHTTP(t, endpoint, contractRequest{
		Method: "GET",
		Path:   "/v1/sandboxes/" + firstID + "/connect",
		Token:  scoped.Token,
	}, 426)
	requireFailure(t, upgradeFailure, types.CodeInvalid, "Connection: Upgrade and Upgrade: sandbox-v1 required")
	controlHTTP(t, endpoint, contractRequest{
		Method: "GET",
		Path:   "/metrics",
		Token:  "transport-token",
	}, 404)
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
	missingKey := controlHTTP(t, endpoint, contractRequest{
		Method: "POST",
		Path:   "/v1/sandboxes",
		Token:  "transport-token",
		Body:   command,
	}, 400)
	requireFailure(t, missingKey, types.CodeInvalid, "Idempotency-Key required (max 256 bytes)")
	missingCheckpoint := controlHTTP(t, endpoint, contractRequest{
		Method: "DELETE",
		Path:   "/v1/checkpoints/missing",
		Token:  "transport-token",
		Key:    "missing-checkpoint",
	}, 404)
	requireFailure(t, missingCheckpoint, types.CodeNotFound, "not found")
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
	oldDeadline := acceptedRecord.Result.Sandbox.Deadline
	renewed, err := tenant.Change(ctx, acceptedRecord.Result.Sandbox.ID, "renew-before-release", types.Command{Action: "renew", TTLSeconds: 120})
	if err != nil || renewed.Sandbox == nil || renewed.Sandbox.Deadline.Equal(oldDeadline) {
		t.Fatal("lease did not change", renewed, err)
	}
	_, err = tenant.Change(ctx, acceptedRecord.Result.Sandbox.ID, "stale-release", types.Command{Action: "release", LeaseDeadline: oldDeadline})
	if err == nil || err.Error() != "conflict: operation conflicts with current state" {
		t.Fatal("release with the previous deadline must conflict", err)
	}
	current, err := tenant.Get(ctx, acceptedRecord.Result.Sandbox.ID)
	if err != nil || current.State != "running" || !current.Deadline.Equal(renewed.Sandbox.Deadline) {
		t.Fatal("stale release changed renewed sandbox", current, err)
	}
	// A bodyless release and its SDK replay describe the same intent. Adding a
	// deadline to the replay must change the command identity.
	message := contractRequest{
		Method: http.MethodDelete,
		Path:   "/v1/sandboxes/" + firstID,
		Token:  tenant.Token,
		Key:    "bodyless-release",
	}
	controlHTTP(t, endpoint, message, 200)
	if replay, err := tenant.Change(ctx, firstID, "bodyless-release", types.Command{Action: "release"}); err != nil || replay.Sandbox == nil || replay.Sandbox.State != "terminated" {
		t.Fatal("bodyless release replay", replay, err)
	}
	message.Body = []byte(`{"lease_deadline":"2020-01-01T00:00:00Z"}`)
	controlHTTP(t, endpoint, message, 409)
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

func requireFailure(t *testing.T, payload []byte, code, message string) {
	t.Helper()
	var failure types.ApiError
	if err := json.Unmarshal(payload, &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Code != code || failure.Message != message {
		t.Fatalf("got %s, want %s / %s", payload, code, message)
	}
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
