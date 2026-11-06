package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

type handlerClient struct {
	api.Client

	calls int
	id    string
	err   error
}

func (f *handlerClient) Submit(
	_ context.Context,
	req api.Request,
) (api.Operation, error) {
	f.calls++
	return api.Operation{ID: "op-1", Key: req.Key}, f.err
}

func (f *handlerClient) FindOperation(
	_ context.Context,
	key string,
) (api.Operation, error) {
	f.id = key
	return api.Operation{Key: key}, f.err
}

func (f *handlerClient) GetOperation(
	_ context.Context,
	id string,
) (api.Operation, error) {
	f.id = id
	return api.Operation{ID: id}, f.err
}

func (f *handlerClient) ListVMs(context.Context) ([]api.VM, error) {
	return []api.VM{}, f.err
}

func (f *handlerClient) GetVM(_ context.Context, id string) (api.VM, error) {
	f.id = id
	return api.VM{ID: id}, f.err
}

func (f *handlerClient) ListSnapshots(context.Context) ([]api.Snapshot, error) {
	return []api.Snapshot{}, f.err
}

func (f *handlerClient) GetSnapshot(
	_ context.Context,
	id string,
) (api.Snapshot, error) {
	f.id = id
	return api.Snapshot{ID: id}, f.err
}

func TestRoutes(t *testing.T) {
	for _, tc := range []struct {
		method, path, body, id string
		status                 int
	}{
		{http.MethodPost, "/v1/operations", `{"key":"create-1","action":"create"}`, "", 200},
		{http.MethodGet, "/v1/operations?key=key%20with%20%2F%3F%20%E4%B8%96%E7%95%8C", "", "key with /? 世界", 200},
		{http.MethodGet, "/v1/operations/op-1", "", "op-1", 200},
		{http.MethodGet, "/v1/vms", "", "", 200},
		{http.MethodGet, "/v1/vms/vm-1", "", "vm-1", 200},
		{http.MethodGet, "/v1/snapshots", "", "", 200},
		{http.MethodGet, "/v1/snapshots/snapshot-1", "", "snapshot-1", 200},
		{http.MethodGet, "/operations", "", "", 404},
		{http.MethodGet, "/v1/vms/vm-1/unexpected", "", "", 404},
		{http.MethodDelete, "/v1/vms/vm-1", "", "", 404},
		{http.MethodPost, "/v1/operations/", `{}`, "", 404},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			client := &handlerClient{}
			w := httptest.NewRecorder()
			Handler(
				client,
			).ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
			if w.Code != tc.status || client.id != tc.id {
				t.Fatalf(
					"status=%d id=%q body=%s",
					w.Code,
					client.id,
					w.Body.String(),
				)
			}
			if w.Header().Get("Content-Type") != "application/json" ||
				!json.Valid(w.Body.Bytes()) {
				t.Fatalf(
					"non-JSON response: %v %s",
					w.Header(),
					w.Body.String(),
				)
			}
		})
	}
}

func TestInvalidRequestNeverSubmitted(t *testing.T) {
	for _, body := range []string{
		`{"key":"intent","unexpected":true}`,
		`{"key":"intent"} {"key":"other"}`,
		`{"key":`,
		`{"key":"` + strings.Repeat("x", 1<<20) + `"}`,
	} {
		client := &handlerClient{}
		w := httptest.NewRecorder()
		Handler(
			client,
		).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/operations", strings.NewReader(body)))
		if w.Code != http.StatusBadRequest || client.calls != 0 {
			t.Fatalf(
				"status=%d submissions=%d body=%s",
				w.Code,
				client.calls,
				w.Body.String(),
			)
		}
	}
}

func TestTypedErrorStatuses(t *testing.T) {
	for _, tc := range []struct {
		code   api.Code
		status int
	}{
		{api.NotFound, 404},
		{api.Invalid, 400},
		{api.Conflict, 409},
		{api.Exhausted, 507},
		{api.Incompatible, 422},
		{api.ManualRecovery, 503},
		{api.Internal, 500},
	} {
		t.Run(string(tc.code), func(t *testing.T) {
			client := &handlerClient{err: api.Fail(tc.code, "failure")}
			w := httptest.NewRecorder()
			Handler(
				client,
			).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/vms", nil))
			var result api.Error
			if err := json.Unmarshal(
				w.Body.Bytes(),
				&result,
			); err != nil || w.Code != tc.status ||
				result.Code != tc.code {
				t.Fatalf("status=%d result=%+v decode=%v", w.Code, result, err)
			}
		})
	}
}
