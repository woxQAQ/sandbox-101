package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/localuds"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestManagementWaitForFailedImport(t *testing.T) {
	app, _ := testapp.ColdService(t, nil)
	id, options := testfixture.SeedPendingImageImport(t, app.State)
	input := types.ImageImport(options)
	body := operation.Encode(types.ManagementRequest{
		Action: types.ManagementActionImport,
		Key:    "original",
		Import: &input,
	})
	peer := adminPeer(t)
	services := api.ManagementServices{Images: app.Images, Operations: app.Operations}
	post := func(ctx context.Context, wantStatus int, wantCode, wantMessage string) {
		t.Helper()
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/manage", bytes.NewReader(body))
		recorder := httptest.NewRecorder()
		api.NewManagement(services).ServeHTTP(recorder, req)
		var failure types.ApiError
		if err := json.Unmarshal(recorder.Body.Bytes(), &failure); err != nil {
			t.Fatal(err)
		}
		if recorder.Code != wantStatus || failure.Code != wantCode || failure.Message != wantMessage {
			t.Fatalf("got %d %+v, want %d %s / %s", recorder.Code, failure, wantStatus, wantCode, wantMessage)
		}
	}
	post(peer, 500, types.CodeInternal, "internal service error")

	// The durable import has failed. Interrupt the next wait after it has read
	// that row, while the worker reader still reports custody.
	ctx, cancel := context.WithCancel(peer)
	defer cancel()
	services.Operations = operation.NewMonitor(app.State.Database(), cancelDuringCustody{cancel}, app.State.Identity().Identity)
	post(ctx, 503, types.CodeUnknown, "operation wait ended")

	recorder := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(peer, http.MethodGet, "/v1/operations/"+id, nil)
	api.NewManagement(services).ServeHTTP(recorder, req)
	var result types.ManagementOperation
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || result.ID != id || result.Status != "failed" || result.Key != "original" {
		t.Fatalf("unexpected operation after interrupted wait: %d %+v", recorder.Code, result)
	}
}

type cancelDuringCustody struct {
	cancel context.CancelFunc
}

func (c cancelDuringCustody) State(string) execution.WorkerState {
	c.cancel()
	return execution.WorkerState{Active: true}
}

func adminPeer(t *testing.T) context.Context {
	t.Helper()
	listener, err := localuds.Open(filepath.Join(t.TempDir(), "manage.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return localuds.PeerContext(t.Context(), conn)
}
