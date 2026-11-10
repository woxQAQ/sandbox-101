package sandboxd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

type observationErrorEngine struct {
	api.Client

	err error
}

func (e *observationErrorEngine) GetVM(
	context.Context,
	string,
) (api.VM, error) {
	return api.VM{}, e.err
}

func TestRecoveryRequiresDirectEngineAbsence(t *testing.T) {
	for _, name := range []string{"local", "translated", "joined", "absent"} {
		t.Run(name, func(t *testing.T) {
			s, engine, _, p := fixture(t)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			switch name {
			case "local":
				s.opts.Engine = &observationErrorEngine{
					Client: engine, err: Fail(ErrNotFound, "not found"),
				}
			case "translated":
				s.opts.Engine = &observationErrorEngine{
					Client: engine,
					err: fmt.Errorf(
						"adapter: %w",
						Wrap(ErrInternal, "dependency failed",
							api.Fail(api.NotFound, "private VM observation")),
					),
				}
			case "joined":
				s.opts.Engine = &observationErrorEngine{
					Client: engine,
					err: errors.Join(
						api.Fail(api.NotFound, "absent"),
						errors.New("observation incomplete"),
					),
				}
			case "absent":
				engine.mu.Lock()
				delete(engine.vms, b.VMID)
				engine.mu.Unlock()
			}
			_, err := s.Execute(
				s.ctx,
				p,
				"release",
				Command{Action: "release", SandboxID: b.ID},
			)
			if name == "absent" {
				if err != nil {
					t.Fatal(err)
				}
				ledgerMoney(t, s, p.TenantID, 0, 0)
			} else {
				if err == nil {
					t.Fatal("non-authoritative absence was accepted")
				}
				ledgerMoney(t, s, p.TenantID, 1, 64)
				if engine.count("delete") != 0 {
					t.Fatal("failed observation authorized physical deletion")
				}
			}
		})
	}
}

func TestHTTPDistinguishesAuthorizationAndMissingEngineVM(t *testing.T) {
	s, engine, _, p := fixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	s.opts.Engine = &observationErrorEngine{
		Client: engine,
		err:    api.Fail(api.NotFound, "/private/engine/diagnostic"),
	}
	handler := s.Handler()
	for _, tc := range []struct {
		method, path, token string
		status              int
		code                ErrorCode
	}{
		{http.MethodGet, "/v1/sandboxes/" + b.ID + "/connect", "alice-token", 503, ErrManualRecovery},
		{http.MethodGet, "/v1/sandboxes/" + b.ID, "", 404, ErrNotFound},
		{http.MethodGet, "/metrics", "alice-token", 403, ErrForbidden},
		{http.MethodPost, "/v1/drain", "alice-token", 403, ErrForbidden},
		{http.MethodPost, "/v1/tenants", "alice-token", 403, ErrForbidden},
	} {
		request := httptest.NewRequest(tc.method, tc.path, nil)
		request.Header.Set("Connection", "Upgrade")
		request.Header.Set("Upgrade", "sandbox-v1")
		if tc.token != "" {
			request.Header.Set("Authorization", "Bearer "+tc.token)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		var failure Error
		if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil ||
			response.Code != tc.status || failure.Code != tc.code ||
			bytes.Contains(response.Body.Bytes(), []byte("/private/")) {
			t.Fatalf(
				"%s: status=%d body=%s decode=%v",
				tc.path,
				response.Code,
				response.Body,
				err,
			)
		}
	}
	ledgerMoney(t, s, p.TenantID, 1, 64)
}

func TestHistoricalOperationProjectionDoesNotRewriteReceipt(t *testing.T) {
	s, engine, a, p := fixture(t)
	a.mu.Lock()
	a.execError = errors.New("/private/host/cause")
	a.mu.Unlock()
	command := templateQuotaBuild("historical")
	command.Template.Warmup = "true"
	o, err := s.Submit(s.ctx, p, "historical", command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(s.ctx, p, o.ID); err == nil {
		t.Fatal("expected warmup failure")
	}
	if diagnostic := templateQuotaOperationDiagnostic(
		t,
		s,
		o.ID,
	); !bytes.Contains(
		[]byte(diagnostic),
		[]byte("/private/host/cause"),
	) {
		t.Fatal("private cause was not retained in the journal", diagnostic)
	}
	// Model a pre-separation receipt using precisely the previous error JSON.
	var body []byte
	if err = s.store.db.QueryRow("SELECT body FROM operations WHERE id=?", o.ID).
		Scan(&body); err != nil {
		t.Fatal(err)
	}
	var record map[string]json.RawMessage
	if err = json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	record["error"] = json.RawMessage(
		`{"code":"conflict","message":"/private/historical/cause"}`,
	)
	body, err = json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.store.db.Exec(
		"UPDATE operations SET body=? WHERE id=?",
		body,
		o.ID,
	); err != nil {
		t.Fatal(err)
	}
	options := s.opts
	options.Initialize, options.Engine = false, engine
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	replayed, err := s.Submit(s.ctx, p, "historical", command)
	if err != nil || replayed.Error == nil ||
		replayed.Error.Code != ErrConflict ||
		replayed.Error.Message != "operation state conflict" {
		t.Fatalf("unsafe replay: %+v %v", replayed.Error, err)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/operations/"+o.ID, nil)
	request.Header.Set("Authorization", "Bearer alice-token")
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != 200 ||
		bytes.Contains(response.Body.Bytes(), []byte("/private/")) ||
		bytes.Contains(response.Body.Bytes(), []byte(`"Cause"`)) {
		t.Fatalf(
			"unsafe operation response: %d %s",
			response.Code,
			response.Body,
		)
	}
	var after []byte
	if err = s.store.db.QueryRow("SELECT body FROM operations WHERE id=?", o.ID).
		Scan(&after); err != nil ||
		!bytes.Equal(body, after) {
		t.Fatal("serving a historical receipt rewrote its durable bytes", err)
	}
}
