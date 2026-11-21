package api

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
)

func TestPreviewSigningParameters(t *testing.T) {
	app, _, _, _ := testapp.SandboxFixture(t)
	owner := authorization.Principal{
		TenantID: "tenant",
	}
	claim, err := app.CommandAdmission.Execute(t.Context(), owner, "preview", operation.Intent{
		Action:     "claim",
		TemplateID: "cold",
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := New(Services{
		Authentication: app.Authentication,
		Dataplane:      app.Dataplane,
	})
	for _, tc := range []struct {
		query  string
		status int
	}{
		{"port=8080", 200},
		{"port=8080&ttl_seconds=1", 200},
		{"port=abc", 400},
		{"port=65536", 400},
		{"port=8080&ttl_seconds=", 400},
		{"port=8080&ttl_seconds=abc", 400},
		{"port=8080&ttl_seconds=0", 400},
		{"port=8080&ttl_seconds=-1", 400},
		{"port=8080&ttl_seconds=9223372037", 400},
		{"port=8080&ttl_seconds=10&ttl_seconds=20", 400},
		{"port=8080&ttl_seconds=%zz", 400},
	} {
		t.Run(tc.query, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/sandboxes/"+claim.Result.SandboxID+"/preview?"+tc.query, strings.NewReader(`{}`))
			req.Header.Set("Authorization", "Bearer fixture-root-token")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("HTTP %d, want %d: %s", w.Code, tc.status, w.Body)
			}
		})
	}
}

func TestPreviewPreservesAdmissionErrorsDuringForward(t *testing.T) {
	app, _, guest, _ := testapp.SandboxFixture(t)
	owner := authorization.Principal{
		TenantID: "tenant",
	}
	claim, err := app.CommandAdmission.Execute(t.Context(), owner, "preview-handoff", operation.Intent{
		Action:     "claim",
		TemplateID: "cold",
	})
	if err != nil {
		t.Fatal(err)
	}
	id := claim.Result.SandboxID
	path, err := app.Dataplane.PreviewURL(t.Context(), owner, dataplane.PreviewRequest{
		SandboxID: id,
		Port:      8080,
		TTL:       time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := New(Services{
		Dataplane: app.Dataplane,
	})
	check := func(want int) {
		t.Helper()
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
		if w.Code != want {
			t.Fatalf("HTTP %d, want %d: %s", w.Code, want, w.Body)
		}
	}
	guest.Before = func(string, loom.Resource) error {
		return errs.ErrUnknown
	}
	check(http.StatusBadGateway)
	guest.Before = func(effect string, _ loom.Resource) error {
		if effect == "connect" {
			_, err := app.CommandAdmission.Execute(t.Context(), owner, "configure-during-forward", operation.Intent{
				Action:    "configure",
				SandboxID: id,
			})
			return err
		}
		return nil
	}
	check(http.StatusConflict)
}

func TestPreviewTransportBoundsForwardAndResponseHeaders(t *testing.T) {
	for _, stage := range []string{
		"forward",
		"headers",
	} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				transport := newPreviewTransport()
				defer transport.CloseIdleConnections()
				target := dataplane.PreviewTarget{
					Forward: func(ctx context.Context) (net.Conn, error) {
						if stage == "forward" {
							<-ctx.Done()
							return nil, ctx.Err()
						}
						client, server := net.Pipe()
						go func() {
							defer func() {
								_ = server.Close()
							}()
							reader := bufio.NewReader(server)
							if _, err := http.ReadRequest(reader); err != nil {
								t.Error(err)
							}
							// The upstream sends no headers. Wait for the transport to release the socket.
							_, _ = reader.ReadByte()
						}()
						return client, nil
					},
				}
				ctx := context.WithValue(t.Context(), previewTargetKey{}, target)
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://preview.test/", nil)
				if err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				_, err = transport.RoundTrip(req)
				want := 5 * time.Second
				if stage == "headers" {
					want = 30 * time.Second
				}
				if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != want {
					t.Fatalf("%s ended after %v: %v", stage, time.Since(start), err)
				}
			})
		})
	}
}
