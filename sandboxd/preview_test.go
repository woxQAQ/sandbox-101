package sandboxd

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPreviewBoundToEpochAndLease(t *testing.T) {
	s, _, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	u, err := s.PreviewURL(
		context.Background(),
		p,
		r.Sandbox.ID,
		8080,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(strings.TrimPrefix(u, "/preview/"), ".")
	if len(parts) != 2 {
		t.Fatal("unsigned preview")
	}
	_, err = s.Execute(
		context.Background(),
		p,
		"release",
		Command{Action: "release", SandboxID: r.Sandbox.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", u, nil))
	if w.Code != 404 {
		t.Fatalf("released preview accepted: %d", w.Code)
	}
}
