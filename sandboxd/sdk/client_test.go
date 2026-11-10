package sdk_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/woxQAQ/sandboxd"
	"github.com/woxQAQ/sandboxd/sdk"
)

func TestHTTPFailuresBelongToSandboxd(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write(
				[]byte(`{"code":"forbidden","message":"root access required"}`),
			)
		}),
	)
	defer server.Close()
	client := sdk.New(server.URL, "unused-test-token")
	for _, connect := range []bool{false, true} {
		var err error
		if connect {
			_, err = client.Connect(context.Background(), "sandbox")
		} else {
			_, err = client.Get(context.Background(), "sandbox")
		}
		var failure *sandboxd.Error
		if !errors.As(err, &failure) || failure.Code != sandboxd.ErrForbidden ||
			failure.Cause != nil {
			t.Fatalf(
				"HTTP error did not retain the sandboxd contract: %#v",
				err,
			)
		}
	}
}
