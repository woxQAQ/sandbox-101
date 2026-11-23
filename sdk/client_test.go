package sdk_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/sdk"
)

func TestHTTPFailuresEndAtHavenContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"forbidden","message":"root access required"}`))
	}))
	defer server.Close()
	client := sdk.New(server.URL, "unused-test-token")
	for _, connect := range []bool{
		false,
		true,
	} {
		var err error
		if connect {
			_, err = client.Connect(context.Background(), "sandbox")
		} else {
			_, err = client.Get(context.Background(), "sandbox")
		}
		if err == nil || !strings.Contains(err.Error(), "forbidden: root access required") {
			t.Fatalf("HTTP error did not retain the Haven wire contract: %#v", err)
		}
	}
}
