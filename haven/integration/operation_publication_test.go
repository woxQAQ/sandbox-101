//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/sdk"
	"github.com/woxQAQ/haven/types"
)

type failedPublicationGuest struct {
	transportGuest
	fail       atomic.Bool
	diagnostic error
}

func (guest *failedPublicationGuest) Activate(ctx context.Context, resource loom.Resource, activation guest.Activation) error {
	if guest.fail.Load() {
		return guest.diagnostic
	}
	return guest.transportGuest.Activate(ctx, resource, activation)
}

func TestHavenHTTPFormatsFrozenOperationFailure(t *testing.T) {
	privatePath := filepath.Join(t.TempDir(), "private-host-input")
	_, missing := os.ReadFile(privatePath)
	if missing == nil {
		t.Fatal("private fixture unexpectedly exists")
	}
	guest := &failedPublicationGuest{
		diagnostic: fmt.Errorf("%s: %w", errs.ErrExhausted.Error(), missing),
	}
	service, scoped, principal, _ := transportFixtureRoot(t, false, guest)
	guest.fail.Store(true)
	command := []byte(`{"template_id":"fixture","policy":{"mode":"none"}}`)
	payload := controlHTTP(t, scoped.Endpoint, "POST", "/v1/claim", "transport-token", "failed-delivery", command, true, 202)
	var accepted types.Operation
	if err := json.Unmarshal(payload, &accepted); err != nil {
		t.Fatal(err)
	}
	client := sdk.New(scoped.Endpoint, "transport-token")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	view := accepted
	for view.Status != "failed" {
		if ctx.Err() != nil {
			t.Fatal("operation did not reach its failed terminal state", view, ctx.Err())
		}
		var err error
		view, err = client.Operation(ctx, accepted.ID)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if view.Error == nil || view.Error.Code != types.ErrInternal || view.Error.Message != "internal service error" {
		t.Fatal("HTTP failure was not formatted at the boundary", view)
	}
	internal, err := service.Delivery.PublicOperation(ctx, principal, accepted.ID)
	if err != nil || internal.Error != nil || internal.Status != "failed" {
		t.Fatal("business publication retained an HTTP error", internal, err)
	}
	var persisted []byte
	for attempt := 0; attempt < 2; attempt++ {
		payload = controlHTTP(t, scoped.Endpoint, "POST", "/v1/claim", "transport-token", "failed-delivery", command, false, 500)
		var failure types.ApiError
		if err = json.Unmarshal(payload, &failure); err != nil || !reflect.DeepEqual(&failure, view.Error) {
			t.Fatal("synchronous retry changed the failure", failure, err)
		}
		payload = controlHTTP(t, scoped.Endpoint, "GET", "/v1/operations/"+accepted.ID, "transport-token", "", nil, false, 200)
		if bytes.Contains(payload, []byte(privatePath)) || bytes.Contains(payload, []byte("private-host-input")) {
			t.Fatal("HTTP operation leaked a private diagnostic")
		}
		if attempt == 0 {
			persisted = payload
		} else if !bytes.Equal(persisted, payload) {
			t.Fatal("retry changed the completed operation response")
		}
	}
}
