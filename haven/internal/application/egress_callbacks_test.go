package application_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testutil"
)

func TestHavenEgressFactoryUsesOwnedCallbacks(t *testing.T) {
	testutil.ParallelLifecycle(t)
	var host application.EgressCallbacks
	service, _ := testapp.ColdServiceWithOptions(t, application.Options{
		EgressFactory: func(callbacks application.EgressCallbacks) (application.Egress, string, error) {
			host = callbacks
			if host.Audit == nil || host.PrepareProxySocket == nil || host.RecoverStaleSocket == nil || host.InstallDirect == nil {
				return nil, "", fmt.Errorf("egress host callback missing")
			}
			return nil, "", host.Audit(context.Background(), map[string]string{
				"action": "fixture-factory",
			})
		},
	}, nil)
	ctx := context.Background()
	var records int
	if err := service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM journal").Scan(&records); err != nil || records != 1 {
		t.Fatal("factory callback did not commit its audit before startup", records, err)
	}
	if err := host.Audit(ctx, map[string]string{
		"action": "fixture-bound",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Audit(ctx, map[string]string{
		"action": "fixture-closed",
	}); !errors.Is(err, errs.ErrConflict) {
		t.Fatal("retained callback bypassed the shared closing barrier", err)
	}
	if _, err := application.InspectState(ctx, service.State.Directory()); err != nil {
		t.Fatal("factory audit did not preserve the authenticated store", err)
	}
}
