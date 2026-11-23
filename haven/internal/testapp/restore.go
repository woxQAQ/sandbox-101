package testapp

import (
	"context"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/loom"
)

func ReopenCloneService(t *testing.T, old *application.App, backend loom.Backend) *application.App {
	t.Helper()
	return ReopenCloneServiceWithFault(t, old, backend, nil)
}

func ReopenCloneServiceWithFault(t *testing.T, old *application.App, backend loom.Backend, fault func(string) error) *application.App {
	t.Helper()
	if err := old.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts := old.Options
	opts.Initialize = false
	opts.Fault = fault
	opts.Backend = backend
	service, err := application.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = service.Close(context.Background())
	})
	return service
}
