package testapp

import (
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
)

func CloneFixtureWithFault(t *testing.T, fault func(string) error) (*application.App, *testfixture.RestorableRuntime, operation.State) {
	t.Helper()
	service, _ := ColdServiceWithBackend(t, fault, testfixture.RestorableBackend)
	backend := service.Options.Backend.(*testfixture.RestorableRuntime)
	capture, _ := testfixture.CaptureReady(t, service, "clone-source")
	step := testutil.AwaitCapture(t, service, capture.ID)
	testutil.CapturePublished(t, service, step, true)
	return service, backend, capture
}
