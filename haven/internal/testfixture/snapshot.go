package testfixture

import (
	"context"
	"errors"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func CaptureReady(t *testing.T, service *application.App, key string) (operation.Record, machine.CaptureRequest) {
	t.Helper()
	claim := ColdClaim(t, service, "source-"+key)
	source := testutil.AwaitColdWorker(t, service, claim.ID)
	if source.Status != "succeeded" {
		t.Fatal(source.State)
	}
	request := machine.CaptureRequest{
		TenantID:         "tenant",
		Key:              key,
		SandboxID:        claim.Result.SandboxID,
		ExpectedInstance: source.State.Witness,
	}
	acceptedOperation, err := service.Captures.AdmitCapture(context.Background(), request)
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation, request
}
