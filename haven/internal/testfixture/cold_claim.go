package testfixture

import (
	"context"
	"errors"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func ColdClaim(t *testing.T, service *application.App, key string) operation.State {
	t.Helper()
	acceptedOperation, err := service.Provisioning.AdmitCold(context.Background(), machine.FreshRequest{
		TenantID:   "tenant",
		Key:        key,
		TemplateID: "cold",
	})
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation
}
