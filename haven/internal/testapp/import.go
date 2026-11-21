package testapp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
)

func FailedCachedImport(t *testing.T, fault func(string) error) (*application.App, operation.State) {
	t.Helper()
	options := testfixture.PrivateOCIImport(t)
	var abort atomic.Bool
	abort.Store(true)
	service, _, _, _ := SandboxFixtureWithFault(t, func(point string) error {
		if abort.Load() && point == "image_import_layer_saved" {
			return errors.New("known fixture abort after cache responsibility persisted")
		}
		if fault != nil {
			return fault(point)
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	acceptedOperation, err := service.Images.AdmitImport(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "import", options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Operations.WaitManagement(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, acceptedOperation.ID)
	if err == nil || result.Status != "failed" {
		t.Fatal(result, err)
	}
	testutil.WaitImportInterruption(t, service, acceptedOperation.ID)
	abort.Store(false)
	return service, result
}
