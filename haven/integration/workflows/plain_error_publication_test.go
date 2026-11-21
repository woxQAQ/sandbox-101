package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenPlainFailureKeepsDiagnosticOutsideFrozenResult(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, guest, _ := testapp.SandboxFixture(t)
	privatePath := filepath.Join(t.TempDir(), "private-host-input")
	_, failure := os.ReadFile(privatePath)
	if !errors.Is(failure, os.ErrNotExist) {
		t.Fatal(failure)
	}
	guest.Before = func(name string, _ loom.Resource) error {
		if name == "activate" {
			return fmt.Errorf("activate private fixture: %w", failure)
		}
		return nil
	}
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	acceptedRecord, err := service.CommandAdmission.Execute(ctx, principal, "plain-host-failure", operation.Intent{
		Action:     "claim",
		TemplateID: "cold",
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err == nil || acceptedRecord.ID == "" || acceptedRecord.Status != "failed" {
		t.Fatal("host failure was not retained", acceptedRecord, err)
	}
	diagnostic := service.Workers.State(acceptedRecord.ID).Err
	if !errors.Is(diagnostic, os.ErrNotExist) || !strings.Contains(diagnostic.Error(), privatePath) {
		t.Fatal("private diagnostic lost its original context", diagnostic)
	}
	view, err := service.Delivery.PublicOperation(ctx, principal, acceptedRecord.ID)
	if err != nil || view.Status != "failed" || view.Error != nil || strings.Contains(string(operation.Encode(view)), privatePath) {
		t.Fatal("private file absence became a public diagnosis", view, err)
	}
	if view.Result.Sandbox != nil && view.Result.Sandbox.State == "running" {
		t.Fatal("failed activation published a running sandbox")
	}
	reopened := testapp.ReopenCloneService(t, service, service.Options.Backend)
	replayed, err := reopened.Delivery.PublicOperation(ctx, principal, acceptedRecord.ID)
	if err != nil || !reflect.DeepEqual(replayed, view) {
		t.Fatal("restart changed the frozen public failure", replayed, err)
	}
}
