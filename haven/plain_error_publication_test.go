package haven

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

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
)

func TestHavenPlainFailureKeepsDiagnosticOutsideFrozenResult(t *testing.T) {
	service, _, guest, _ := sandboxFixture(t)
	privatePath := filepath.Join(t.TempDir(), "private-host-input")
	_, failure := os.ReadFile(privatePath)
	if !errors.Is(failure, os.ErrNotExist) {
		t.Fatal(failure)
	}
	guest.before = func(name string, _ loom.Resource) error {
		if name == "activate" {
			return fmt.Errorf("activate private fixture: %w", failure)
		}
		return nil
	}
	principal := Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	operation, err := service.Execute(ctx, principal, "plain-host-failure", Command{
		Action:     "claim",
		TemplateID: "cold",
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err == nil || operation.ID == "" || operation.Status != "failed" {
		t.Fatal("host failure was not retained", operation, err)
	}
	service.mu.Lock()
	diagnostic := service.lastErrors[operation.ID]
	service.mu.Unlock()
	if !errors.Is(diagnostic, os.ErrNotExist) || !strings.Contains(diagnostic.Error(), privatePath) {
		t.Fatal("private diagnostic lost its original context", diagnostic)
	}
	view, err := service.PublicOperation(ctx, principal, operation.ID)
	if err != nil || view.Status != "failed" || view.Error != nil || strings.Contains(string(encode(view)), privatePath) {
		t.Fatal("private file absence became a public diagnosis", view, err)
	}
	if view.Result.Sandbox != nil && view.Result.Sandbox.State == "running" {
		t.Fatal("failed activation published a running sandbox")
	}
	reopened := reopenCloneService(t, service, service.opts.Backend)
	replayed, err := reopened.PublicOperation(ctx, principal, operation.ID)
	if err != nil || !reflect.DeepEqual(replayed, view) {
		t.Fatal("restart changed the frozen public failure", replayed, err)
	}
}
