package application_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func TestHavenDrainPreservesReplayAndRunningAdmission(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	ctx := context.Background()
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	command := operation.Command{
		Action:     "claim",
		TemplateID: "cold",
		TTLSeconds: 60,
		Policy: egress.Policy{
			Mode: "none",
		},
	}
	original, err := service.SandboxRequests.Execute(ctx, principal, "before-drain", command)
	if err != nil || original.Status != "succeeded" {
		t.Fatal(original, err)
	}
	root := authorization.Principal{Root: true, TenantID: "root"}
	for _, denied := range []authorization.Principal{principal, {Root: true, SandboxID: original.Result.SandboxID}} {
		if err := service.Node.Drain(denied); !errors.Is(err, errs.ErrForbidden) {
			t.Fatal("unauthorized drain", err)
		}
	}
	status, err := service.Node.Status(ctx, root)
	if err != nil || status.Draining {
		t.Fatal("rejected drain changed admission", status, err)
	}
	if err := service.Node.Drain(root); err != nil {
		t.Fatal(err)
	}
	status, err = service.Node.Status(ctx, root)
	if err != nil || !status.Draining {
		t.Fatal("drain decision was not visible to management", status, err)
	}
	replayed, err := service.SandboxRequests.Execute(ctx, principal, "before-drain", command)
	if err != nil || replayed.ID != original.ID || replayed.Status != original.Status {
		t.Fatal("drain changed an accepted operation", replayed, err)
	}
	if _, err = service.Sandboxes.AdmitClaim(ctx, principal, "after-drain", command); !errors.Is(err, errs.ErrExhausted) {
		t.Fatal("draining node admitted a new claim", err)
	}
	var admitted int
	if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM operations WHERE scope_kind='tenant' AND scope_id=? AND idem_key=?", principal.TenantID, "after-drain").Scan(&admitted); err != nil || admitted != 0 {
		t.Fatal("rejected claim left intent", admitted, err)
	}
	running, err := service.Dataplane.Ready(ctx, principal, original.Result.SandboxID)
	if err != nil || running.ID != original.Result.SandboxID || running.State != "running" {
		t.Fatal("drain revoked an existing running sandbox", running, err)
	}
}

func TestHavenStepShutdownBackendDeadline(t *testing.T) {
	testutil.ParallelLifecycle(t)
	release := make(chan struct{})
	var closer *blockingCloseRuntime
	service, _ := testapp.ColdServiceWithBackend(t, nil, func(backend *testfixture.ColdRuntime) loom.Backend {
		closer = &blockingCloseRuntime{
			ColdRuntime: backend,
			Entered:     make(chan struct{}),
			Release:     release,
		}
		return closer
	})
	var unblock sync.Once
	t.Cleanup(func() {
		unblock.Do(func() {
			close(release)
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- service.Close(ctx)
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("backend close did not retain caller deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("backend close blocked the caller beyond its deadline")
	}
	select {
	case <-closer.Entered:
	case <-time.After(time.Second):
		t.Fatal("backend finalization did not start")
	}
	if other, err := testapp.OpenStore(service.State.Directory(), false); !errors.Is(err, store.ErrConflict) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal("blocked backend finalization released the writer lock", err)
	}
	unblock.Do(func() {
		close(release)
	})
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := testapp.OpenStore(service.State.Directory(), false)
	if err != nil {
		t.Fatal("completed finalization did not release the writer", err)
	}
	if err = state.Close(); err != nil {
		t.Fatal(err)
	}
}

type blockingCloseRuntime struct {
	*testfixture.ColdRuntime

	Entered chan struct{}
	Release <-chan struct{}
}

func (backend *blockingCloseRuntime) Close() error {
	close(backend.Entered)
	<-backend.Release
	return nil
}
