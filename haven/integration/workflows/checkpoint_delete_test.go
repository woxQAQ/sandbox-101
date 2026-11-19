package workflows_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestHavenCheckpointDeleteRetainsForkBacking(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	checkpoint, err := service.SandboxRequests.Execute(ctx, principal, "checkpoint", operation.Command{
		Action:    "checkpoint",
		SandboxID: claim.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := service.SandboxRequests.Execute(ctx, principal, "fork", operation.Command{
		Action:       "fork",
		CheckpointID: checkpoint.Result.CheckpointID,
		TTLSeconds:   60,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Lifecycle.AdmitCheckpointDelete(ctx, authorization.Principal{
		TenantID: "other",
	}, "foreign", operation.Command{
		Action:       "checkpoint_delete",
		CheckpointID: checkpoint.Result.CheckpointID,
	}); err == nil {
		t.Fatal("foreign checkpoint deletion accepted")
	}
	deleted, err := service.SandboxRequests.Execute(ctx, principal, "delete", operation.Command{
		Action:       "checkpoint_delete",
		CheckpointID: checkpoint.Result.CheckpointID,
	})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.SandboxRequests.Execute(ctx, principal, "delete", operation.Command{
		Action:       "checkpoint_delete",
		CheckpointID: checkpoint.Result.CheckpointID,
	})
	if err != nil || !reflect.DeepEqual(replay, deleted) {
		t.Fatal(replay, deleted, err)
	}
	if _, err = service.Catalog.GetCheckpoint(ctx, principal, checkpoint.Result.CheckpointID); err == nil {
		t.Fatal("checkpoint binding remained published")
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "source-release", operation.Command{
		Action:    "release",
		SandboxID: claim.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := service.GC.Collect(ctx, authorization.Principal{
		Root:     true,
		TenantID: "root",
	}, "gc", types.GCOptions{
		Budget: 15 * time.Second,
	})
	if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "snapshot" {
		t.Fatal(report, err)
	}
	var slots, storage int
	if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots),SUM(storage_mib) FROM quota_resources").Scan(&slots, &storage); err != nil || slots != 1 || storage != 256 {
		t.Fatal("snapshot backing responsibility retired early", slots, storage, err)
	}
	if _, err = service.Dataplane.Ready(ctx, principal, fork.Result.SandboxID); err != nil {
		t.Fatal("checkpoint deletion invalidated independent fork", err)
	}
	if _, err = service.SandboxRequests.Execute(ctx, principal, "fork-release", operation.Command{
		Action:    "release",
		SandboxID: fork.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources").Scan(&slots, &storage); err != nil || slots != 0 || storage != 0 {
		t.Fatal("last backing reclamation did not retire snapshot", slots, storage, err)
	}
	original, err := service.Operations.GetOperation(ctx, "tenant", checkpoint.ID)
	if err != nil || !reflect.DeepEqual(original, checkpoint) {
		t.Fatal("checkpoint cleanup changed original result", original, checkpoint, err)
	}
}
