package haven

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/egress"
)

func TestHavenCheckpointDeleteRetainsForkBacking(t *testing.T) {
	service, runtime, _, _ := sandboxFixture(t)
	service.opts.Backend = &restorableRuntime{runtime}
	principal := Principal{
		TenantID: "tenant",
	}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	checkpoint, err := service.Execute(ctx, principal, "checkpoint", Command{
		Action:    "checkpoint",
		SandboxID: claim.Result.SandboxID,
	})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := service.Execute(ctx, principal, "fork", Command{
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
	if _, err = service.AdmitCheckpointDelete(ctx, Principal{
		TenantID: "other",
	}, "foreign", Command{
		Action:       "checkpoint_delete",
		CheckpointID: checkpoint.Result.CheckpointID,
	}); err == nil {
		t.Fatal("foreign checkpoint deletion accepted")
	}
	deleted, err := service.Execute(ctx, principal, "delete", Command{
		Action:       "checkpoint_delete",
		CheckpointID: checkpoint.Result.CheckpointID,
	})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.Execute(ctx, principal, "delete", Command{
		Action:       "checkpoint_delete",
		CheckpointID: checkpoint.Result.CheckpointID,
	})
	if err != nil || !reflect.DeepEqual(replay, deleted) {
		t.Fatal(replay, deleted, err)
	}
	if _, err = service.GetCheckpoint(ctx, principal, checkpoint.Result.CheckpointID); err == nil {
		t.Fatal("checkpoint binding remained published")
	}
	if _, err = service.Execute(ctx, principal, "source-release", Command{
		Action:    "release",
		SandboxID: claim.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := service.CollectGarbage(ctx, Principal{
		Root:     true,
		TenantID: "root",
	}, "gc", GCOptions{
		Budget: 15 * time.Second,
	})
	if err != nil || len(report.Collected) != 1 || report.Collected[0].Kind != "snapshot" {
		t.Fatal(report, err)
	}
	var slots, storage int
	if err = service.store.db.QueryRow("SELECT SUM(sandbox_slots),SUM(storage_mib) FROM quota_resources").Scan(&slots, &storage); err != nil || slots != 1 || storage != 256 {
		t.Fatal("snapshot backing responsibility retired early", slots, storage, err)
	}
	if _, err = service.DataReady(ctx, principal, fork.Result.SandboxID); err != nil {
		t.Fatal("checkpoint deletion invalidated independent fork", err)
	}
	if _, err = service.Execute(ctx, principal, "fork-release", Command{
		Action:    "release",
		SandboxID: fork.Result.SandboxID,
	}); err != nil {
		t.Fatal(err)
	}
	if err = service.store.db.QueryRow("SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources").Scan(&slots, &storage); err != nil || slots != 0 || storage != 0 {
		t.Fatal("last backing reclamation did not retire snapshot", slots, storage, err)
	}
	original, err := service.GetOperation(ctx, "tenant", checkpoint.ID)
	if err != nil || !reflect.DeepEqual(original, checkpoint) {
		t.Fatal("checkpoint cleanup changed original result", original, checkpoint, err)
	}
}
