package testutil

import (
	"context"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
)

func CapturePublished(t *testing.T, service *application.App, step machine.CaptureStep, want bool) {
	t.Helper()
	snapshot, err := machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), step.Input.Request.TenantID, step.Input.Snapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	if want != (snapshot.Manifest != nil) || want != (step.Status == "succeeded") {
		t.Fatalf("snapshot and step publication disagree: status=%s manifest=%v phase=%s", step.Status, snapshot.Manifest != nil, step.State.Phase)
	}
	var storage, pending, charged int64
	if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(storage_mib),0),COALESCE(SUM(kind='pending_storage'),0),COALESCE(SUM(kind='snapshot'),0) FROM quota_resources WHERE deleted_at IS NULL AND source_operation_id=?", step.OperationID).Scan(&storage, &pending, &charged); err != nil {
		t.Fatal(err)
	}
	if storage != 192 || want && (charged != 1 || pending != 0) || !want && (charged != 0 || pending != 1) {
		t.Fatal("capture lost or partially transferred its liability", storage, pending, charged)
	}
	if want {
		actual, err := loom.HashTree(snapshot.Dir)
		if err != nil || !reflect.DeepEqual(actual, snapshot.Manifest.Files) {
			t.Fatal("published bytes differ from fixed proof", err)
		}
	}
}
