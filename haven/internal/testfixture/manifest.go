package testfixture

import (
	"context"
	"testing"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/store"
)

// PublishManifest installs a fixture's sealed manifest using the same catalog transaction as the importer.
func PublishManifest(t *testing.T, db store.WorkflowDatabase, manifest host.ImageManifest) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = image.PublishManifest(ctx, tx, manifest); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
